---- MODULE MongoInsert ----
\* The MongoDB extension's v1 write path, at the grain of one insert statement.
\* Written from src/mongo_insert.cpp (the MongoInsertOperator sink) and its
\* wiring in MongoCatalog::PlanInsert. What a real sink worker does: serialize
\* each input row to a BSON document, stage it, and flush a batch to the target
\* collection with insert_many; a lock combines each worker's row count into the
\* statement's INSERT count.
\*
\* Two axes carry the risk. Data representation: SerializeRow reverses the read
\* path's flattening, so a column named "address_city" mapped to the path
\* <<"address","city">> is written as a nested subdocument, and its DuckDB type
\* decides the canonical extended-JSON shape the driver serializes. Encode below
\* models that rule, and the conformance edge holds the code's serializer to it.
\* Atomicity under fault: insert_many is called unordered (ordered:false), so a
\* batch that fails on some documents (a duplicate _id) inserts every other one and
\* then throws. The durable set is the batch minus the rejected documents, not a
\* prefix. The exception is uncaught, so the statement aborts and reports no count.
\* FlushBatchFail models that: durable gains the batch minus a non-empty failed
\* subset, and reported stays NONE. The insert is not atomic; the properties state
\* the delivered guarantee (no count on failure, no over-insert) rather than an
\* all-or-nothing insert the code does not provide.
\*
\* Participants are the sink's parallel workers. DuckDB routes the input rows
\* (Pool) to workers, which Init models by any assignment of rows to workers.
\* Each worker reads and writes only its own pending rows, buffer, and staged
\* count; the collection (durable) and the combined count (total) are the shared
\* store and the locked accumulator. Safety only.
EXTENDS Naturals, Sequences

CONSTANTS
  Workers,   \* the set of sink workers (LocalSinkState instances).
  Cols,      \* the set of table column names, in the collection's inferred schema.
  Path,      \* Path[c] is column c's MongoDB path, a sequence of 1 or 2 segments.
  Pool,      \* the statement's input rows, a sequence; Init routes them to workers.
  BatchSize, \* the flush threshold: a worker flushes once its buffer reaches it.
  NULL,      \* the sentinel for a cell the row does not supply.
  NONE,      \* the value reported holds until (and unless) the source emits.
  FREE       \* the lockHolder value when the count lock is held by no worker.

VARIABLES
  pending,   \* pending[w]: rows worker w has not yet staged (Sink's remaining input).
  buffer,    \* buffer[w]: documents staged and not yet flushed (the worker's batch).
  staged,    \* staged[w]: rows worker w has staged in total (its row_count).
  durable,   \* the collection: documents insert_many has made durable, in order.
  total,     \* the combined INSERT count (insert_count), summed under the lock.
  combined,  \* combined[w]: whether worker w has run Combine.
  aborted,   \* whether a flush failed, so the statement is aborting.
  reported,  \* the count the source emitted, or NONE until (and unless) it does.
  lockHolder,\* the worker inside its Combine critical section, or FREE (gstate.lock).
  snap       \* snap[w]: the count worker w read at CombineRead, before its write-back.

vars == <<pending, buffer, staged, durable, total, combined, aborted, reported,
          lockHolder, snap>>

SE == INSTANCE SequencesExt

\* One cell, in the canonical extended-JSON shape bsoncxx::to_json produces: an
\* int32 is {"$numberInt": "<decimal>"}, an int64 {"$numberLong": ...}, a double
\* {"$numberDouble": ...}, an ObjectId {"$oid": "<hex>"}, a date (DATE or
\* TIMESTAMP) {"$date": {"$numberLong": "<ms>"}}, a boolean the bare JSON value,
\* and a string the bare string. cell.s is the decimal, hex, millisecond, or text
\* the driver writes (cell.b for a boolean); a subdocument is built by Encode.
\* Recursive, since a list holds cells and a struct holds named cells: a LIST is a
\* sequence of encoded elements, a STRUCT a subdocument of encoded fields, and a
\* BLOB the {"$binary": {"base64": ..., "subType": "00"}} form.
RECURSIVE EncodeCell(_)
EncodeCell(cell) ==
  CASE cell.type = "int32"  -> [k \in {"$numberInt"}    |-> cell.s]
    [] cell.type = "int64"  -> [k \in {"$numberLong"}   |-> cell.s]
    [] cell.type = "double" -> [k \in {"$numberDouble"} |-> cell.s]
    [] cell.type = "oid"    -> [k \in {"$oid"}          |-> cell.s]
    [] cell.type = "date"   -> [k1 \in {"$date"} |-> [k2 \in {"$numberLong"} |-> cell.s]]
    [] cell.type = "bool"   -> cell.b
    [] cell.type = "binary" -> [k \in {"$binary"} |-> [base64 |-> cell.s, subType |-> "00"]]
    [] cell.type = "list"   -> [i \in DOMAIN cell.elems  |-> EncodeCell(cell.elems[i])]
    [] cell.type = "struct" -> [nm \in DOMAIN cell.fields |-> EncodeCell(cell.fields[nm])]
    [] OTHER                -> cell.s

\* The columns this row supplies. A NULL cell is omitted (SerializeRow skips it),
\* so a row that supplies no _id leaves the document without one and MongoDB
\* generates it.
Present(row) == {c \in Cols : row[c] # NULL}

\* The top-level document keys: the first segment of each present column's path.
Tops(row) == {Head(Path[c]) : c \in Present(row)}

\* A top key is direct when a column's whole path is that one segment; the
\* columns nested under it otherwise supply its subdocument's fields.
DirectCol(row, t) == CHOOSE c \in Present(row) : Path[c] = <<t>>
NestCols(row, t)  == {c \in Present(row) : Head(Path[c]) = t /\ Len(Path[c]) = 2}
SubCol(row, t, s) == CHOOSE c \in NestCols(row, t) : Path[c][2] = s

\* The document for one row: a function keyed by top segment. A direct key holds
\* the encoded cell; a nested key holds a subdocument keyed by second segment,
\* merging siblings (address_city and address_zip into one address subdoc). Paths
\* run at most two deep, which is what a flattened read schema produces.
Encode(row) ==
  [t \in Tops(row) |->
      IF \E c \in Present(row) : Path[c] = <<t>>
      THEN EncodeCell(row[DirectCol(row, t)])
      ELSE [s \in {Path[c][2] : c \in NestCols(row, t)} |->
               EncodeCell(row[SubCol(row, t, s)])]]

\* Init routes the input rows to workers: any function from row index to worker,
\* each worker's pending being the subsequence it was assigned, in Pool order.
IdxSeq == [k \in 1..Len(Pool) |-> k]
PendingFor(w, assign) ==
  SE!FoldLeft(LAMBDA acc, k : IF assign[k] = w THEN Append(acc, Pool[k]) ELSE acc,
              <<>>, IdxSeq)

\* The documents of a batch that survive an unordered flush: those whose index is
\* not among the failed set, kept in batch order.
Kept(batch, failed) ==
  SE!FoldLeft(LAMBDA acc, k : IF k \in failed THEN acc ELSE Append(acc, batch[k]),
              <<>>, [k \in 1..Len(batch) |-> k])

Init ==
  /\ \E assign \in [1..Len(Pool) -> Workers] :
       pending = [w \in Workers |-> PendingFor(w, assign)]
  /\ buffer   = [w \in Workers |-> <<>>]
  /\ staged   = [w \in Workers |-> 0]
  /\ durable  = <<>>
  /\ total    = 0
  /\ combined = [w \in Workers |-> FALSE]
  /\ aborted  = FALSE
  /\ reported = NONE
  /\ lockHolder = FREE
  /\ snap     = [w \in Workers |-> 0]

\* Sink's per-row work: serialize the next pending row and stage it, counting it.
StageRow(w) ==
  /\ ~aborted
  /\ pending[w] # <<>>
  /\ buffer'  = [buffer  EXCEPT ![w] = Append(buffer[w], Encode(Head(pending[w])))]
  /\ pending' = [pending EXCEPT ![w] = Tail(pending[w])]
  /\ staged'  = [staged  EXCEPT ![w] = staged[w] + 1]
  /\ UNCHANGED <<durable, total, combined, aborted, reported, lockHolder, snap>>

\* A flush happens at the code's flush points: the buffer reached the batch size,
\* or the worker has staged everything and flushes the remainder at Combine.
AtFlushPoint(w) == Len(buffer[w]) >= BatchSize \/ pending[w] = <<>>

\* insert_many the worker's staged batch, all of it made durable, and clear the
\* buffer.
FlushBatchOk(w) ==
  /\ ~aborted
  /\ buffer[w] # <<>>
  /\ AtFlushPoint(w)
  /\ durable' = durable \o buffer[w]
  /\ buffer'  = [buffer EXCEPT ![w] = <<>>]
  /\ UNCHANGED <<pending, staged, total, combined, aborted, reported, lockHolder, snap>>

\* The unordered insert_many fails on some documents (a duplicate _id) but inserts
\* the rest: the durable set is the batch minus a non-empty failed subset, kept in
\* batch order. mongocxx throws and nothing catches it, so the statement aborts.
\* No further work happens and no count is reported.
FlushBatchFail(w) ==
  /\ ~aborted
  /\ buffer[w] # <<>>
  /\ AtFlushPoint(w)
  /\ \E failed \in (SUBSET DOMAIN buffer[w]) \ {{}} :
       durable' = durable \o Kept(buffer[w], failed)
  /\ buffer'  = [buffer EXCEPT ![w] = <<>>]
  /\ aborted' = TRUE
  /\ UNCHANGED <<pending, staged, total, combined, reported, lockHolder, snap>>

\* Combine, decomposed to the grain the code has: the shared count update
\* (gstate.insert_count += lstate.row_count) is a read-modify-write, and the code
\* runs it under gstate.lock. Modeling it as two steps guarded by lockHolder lets
\* the check verify the lock serializes the update rather than assume the RMW is
\* atomic. CombineRead acquires the lock and reads the count; CombineWrite adds
\* the worker's row_count to what it read and releases. Two workers cannot both be
\* between their read and write, so no increment is lost.
CombineRead(w) ==
  /\ ~aborted
  /\ pending[w] = <<>>
  /\ buffer[w]  = <<>>
  /\ ~combined[w]
  /\ lockHolder = FREE
  /\ lockHolder' = w
  /\ snap' = [snap EXCEPT ![w] = total]
  /\ UNCHANGED <<pending, buffer, staged, durable, total, combined, aborted, reported>>

CombineWrite(w) ==
  /\ lockHolder = w
  /\ ~combined[w]
  /\ total'      = snap[w] + staged[w]
  /\ combined'   = [combined EXCEPT ![w] = TRUE]
  /\ lockHolder' = FREE
  /\ UNCHANGED <<pending, buffer, staged, durable, aborted, reported, snap>>

\* The source emits the combined count as the statement's INSERT count. Its guard
\* is the completeness the run reached: every worker combined and nothing aborted,
\* so every input row is durable. A run short of that does not report.
Report ==
  /\ ~aborted
  /\ \A w \in Workers : combined[w]
  /\ reported = NONE
  /\ reported' = total
  /\ UNCHANGED <<pending, buffer, staged, durable, total, combined, aborted, lockHolder, snap>>

Next ==
  \/ Report
  \/ \E w \in Workers :
       StageRow(w) \/ FlushBatchOk(w) \/ FlushBatchFail(w)
         \/ CombineRead(w) \/ CombineWrite(w)

Spec == Init /\ [][Next]_vars

====
