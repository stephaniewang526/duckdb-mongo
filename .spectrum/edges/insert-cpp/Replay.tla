---- MODULE Replay ----
\* Conformance edge: the C++ MongoInsertOperator conforms to MongoInsert.
\*
\* Replays one recorded execution of the DuckDB extension's insert path through
\* the L2 node. TLC keeps the specification's state and each recorded event must
\* be a legal step from the current state. The events are TraceLog, in the record
\* module Trace, which the runner replaces once per replayed trace.
\*
\* Each action is deterministic once its worker parameter is fixed, so Step
\* dispatches directly with no internal choice to pin. Requiring the action
\* enabled says the code took a step the specification allows; requiring the
\* reported state to match says it reached the state the specification computes.
\* The StageRow event carries the serialized document, held to Encode of the row
\* being staged: this is where the data-representation rule (flatten reversal and
\* the canonical extended-JSON encoding) is compared to what the code emitted, and
\* what the state-bug control breaks. FlushBatchFail carries no shipped-code seam,
\* since insert_many throws before one; the fail model's run appends it at the
\* harness level (append_fail.js reads the durable documents back from MongoDB),
\* and ReplayFail below validates that set against the staged batch.
\*
\* The schema and input are the general rule the node ranges over, pinned here to
\* the one the run inserted: each model binds Cols, Path, Pool and BatchSize
\* to a dataset. Worker names are the strings the code prints ("0"), so the
\* constants are strings and there is no SYMMETRY, a replay walking one trace.
\*
\* Verdict, read by scripts/conformance:
\*   CONFORMS: i reaches Len(TraceLog), so Unconsumed is violated.
\*   DIVERGES: some event has no enabled matching step, so TraceNext deadlocks,
\*     which is why the model sets CHECK_DEADLOCK TRUE.
EXTENDS MongoInsert, Trace, TLC, FiniteSets

VARIABLE i     \* 1-indexed position of the last consumed trace record.

tvars == <<vars, i>>

\* This replay is single-worker, and must be. The shared StageRow/FlushBatchOk
\* seams live in MongoBatchSink, which carries no worker id, so a multi-worker
\* trace could not be attributed to a worker: the \E over Workers below would guess,
\* over-accepting where two workers stage the same document. That is by design:
\* DuckDB runs one sink worker at these sizes, a larger run is too big for TLC, and
\* the multi-worker count race is the node's obligation, verified there. This
\* assumption makes a multi-worker configuration fail at once rather than silently.
ASSUME Cardinality(Workers) = 1

\* The recorded Init must be a legal Init of the specification. Init pins every
\* variable, so the pinned constants decide the rest.
TraceInit ==
  /\ i = 1
  /\ Init
  /\ TraceLog[1].action = "Init"

\* The document the writer serialized equals Encode of the row it staged. This is
\* the one thing StageRow carries, since the shared MongoBatchSink owns the buffer
\* and the document but not the operator's row_count. The count is checked at
\* Combine and Report, which only the operator emits.
ValidateStage(w, e)   == e.doc = Encode(Head(pending[w]))
ValidateCombine(w, e) == total'    = e.state.total
ValidateReport(e)     == reported' = e.total

\* The code's Combine is one atomic step under gstate.lock: it emits one event.
\* The node decomposes it into CombineRead and CombineWrite to verify the lock;
\* the replay walks a single worker, whose lock is free throughout, so their net
\* effect is one step. ReplayCombine is that composition.
ReplayCombine(w) ==
  /\ ~aborted
  /\ pending[w] = <<>>
  /\ buffer[w]  = <<>>
  /\ ~combined[w]
  /\ lockHolder = FREE
  /\ total'      = total + staged[w]
  /\ combined'   = [combined EXCEPT ![w] = TRUE]
  /\ snap'       = [snap EXCEPT ![w] = total]
  /\ lockHolder' = FREE
  /\ UNCHANGED <<pending, buffer, staged, durable, aborted, reported>>

\* The unordered-failure abort, captured at the harness level: append_fail.js read
\* the durable documents back from MongoDB after the run aborted, and e.docs holds
\* them. Require those documents to be a proper sub-collection of the staged batch:
\* every durable document was staged (a wrong serializer leaves one that was not,
\* and diverges), and at least one was not inserted (the failure). A record leaving
\* documents outside the batch, or the whole batch, is not a partial failure and
\* diverges. reported stays NONE, since the aborted run emits no Report.
ReplayFail(w, e) ==
  /\ ~aborted
  /\ buffer[w] # <<>>
  /\ AtFlushPoint(w)
  /\ Len(e.docs) < Len(buffer[w])
  /\ SE!ToSet(e.docs) \subseteq SE!ToSet(buffer[w])
  /\ durable' = durable \o e.docs
  /\ buffer'  = [buffer EXCEPT ![w] = <<>>]
  /\ aborted' = TRUE
  /\ UNCHANGED <<pending, staged, total, combined, reported, lockHolder, snap>>

\* The shared StageRow/FlushBatchOk seams (in MongoBatchSink) carry no worker, so
\* the step ranges over Workers; replay is single-worker, so the set is a
\* singleton and the choice is forced. Combine, Report, and the harness-appended
\* FlushBatchFail name their worker in the event.
Step(e) ==
  CASE e.action = "StageRow"       -> \E w \in Workers : StageRow(w) /\ ValidateStage(w, e)
    [] e.action = "FlushBatchOk"   -> \E w \in Workers : FlushBatchOk(w)
    [] e.action = "FlushBatchFail" -> ReplayFail(e.w, e)
    [] e.action = "Combine"        -> ReplayCombine(e.w) /\ ValidateCombine(e.w, e)
    [] e.action = "Report"         -> Report            /\ ValidateReport(e)
    [] OTHER -> FALSE

TraceNext ==
  /\ i < Len(TraceLog)
  /\ i' = i + 1
  /\ Step(TraceLog[i + 1])

TraceSpec == TraceInit /\ [][TraceNext]_tvars

\* Violated exactly when the whole trace is consumed: the CONFORMS signal.
Unconsumed == i < Len(TraceLog)

\* ---- the pinned datasets, one per model ----
I32(s) == [type |-> "int32", s |-> s]
I64(s) == [type |-> "int64", s |-> s]
Dbl(s) == [type |-> "double", s |-> s]
OID(s) == [type |-> "oid",   s |-> s]
Dt(s)  == [type |-> "date",  s |-> s]
Bool(b) == [type |-> "bool", b |-> b]
Str(s) == [type |-> "str",   s |-> s]
Bin(s) == [type |-> "binary", s |-> s]         \* s is the base64 of the bytes.
Lst(elems)  == [type |-> "list",   elems  |-> elems]   \* elems: a sequence of cells.
Strct(flds) == [type |-> "struct", fields |-> flds]    \* flds: a name-keyed function of cells.

\* flat: insert_smoke.test, two rows with an explicit BIGINT _id and a string.
FlatWorkers == {"0"}
FlatCols    == {"_id", "name"}
FlatPath    == ("_id" :> <<"_id">>) @@ ("name" :> <<"name">>)
FlatPool    == << ("_id" :> I64("1")) @@ ("name" :> Str("ann")),
                  ("_id" :> I64("2")) @@ ("name" :> Str("bo")) >>

\* nested: conf_nested.test, one row that omits _id (auto-generated) and nests
\* address_city and address_zip into one address subdocument.
NestedWorkers == {"0"}
NestedCols    == {"_id", "addr_city", "addr_zip", "tag"}
NestedPath    == ("_id"       :> <<"_id">>)
              @@ ("addr_city" :> <<"address", "city">>)
              @@ ("addr_zip"  :> <<"address", "zip">>)
              @@ ("tag"       :> <<"tag">>)
NestedPool    == << ("_id" :> NULL) @@ ("addr_city" :> Str("NYC"))
                    @@ ("addr_zip" :> I64("10001")) @@ ("tag" :> Str("x")) >>

\* fail: conf_fail.test, four rows staged into one batch whose unordered insert_many
\* rejects the third (_id 999 duplicates the seed) and inserts the other three, so
\* the durable set is the batch minus that one document.
FailWorkers == {"0"}
FailCols    == {"_id", "name"}
FailPath    == ("_id" :> <<"_id">>) @@ ("name" :> <<"name">>)
FailPool    == << ("_id" :> I64("1"))   @@ ("name" :> Str("a")),
                  ("_id" :> I64("2"))   @@ ("name" :> Str("b")),
                  ("_id" :> I64("999")) @@ ("name" :> Str("dup")),
                  ("_id" :> I64("3"))   @@ ("name" :> Str("c")) >>

\* typed: conf_typed.test, one row whose _id is a 24-hex string written as an
\* ObjectId ({"$oid": ...}). Drives the oid branch of Encode; n is int64, since
\* schema inference types every Mongo integer as BIGINT (the int32 branch is not
\* reachable through the inferred-schema insert path, README).
TypedWorkers == {"0"}
TypedCols    == {"_id", "n", "label"}
TypedPath    == ("_id" :> <<"_id">>) @@ ("n" :> <<"n">>) @@ ("label" :> <<"label">>)
TypedPool    == << ("_id" :> OID("0123456789abcdef01234567"))
                   @@ ("n" :> I64("42")) @@ ("label" :> Str("z")) >>

\* ctas: conf_ctas.test, a CREATE TABLE AS into a new collection. There is no
\* inferred schema, so columns are top-level and no _id is supplied; the column
\* types come from the SELECT, so an integer literal is INTEGER and serializes as
\* int32 ({"$numberInt": ...}), the branch the inferred-schema INSERT path cannot
\* reach.
CtasWorkers == {"0"}
CtasCols    == {"id","name"}
CtasPath    == ("id" :> <<"id">>) @@ ("name" :> <<"name">>)
CtasPool    == << ("id" :> I32("1")) @@ ("name" :> Str("ann")),
                  ("id" :> I32("2")) @@ ("name" :> Str("bo")) >>

\* types: conf_types.test, a CTAS over one row exercising the scalar encodings the
\* other scenarios do not: a double, a boolean, a DATE and a TIMESTAMP (both an
\* extended-JSON $date), beside a string. The millisecond values are the ones the
\* literals in the test produce. LIST, STRUCT and BLOB are the structured model.
TypesWorkers == {"0"}
TypesCols    == {"dbl","flag","d","ts","name"}
TypesPath    == ("dbl" :> <<"dbl">>) @@ ("flag" :> <<"flag">>) @@ ("d" :> <<"d">>)
             @@ ("ts" :> <<"ts">>) @@ ("name" :> <<"name">>)
TypesPool    == << ("dbl" :> Dbl("1.5")) @@ ("flag" :> Bool(TRUE))
                   @@ ("d" :> Dt("1623024000000")) @@ ("ts" :> Dt("1623053350000"))
                   @@ ("name" :> Str("plain")) >>

\* structured: conf_structured.test, a CTAS over one row exercising the nested
\* encodings the serializer now builds instead of stringifying: a LIST of INTEGER
\* as a BSON array of int32, a STRUCT as a subdocument, and a BLOB as
\* {"$binary": {"base64": ..., "subType": "00"}}. "3q0=" is the base64 of the two
\* bytes 0xDE 0xAD.
StructuredWorkers == {"0"}
StructuredCols    == {"arr", "obj", "bin", "name"}
StructuredPath    == ("arr" :> <<"arr">>) @@ ("obj" :> <<"obj">>)
                  @@ ("bin" :> <<"bin">>) @@ ("name" :> <<"name">>)
StructuredPool    == << ("arr" :> Lst(<<I32("1"), I32("2"), I32("3")>>))
                        @@ ("obj" :> Strct(("a" :> I32("1")) @@ ("b" :> Str("x"))))
                        @@ ("bin" :> Bin("3q0="))
                        @@ ("name" :> Str("plain")) >>

====
