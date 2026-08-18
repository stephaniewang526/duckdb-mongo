# mongo-insert (L2)

The v1 write path of the DuckDB MongoDB extension, seeded from the RFC
(discussion #47, the intent, linked in `spectrum.json`'s `seed.input`) together
with the code that implements it (`src/mongo_insert.cpp`, the `MongoInsertOperator`
sink, and its wiring in `MongoCatalog::PlanInsert`).

## Verdict

**What the system does.** `INSERT INTO mongo.<db>.<coll> SELECT ...` plans a
`MongoInsertOperator` sink over the query's rows. The sink runs as parallel
workers; each serializes every row it receives to a BSON document, stages it,
and flushes the buffer to the collection with `insert_many` once it reaches the
batch size (default 1000, overridable by `MONGO_INSERT_BATCH_SIZE`) and once more
at `Combine`. A lock sums each worker's row count into the statement's INSERT
count, which the source side reports. Serialization reverses the read path's
flattening (`address_city` mapped to `address.city` is written into a nested
`address` subdocument, siblings merged); a NULL cell is omitted, so a row with no
`_id` leaves MongoDB to generate one.

**Which layer: L2.** Each step is what one real worker does over its own buffer,
row count, and client, appending a batch to the shared collection. Written from
code, so the conformance edge `insert-cpp` carries the risk the other way.

**The failure semantics.** `insert_many` runs with `ordered: false` as the RFC
proposed, settled in one place (`MongoBatchSink::Flush`): a batch that hits a
duplicate `_id` inserts every other document and then throws, so the durable set
is the batch minus the rejected documents. All three RFC write forms are now
built:
`INSERT ... SELECT` and `CREATE TABLE AS` through `MongoInsertOperator`,
`COPY ... TO` through a separate `CopyFunction`. All three delegate staging and
flushing to the shared `MongoBatchSink`, which is where this node's `StageRow` and
`FlushBatchOk` correspond, so one spec covers every write path. `COPY` adds no
count of its own (DuckDB's `rows_copied`), so it exercises `StageRow`/`FlushBatchOk`
but never `Combine`/`Report`; the count guarantees below bind the operator paths
and are vacuous for `COPY`.

## The actions

- **StageRow(w)** serializes the next row (`Encode`) and stages it, counting it.
- **FlushBatchOk(w)** makes the staged batch durable and clears the buffer, at a
  flush point (`AtFlushPoint`: the buffer reached `BatchSize`, or the worker has
  staged everything and flushes the remainder at Combine).
- **FlushBatchFail(w)** models an unordered `insert_many` failing on some
  documents: every other document in the batch is inserted, so the durable set is
  the batch minus the rejected documents, and the uncaught exception aborts the
  statement.
- **CombineRead(w) / CombineWrite(w)** are the shared count update decomposed to
  the grain the code has (below): the read-modify-write of `insert_count` under
  `gstate.lock`, as an acquire-and-read step and a write-and-release step guarded
  by `lockHolder`.
- **Report** emits the count; its guard is the completeness reached (every worker
  combined, nothing aborted), so a run short of that reports nothing.

`Init` routes the input `Pool` to workers by any assignment of rows to workers,
so the check spans input distributions rather than one pinned split. `Encode` is
the serialization rule over the schema constants `Cols`/`Path`, with each cell in
canonical extended JSON: `int32` -> `{"$numberInt": ...}`, `int64` ->
`{"$numberLong": ...}`, `double`, `oid` -> `{"$oid": ...}`, DATE/TIMESTAMP ->
`{"$date": ...}`, a bare string or boolean, a LIST -> a BSON array, a STRUCT -> a
subdocument, and a BLOB -> `{"$binary": {"base64": ..., "subType": "00"}}`.
`EncodeCell` recurses for the array and the subdocument.

## Unordered inserts

`Flush` calls `insert_many` with `ordered:false` (`mongo_batch_sink.cpp`), so a
batch that hits a duplicate `_id` inserts every other document and then throws.
`FlushBatchFail` keeps that set durable: the batch minus a non-empty rejected
subset, in batch order (`Kept`). mongocxx still throws and nothing catches it, so
the statement aborts before any count. The durable set is not a prefix, which is
what `ReplayFail` verifies at the edge: the documents read back from MongoDB are a
proper sub-collection of the staged batch, and at least one staged document is
missing.

## Concurrency: the count lock is verified, not assumed

The sink runs multi-worker: `MongoInsertOperator::ParallelSink()` returns `true`,
`GetLocalSinkState` mints one `MongoInsertLocalState` per thread, and `Combine`
does `gstate.insert_count += lstate.row_count` under `lock_guard<mutex>`. The
shared count is a real read-modify-write under a lock. Rather than model that
update as one atomic step (which would assume the lock is correct), the node
decomposes it: `CombineRead(w)` acquires (`lockHolder = FREE` then `lockHolder' =
w`) and reads the count into `snap[w]`; `CombineWrite(w)` writes `snap[w] +
staged[w]` back and releases. Both the read and the write sit inside the critical
section `lockHolder` gates, as the code's read and write both sit inside the
`lock_guard` scope, so no other worker acts between them. `TotalAccounting` holds,
which verifies the lock serializes the update.

**The lock is load-bearing.** Drop the mutual exclusion from the shared-count
read-modify-write and `TotalAccounting` fails. The mutation: replace the
lock-guarded `CombineRead`/`CombineWrite` with an unsynchronised read-modify-write
(a per-worker read that owes a later write, with no `lockHolder`), so two workers
may sit between their read and write at once. TLC then finds `TotalAccounting`
violated at **depth 8**, the lost update: both workers finish (stage and flush),
both read `total = 0`, both write `snap + staged` back, and the second write
clobbers the first, leaving `total = 0` with both combined and `CombinedStaged =
2`. The safety is scale-dependent: with one worker there is no contention and the
lock is idle, so the guarantee is exercised only at the two-worker canonical
model. This is the result of a vacuity check, kept as the record rather than as a
committed broken specification, the same as the state-bug field check on the edge.

## The intent-to-delivery gap

The RFC's open question 3 asks whether a partial failure should report the
successful count or abort. The code does neither cleanly: the exception
propagates uncaught, so the statement **aborts and reports no count**, and with
the unordered insert the durable rows are the batch minus the rejected documents.
The insert is **not atomic**. The properties state the delivered guarantee:

- `SuccessAccounting`: a reported count equals the input size and every input row
  is durable.
- `NoCountOnFailure`: an aborted statement reports no count.
- `NoOverInsert`: `insert_many` never makes more documents durable than supplied.
- `TotalAccounting`: the count is exactly the combined workers' staged rows.

## Risk inventory

1. **The serializer** (`SerializeRow`, `AppendValueToDocument`, `EmitNode`): a
   wrong path split, mismerged sibling, omitted-vs-null cell, or wrong type
   encoding. Held to `Encode` at the edge (`ValidateStage`); the `state-bug`
   control breaks it.
2. **`_id` omission** (`SerializeRow` skips NULL cells): part of `Encode` via
   `Present`; the nested scenario drives the auto-generated `_id`.
3. **The count** (`row_count`, `insert_count`, the lock in `Combine`):
   `TotalAccounting`; the `swallow` control (reports a count without a valid
   flush) diverges at the edge.
4. **Durability under partial failure** (the uncaught throw, unordered):
   `FlushBatchFail`, `NoCountOnFailure`; the fail scenario conforms and the
   `fail-swallow` and `fail-state-bug` controls diverge at the edge.

## Choices where the input was silent

- **Nondeterministic failure position** in `FlushBatchFail`, over-approximating
  which document `insert_many` rejects.
- **Any row-to-worker assignment** in `Init`, since the partition is DuckDB's
  scheduling, not the operator's logic.
- **The flush point** is `AtFlushPoint`, the code's two flush triggers.

## Self-audit

| risk / guarantee | met by | falsified by |
|---|---|---|
| serializer correctness (1) | `Encode` + edge `ValidateStage` | edge `state-bug` control |
| `_id` omission (2) | `Encode`/`Present` | nested scenario auto-generates `_id` |
| count accounting (3) | `TotalAccounting` | a double-counting `Combine`; edge `swallow` control |
| durability on failure (4) | `FlushBatchFail` + `NoCountOnFailure` | edge `fail-swallow` control (count despite the throw) |
| completeness on success | `SuccessAccounting` | a `Report` that reads before draining |
| no over-insert | `NoOverInsert` | a flush that does not clear its buffer |
| batching boundary | `AtFlushPoint` + `BatchSize` | a flush before the threshold |
| count lock (concurrency) | `CombineRead`/`CombineWrite` gated by `lockHolder` | the no-lock scratch variant (violates `TotalAccounting`) |
| all four cell kinds | canonical schema (`c_id` oid, `c_i32` int32, `city` str, `zip` int64) | — |

Every action is taken in the canonical model (`--coverage`), `CombineRead`,
`CombineWrite` and `FlushBatchFail` among them. The canonical model distributes a
three-row pool over two workers under `SYMMETRY Permutations(Workers)` (sound:
safety only, `Init` and the invariants are permutation-invariant, no `CHOOSE`
ranges over `Workers`). Three rows reach a worker that flushes a full batch at the
boundary (`BatchSize` = 2) and then the remainder at drain, and a failing flush of
that buffer ranges over every non-empty rejected subset. The model has **237
distinct states**.

## Faithful, not complete

- **The failure path is now tied to code** through the edge's fail model: a real
  duplicate-key insert aborts, and the harness reads the durable subset back from
  MongoDB (edge README). What stays node-only is the multi-worker count lock,
  which the node verifies (CombineRead/CombineWrite under `lockHolder`) but the
  edge cannot drive, since DuckDB runs one sink worker at the replay sizes.
- **The `int32` cell kind is reached through `CREATE TABLE AS`, not `INSERT`.**
  Schema inference maps every Mongo integer to `BIGINT`
  (`mongo_schema_inference_helpers.cpp:260`), so the serializer's `INTEGER` ->
  int32 branch is unreachable on the inferred-schema insert path. `CREATE TABLE AS`
  takes its column types from the `SELECT`, where an integer literal is `INTEGER`,
  so the edge's `ctas` model drives int32; the node covers it too.
- **Scalar cell kinds** double, boolean, DATE and TIMESTAMP are modeled
  (`EncodeCell`) and conformed at the edge (the `types` scenario): a double is
  `{"$numberDouble": ...}`, a boolean the bare value, and a DATE or TIMESTAMP a
  `{"$date": {"$numberLong": "<ms>"}}` (DATE at midnight).
- **LIST, STRUCT and BLOB are modeled and conformed.** The RFC's type table calls
  for LIST -> Array, STRUCT -> Subdocument, BLOB -> Binary, and `CellToBson`
  (`src/mongo_insert.cpp`) builds each: a list recurses into a BSON array, a struct
  into a subdocument, a blob into `{"$binary": {"base64": ..., "subType": "00"}}`.
  `EncodeCell` recurses to match, and the edge's `structured` scenario drives all
  three (`conf_structured.test`).
