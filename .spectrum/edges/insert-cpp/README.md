# insert-cpp (conformance: mongo-insert -> cpp)

Holds the C++ write path to `MongoInsert` by replaying the DuckDB extension's own
sqllogictest suite. All three write forms conform to this one node, because they
share a batch-sink core: `INSERT` and `CREATE TABLE AS` go through
`MongoInsertOperator`, `COPY ... TO` through a separate `CopyFunction`
(`src/mongo_copy.cpp`), and all three delegate staging and flushing to
`MongoBatchSink` (`src/mongo_batch_sink.cpp`). The runner builds the instrumented
extension, drives one `.test` file per scenario against a live `mongod`, and
replays each trace through `Replay.tla`.

## Seams and events

The core seams live in the shared component, so every write path emits them; the
count seams are the operator's, which `COPY` does not have.

| action | seam site (file) | carries |
|---|---|---|
| `Init` | `GetGlobalSinkState` / `MongoCopyInitializeGlobal` | nothing |
| `StageRow` | `MongoBatchSink::Stage` (shared) | the serialized `doc` (canonical extended JSON) |
| `FlushBatchOk` | `MongoBatchSink::Flush` (shared) | nothing |
| `Combine` | operator `Combine`, after the count is summed under the lock | `w`, `total` |
| `Report` | operator `GetDataInternal`, after the count is set | `total` |

`StageRow` carries only the `doc`, since `MongoBatchSink` owns the buffer and the
document but not the operator's `row_count`; the row count is checked at `Combine`
and `Report`, which only the operator emits. A `COPY` run therefore emits `Init`,
`StageRow`, `FlushBatchOk` and stops: it never combines or reports (DuckDB counts
its rows), so the node's count guarantees are unexercised by it, honestly vacuous.

The code's `Combine` is one atomic step under `gstate.lock`, so it emits one
event; the node decomposes that read-modify-write into `CombineRead` and
`CombineWrite` to verify the lock. The replay walks a single worker, whose lock is
free throughout, so `ReplayCombine` takes their composition in one step, matching
the one event.

`ValidateStage` holds the reported `doc` to `Encode` of the row being staged: the
flatten-reversal and the canonical extended-JSON encoding are compared to what the
serializer produced. Dropping that conjunct makes a `state-bug` control conform
(checked for a `COPY` run too), which confirms the field, not a guard, catches it.

## The unordered-failure path, captured at the harness level

`insert_many` throws before the `FlushBatchOk` seam, so the abort carries no
shipped-code seam. The **fail** model drives a real duplicate-key failure
(`conf_fail.test`: the batch stages `_id` 1, 2, 999, 3; unordered, MongoDB inserts
1, 2 and 3, rejects 999, and the statement aborts). After the run,
`append_fail.js` reads the durable documents back from MongoDB and prints a
`FlushBatchFail` event carrying them; the run appends that line to the trace.
`ReplayFail` requires those documents to be a **proper sub-collection** of the
staged batch (every one was staged, at least one is missing) and leaves `reported`
at NONE (the aborted run emits no `Report`). This ties the abort, the surviving
set, and the absent count to real execution, not the model alone.

This is the one place the trace carries an event no shipped seam emits. It is a
harness-level observation of committed state, not an edit to the code: the seams
(comments in `mongo_batch_sink.cpp`, `mongo_insert.cpp`, and `mongo_copy.cpp`)
stay comments and the shipped program is unchanged.

## Models: the schema pinned to each run

| model | test | drives |
|---|---|---|
| flat | `insert_smoke.test`, batch 1000 | `int64` `_id`, `str`, one flush |
| flatperrow | `insert_smoke.test`, batch 1 | the same, one flush per row (`BatchSize` = 1) |
| nested | `conf_nested.test`, batch 1000 | the `address` subdocument, auto-generated `_id` |
| typed | `conf_typed.test`, batch 1000 | the `oid` branch (`{"$oid": ...}`), `int64`, `str` |
| ctas | `conf_ctas.test` and `conf_copy.test`, batch 1000 | `CREATE TABLE AS` and `COPY ... TO`, both flat with types from the SELECT; the `int32` branch (`{"$numberInt": ...}`) |
| types | `conf_types.test`, batch 1000 | the scalar encodings: `double`, `boolean`, DATE and TIMESTAMP (both `{"$date": ...}`) |
| structured | `conf_structured.test`, batch 1000 | the nested encodings: a LIST as a BSON array, a STRUCT as a subdocument, a BLOB as `{"$binary": ...}` |
| fail | `conf_fail.test`, batch 1000 | the unordered-failure abort and the surviving durable set |

The `ctas` model hosts two scenarios, `ctas` and `copy`: `CREATE TABLE AS` and
`COPY ... TO` produce the same flat, int32 schema, so they share one pinned model,
differing only in that the `COPY` run emits no `Combine`/`Report`.

Each model pins `Cols`, `Path`, `Pool` and `BatchSize` to its run. Across
them the driven runs reach `int32`, `int64`, `double`, `oid`, `str`, `bool`, the
`$date` form, the nested subdocument, the omitted-`_id` branch, the LIST, STRUCT
and BLOB encodings (the `structured` model: array, subdocument, `{"$binary": ...}`),
the batch boundary (flat vs flatperrow), and the abort.
The `int32` branch is unreachable through `INSERT`, since schema inference maps
every Mongo integer to `BIGINT` (`mongo_schema_inference_helpers.cpp:260`); the
`ctas` model reaches it because `CREATE TABLE AS` takes its column types from the
`SELECT`, where an integer literal is `INTEGER`.

## Controls (one or two per model, all must diverge)

- **bug** (flat, nested, typed, ctas): removes the final flush in the operator's
  `Combine`, so the worker combines with a non-empty buffer. Diverges at `Combine`,
  whose guard needs an empty buffer. An action the specification does not allow.
- **state-bug** (flat, flatperrow, nested, copy, types, structured, fail): appends
  `_BUG` to every serialized string in the shared serializer, so `StageRow` reports
  a `doc` that is not `Encode` of the row. Diverges at `StageRow`, caught only by
  `ValidateStage`. A right action, wrong result. The `copy` control run confirms a
  defect on the `COPY` path is caught, not only on the operator path.
- **swallow** (fail): catches the `insert_many` exception in `Combine` and
  reports a count anyway. Diverges at `Combine`: the code combines and reports
  without a valid flush, which the specification forbids (a non-empty buffer at
  Combine).

flatperrow uses `state-bug` alone: at batch 1 the buffer is empty at Combine, so
the `bug` patch is a no-op there.

## Non-vacuity and saturation, deliberately

Each model's replay is single-worker and its scheduling fixed, so each scenario
produces one recorded order: the models **saturate at one distinct trace**, below
the runner's floor of ten. This is the deterministic case the floor's warning
names. The concurrency the node models (several workers, the locked count) is
unreachable at these input sizes: DuckDB runs one sink worker for inputs this
small, and forcing a second needs an input too large for TLC to replay. Single
worker is representative of what the extension does on these statements; the node
verifies the multi-worker race exhaustively (its canonical model distributes a
pool over two workers), and the edge replays the sequential path it can drive.
The floor is cleared here by stating the saturation, not by more repeats.

**Which sizes carry the guarantee.** The node's properties hold at the sizes it
was checked at (sweep and canonical). The edge establishes that the code's steps
are steps the node allows on the driven runs, including the unordered-failure
abort; it does not re-establish the node's properties.

## Embedded build

The L3 node `cpp` names the code in place (`../../../src/`); the build closure
(the DuckDB tree and vcpkg dependencies) is too large to vendor, so
`overlay_build.sh` overlays the patched sources onto the checkout, rebuilds
`unittest` (which statically links the extension), and restores the originals.
The checkout is named by **`DUT_CHECKOUT`**, set when the check runs. Because
the overlay mutates that live tree in place, `overlay_build.sh` takes an atomic
`mkdir` lock (`.spectrum-overlay.lock` at the checkout root) for the
overlay-build-restore span and releases it on exit, so two runs on one checkout
cannot interleave; a second run fails fast, and a build killed with SIGKILL
leaves the lock and a half-patched tree, which the lock message says how to
clear.
Dependencies are the ones already installed in that `build/release`, not resolved
afresh, so the record claims conformance for the code under `source` with the
libraries the checkout had. `conf_reset.js` restores the conformance collections
to their schema-only baseline before each run; a live `mongod` on
`localhost:27017` with `MONGODB_TEST_DATABASE_AVAILABLE=1` is required.

Each run appends the `{post}` script's output to the trace: `conf_noop.js` (prints
nothing) for the normal scenarios, `append_fail.js` for the fail model.

## Added scenarios

`conf_nested.test`, `conf_typed.test`, `conf_ctas.test`, `conf_copy.test`,
`conf_types.test`, `conf_structured.test` and `conf_fail.test` are tests this
spectrum **added** to the suite (not present before), the only source of the
nested, oid, `CREATE TABLE AS`/int32, `COPY ... TO`, scalar-type, structured-type,
and unordered-failure coverage. They were added, not modified; `insert_smoke.test`
is used unchanged for the flat and flatperrow models.
