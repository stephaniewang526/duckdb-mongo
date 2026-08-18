# outcome-mongo (refinement: mongo-insert -> insert-outcome)

`mongo-insert` (L2) refines `insert-outcome` (L0): every behavior the mechanism
allows is one the effect allows. Authored with the coarsen that produced
`insert-outcome`; the mapping lives here, in the edge harness, so `mongo-insert`'s
own record is untouched.

## The mapping

A **state function**, the weakest rung that holds, with no auxiliary variable:

| coarse variable | finer expression |
|---|---|
| `inserted` | `SE!ToSet(durable)` (the set of documents in the durable sequence) |
| `aborted` | `aborted` |
| `reported` | `reported` |
| `Docs` (constant) | `{Encode(r) : r \in SE!ToSet(Pool)}` (the input rows' encodings) |

No history variable is needed, because `inserted` is a function of the finer
node's current `durable`: two paths that reach the same `durable` map to the same
coarse state, and the coarse node fixes no order for them to disagree on.

## How each finer step lands

- **StageRow, CombineRead, CombineWrite** change only mechanism the mapping does
  not read (`buffer`, `staged`, `total`, `combined`, `snap`, `lockHolder`), so
  they are stutters.
- **FlushBatchOk** adds its batch to `durable`, which is `Insert` of that batch:
  non-empty (the flush guard), and fresh, since a document is inserted once.
- **FlushBatchFail** adds its ordered prefix and sets `aborted`, which is `Abort`
  of that prefix (possibly empty). One finer step, one coarse step, because
  `Abort` absorbs a batch and aborts together.
- **Report** sets `reported` to the count, which is the coarse `Report`: every
  worker having combined means every buffer flushed and (`~aborted`) every
  document durable, so `SE!ToSet(durable) = Docs` and the finer `total = Len(Pool)
  = Cardinality(Docs)`.

`Insert` taking any non-empty batch is what makes FlushBatchOk land whatever the
buffer held; had the coarse node absorbed all of `Docs` at once, the per-worker
flushes would have had no coarse image.

## Property transfer

The mapping leaves `aborted` and `reported` alone and computes `inserted` from
`durable`, so `NoCountOnFailure` and `SuccessAccounting` transfer to `mongo-insert`
verbatim over `aborted`/`reported`, and `NoOverInsert` transfers as the statement
about `Cardinality(SE!ToSet(durable))`. `mongo-insert` also asserts these over its
own variables directly, so the two readings are both checked.

## Models and evidence

The edge declares `mongo-insert`'s canonical model so the counts line up (two
workers, a three-row pool, `BatchSize` 2). With a pure state-function mapping the
edge explores exactly the finer node's reachable states, **209 distinct**, the
same as `mongo-insert`'s own check, which is the tell that the mapping adds no
auxiliary state. It finishes in well under ten seconds, so there is no `sweep`
model beside it. `SYMMETRY Permutations(Workers)` is sound
here: the refinement of a fairness-free coarser node is a safety obligation, and
the mapping (`SE!ToSet(durable)`, `DocSet`) names no worker.

The datasets are mirrored from `MCMongoInsert` rather than shared, since the runner
assembles the finer node's specification and not its harness; they are kept
identical to that node's canonical model.
