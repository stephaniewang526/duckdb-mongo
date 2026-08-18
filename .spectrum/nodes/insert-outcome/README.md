# insert-outcome (L0)

The coarsest node of the spectrum: what one insert statement achieves, with no
mechanism. Authored by a coarsen above `mongo-insert` (L2), the node the spectrum
was seeded from.

## Placement

**L0.** A step reads the whole state (the global set of durable documents) and
settles a batch of it directly. There is no medium, no participant, no
coordinator: the workers, the per-worker buffers, the batch-size threshold, the
lock on the shared count, and the order documents are inserted in are all gone.
`Docs` is opaque, an abstract set the edge binds to the encodings of the input
rows, since the document structure is a data-representation detail the effect has
no use for.

## What it says

- **Insert(S)** makes a non-empty batch of not-yet-durable documents durable.
- **Abort(S)** makes a batch (possibly empty) durable and aborts, before any
  count is reported.
- **Report** reports the count once every document is durable.

State is `inserted` (the durable set), `aborted`, and `reported`. That is the
whole of it: the accounting variables the finer node coordinated through (`staged`,
`total`, `combined`, `snap`, `lockHolder`) collapse into "the count is the size of
`Docs`, reported when all of `Docs` is durable", and the batching mechanism
(`buffer`, `BatchSize`, per worker) collapses into "any non-empty batch".

## What disappeared, and what survives on purpose

Gone: the parallel workers and their buffers, the batch threshold, the count lock
and its read-modify-write, and the insertion order (`durable` was a sequence; here
it is a set). Kept: `inserted` (which documents are durable is the visible
effect), `aborted`, and `reported` (the INSERT count DuckDB returns). These are
what the intent decides over, not mechanism.

## Not over-specified

`Insert` absorbs **any** non-empty batch, and `Abort` any batch including the
empty one, so the node pins neither an order nor an atomicity beyond one batch.
That is what lets the finer node refine it while propagating in the per-worker
increments it actually uses: a step that absorbed all of `Docs` at once would
pin a granularity `mongo-insert` does not have. The failure is part of the effect,
not an added behavior: `mongo-insert` was seeded from code that can fail partway,
so its `FlushBatchFail` refines `Abort` directly rather than through a variant.

## Guarantees

The same accounting the finer node states, established here where the search is a
handful of states rather than 209:

- `NoCountOnFailure`: `aborted => reported = NONE`.
- `SuccessAccounting`: `reported # NONE => (reported = Cardinality(Docs) /\
  inserted = Docs)`.
- `NoOverInsert`: `Cardinality(inserted) <= Cardinality(Docs)`.

These do not travel up from `mongo-insert` on their own; asserting them here is a
separate claim, and worth making, since this is the cheapest place to establish
them. They then transfer down to `mongo-insert` through the `outcome-mongo`
mapping (edge README).

## Self-audit

| coarsen point | how it is met |
|---|---|
| mechanism collapsed to visible effects | workers/buffers/threshold/lock/order gone; `inserted`, `aborted`, `reported` kept |
| not a too-shallow coarsen | the staged steps are fused, not merely stripped of a medium: `Insert`/`Abort` act on the global durable set, no `StageRow`/`Combine` survives |
| intent not over-specified | `Insert`/`Abort` take any batch, pinning no order or atomicity |
| failure modelled, not forbidden | `Abort` is the coarse image of `FlushBatchFail`, so no variant is needed |
| guarantees stated at coarse grain | the three invariants above, over `inserted`/`reported`, not over any mechanism |

Every action is taken (`--coverage`): `Insert`, `Abort`, `Report`. The canonical
model binds `Docs` to three model values under `SYMMETRY Permutations(Docs)` (sound:
safety only, the actions quantify over subsets and no `CHOOSE` ranges over `Docs`).
The check finishes in well under ten seconds, so it needs no `sweep` model beside
it.
