---- MODULE InsertOutcome ----
\* L0: the effect of one insert statement, with no mechanism. Documents from a
\* fixed set Docs become durable in batches; a failure aborts, leaving whatever
\* was already durable and reporting no count; a run that makes every document
\* durable reports the count.
\*
\* This is what the write path achieves, stated over the global set of durable
\* documents. The finer node MongoInsert refines it: its parallel workers, the
\* per-worker buffers, the batch-size threshold, the lock on the shared count, and
\* the order documents are inserted in are all mechanism this node hides. Docs is
\* opaque here (the edge binds it to the encodings of the input rows), since the
\* document structure is a data-representation detail the effect has no use for.
\*
\* The failure is part of the effect, not an added behavior: the finer node was
\* seeded from code that can fail partway, so its FlushBatchFail has a counterpart
\* here (Abort) and refines it directly, rather than through a variant.
EXTENDS Naturals, FiniteSets

CONSTANTS
  Docs,   \* the documents the statement inserts (opaque; the edge binds it).
  NONE    \* the value reported holds until (and unless) the count is reported.

VARIABLES
  inserted,   \* the documents made durable so far.
  aborted,    \* whether a failure aborted the statement.
  reported    \* the reported INSERT count, or NONE.

vars == <<inserted, aborted, reported>>

Init ==
  /\ inserted = {}
  /\ aborted  = FALSE
  /\ reported = NONE

\* A successful flush makes a non-empty batch of not-yet-durable documents
\* durable. Any non-empty batch: the node fixes no order and no atomicity beyond
\* one batch, which is what leaves the finer node free to propagate in the
\* per-worker increments it uses.
Insert(S) ==
  /\ ~aborted
  /\ S \subseteq (Docs \ inserted)
  /\ S # {}
  /\ inserted' = inserted \cup S
  /\ UNCHANGED <<aborted, reported>>

\* A failing flush makes its durable batch (possibly empty) durable and aborts.
\* No count is reported thereafter.
Abort(S) ==
  /\ ~aborted
  /\ reported = NONE
  /\ S \subseteq (Docs \ inserted)
  /\ inserted' = inserted \cup S
  /\ aborted'  = TRUE
  /\ UNCHANGED reported

\* The source reports the count once every document is durable.
Report ==
  /\ ~aborted
  /\ inserted = Docs
  /\ reported = NONE
  /\ reported' = Cardinality(Docs)
  /\ UNCHANGED <<inserted, aborted>>

Next ==
  \/ Report
  \/ \E S \in SUBSET Docs : Insert(S) \/ Abort(S)

Spec == Init /\ [][Next]_vars

====
