---- MODULE MCInsertOutcome ----
\* Harness for InsertOutcome: TypeOK and the accounting guarantees, stated at the
\* effect grain. The same guarantees the finer node carries, established here where
\* the state space is a handful rather than hundreds.
EXTENDS InsertOutcome, TLC

TypeOK ==
  /\ inserted \in SUBSET Docs
  /\ aborted  \in BOOLEAN
  /\ reported \in (Nat \cup {NONE})

\* An aborted statement reports no count.
NoCountOnFailure == aborted => (reported = NONE)

\* When a count is reported, every document is durable and the count is the input
\* size.
SuccessAccounting == (reported # NONE) => (reported = Cardinality(Docs) /\ inserted = Docs)

\* No more documents are durable than were supplied.
NoOverInsert == Cardinality(inserted) <= Cardinality(Docs)

\* ----------------------------------------------------------------------------
\* An optimisation for TLC, not a property it checks: InsertOutcome is symmetric
\* in Docs (the actions quantify over subsets of Docs and no CHOOSE ranges over
\* it), and the check is safety only, so one representative per permutation is
\* sound.
Symmetry == Permutations(Docs)

====
