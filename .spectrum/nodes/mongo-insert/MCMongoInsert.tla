---- MODULE MCMongoInsert ----
\* Harness for MongoInsert: TypeOK, the accounting and durability properties, and
\* the canonical model's schema and input. The schema (Cols, Path) is a general
\* rule the model binds. canonical distributes an input pool over two workers
\* (Init routes rows to workers, so the check spans every distribution), reaches
\* all four serialized kinds in one schema, and uses a batch size the pool
\* crosses. It finishes well under ten seconds, so it needs no sweep model beside
\* it (ARCHITECTURE.md §5).
EXTENDS MongoInsert, TLC, FiniteSetsExt

AllRows     == SE!ToSet(Pool)
EncodedDocs == {Encode(r) : r \in AllRows}

TypeOK ==
  /\ pending  \in [Workers -> Seq(AllRows)]
  /\ buffer   \in [Workers -> Seq(EncodedDocs)]
  /\ staged   \in [Workers -> Nat]
  /\ durable  \in Seq(EncodedDocs)
  /\ total    \in Nat
  /\ combined \in [Workers -> BOOLEAN]
  /\ aborted  \in BOOLEAN
  /\ reported \in (Nat \cup {NONE})
  /\ lockHolder \in (Workers \cup {FREE})
  /\ snap     \in [Workers -> Nat]

InputLen       == Len(Pool)
CombinedStaged == MapThenSumSet(LAMBDA w : IF combined[w] THEN staged[w] ELSE 0, Workers)

\* The INSERT count is exactly the row_count of the workers that combined; a
\* double count or a count of unstaged rows breaks it.
TotalAccounting == total = CombinedStaged

\* A flush that fails aborts the statement, which reports no count. This is the
\* delivered guarantee in place of an atomic insert: on partial failure the
\* count is absent, not a tally of the rows that made it.
NoCountOnFailure == aborted => (reported = NONE)

\* When a count is reported, every input row is durable and the count equals the
\* input size. The completeness the source's guard stands for.
SuccessAccounting == (reported # NONE) => (reported = InputLen /\ Len(durable) = InputLen)

\* insert_many never makes more documents durable than were supplied.
NoOverInsert == Len(durable) <= InputLen

\* ----------------------------------------------------------------------------
\* An optimisation for TLC, not a property it checks: MongoInsert is symmetric in
\* Workers (Init quantifies over assignments of rows to workers, the invariants
\* sum uniformly over Workers, and no CHOOSE ranges over Workers), and the check
\* is safety only, so exploring one representative per permutation is sound.
Symmetry == Permutations(Workers)

\* ---- cell constructors and the canonical dataset ----
I32(s) == [type |-> "int32", s |-> s]
I64(s) == [type |-> "int64", s |-> s]
OID(s) == [type |-> "oid",   s |-> s]
Str(s) == [type |-> "str",   s |-> s]

\* canonical: two workers, an input pool Init distributes over them, and a schema
\* reaching every serialized kind: c_id is an ObjectId, c_i32 an int32, city a
\* string nested under address, zip an int64 nested under address. Cn_r2 omits
\* c_id, so MongoDB would generate it.
CanonCols == {"c_id", "c_i32", "city", "zip"}
CanonPath == ("c_id"  :> <<"_id">>) @@ ("c_i32" :> <<"n">>)
          @@ ("city"  :> <<"addr", "city">>) @@ ("zip" :> <<"addr", "zip">>)
Cn_r1 == ("c_id" :> OID("a")) @@ ("c_i32" :> I32("7")) @@ ("city" :> Str("NYC")) @@ ("zip" :> I64("10001"))
Cn_r2 == ("c_id" :> NULL)     @@ ("c_i32" :> I32("8")) @@ ("city" :> Str("LA"))  @@ ("zip" :> I64("90001"))
Cn_r3 == ("c_id" :> OID("c")) @@ ("c_i32" :> I32("9")) @@ ("city" :> Str("SF"))  @@ ("zip" :> I64("94101"))
\* Three rows over two workers with BatchSize 2: the all-to-one assignment stages
\* three into one worker, so it flushes a full batch at the boundary and then the
\* remainder at drain, and a failing flush of that three-document buffer reaches
\* prefix lengths 0..2.
CanonPool == <<Cn_r1, Cn_r2, Cn_r3>>

====
