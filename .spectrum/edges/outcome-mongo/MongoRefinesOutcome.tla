---- MODULE MongoRefinesOutcome ----
\* Refinement edge: MongoInsert refines InsertOutcome.
\*
\* This harness builds on the finer node's specification and instantiates the
\* coarser one under the refinement mapping, a state function with no auxiliary
\* variable: the coarser node's set of durable documents is the set of documents
\* in the finer node's durable sequence, and the coarser Docs is the set of
\* documents the input rows encode to. aborted and reported carry over unchanged.
\*
\* How each finer step lands:
\*   - StageRow, CombineRead, CombineWrite change only mechanism the mapping does
\*     not read (buffers, staged, the count, the lock), so they are stutters;
\*   - FlushBatchOk adds its batch's documents to durable, which is Insert of that
\*     batch (non-empty, and fresh since a document is inserted once);
\*   - FlushBatchFail adds its ordered prefix and aborts, which is Abort of that
\*     prefix (possibly empty);
\*   - Report sets reported to the count, which is the coarser Report, since every
\*     worker having combined means every document is durable.
\*
\* The dataset mirrors MCMongoInsert's canonical model: the runner assembles the
\* finer node's specification, not its harness, so the pool the configuration binds
\* is restated here, kept identical so the two nodes' verdicts and state counts are
\* comparable.
\*
\* The lifted specification is aliased because a .cfg PROPERTY cannot name an
\* instance-qualified operator: PROPERTY IO!Spec does not parse.
EXTENDS MongoInsert, TLC

DocSet == {Encode(r) : r \in SE!ToSet(Pool)}

IO == INSTANCE InsertOutcome WITH
        inserted <- SE!ToSet(durable),
        aborted  <- aborted,
        reported <- reported,
        Docs     <- DocSet,
        NONE     <- NONE

RefinesOutcome == IO!Spec

\* ---- cell constructors and the datasets (mirror of MCMongoInsert) ----
I32(s) == [type |-> "int32", s |-> s]
I64(s) == [type |-> "int64", s |-> s]
OID(s) == [type |-> "oid",   s |-> s]
Str(s) == [type |-> "str",   s |-> s]

CanonCols == {"c_id","c_i32","city","zip"}
CanonPath == ("c_id"  :> <<"_id">>) @@ ("c_i32" :> <<"n">>)
          @@ ("city"  :> <<"addr","city">>) @@ ("zip" :> <<"addr","zip">>)
Cn_r1 == ("c_id" :> OID("a")) @@ ("c_i32" :> I32("7")) @@ ("city" :> Str("NYC")) @@ ("zip" :> I64("10001"))
Cn_r2 == ("c_id" :> NULL)     @@ ("c_i32" :> I32("8")) @@ ("city" :> Str("LA"))  @@ ("zip" :> I64("90001"))
Cn_r3 == ("c_id" :> OID("c")) @@ ("c_i32" :> I32("9")) @@ ("city" :> Str("SF"))  @@ ("zip" :> I64("94101"))
CanonPool == <<Cn_r1, Cn_r2, Cn_r3>>

\* ----------------------------------------------------------------------------
\* An optimisation for TLC, not a property it checks: a refinement of the
\* fairness-free InsertOutcome is a safety obligation, and MongoInsert is
\* symmetric in Workers with a mapping (SE!ToSet(durable), DocSet) that names no
\* worker, so exploring one representative per permutation is sound.
Symmetry == Permutations(Workers)

====
