# Spectrum: MongoDB extension write path

A spectrum of the DuckDB MongoDB extension's v1 write support, seeded from the
RFC (discussion #47, the link `seed.input` records) and `src/mongo_insert.cpp`.
It sits inside the
extension's checkout (embedded): the L3 node names the code in place and the
conformance edge builds it there.

- **insert-outcome** (L0): what an insert statement achieves, with no mechanism,
  over `InsertOutcome.tla`.
- **mongo-insert** (L2): what the insert operator does, over `MongoInsert.tla`.
- **cpp** (L3): the extension, held in place at `../../../src/`.
- **outcome-mongo** (refinement): `mongo-insert` refines `insert-outcome`.
- **insert-cpp** (conformance): holds the C++ code to `mongo-insert` by replaying
  the extension's own sqllogictest suite.

The graph and every obligation's status, cost and evidence are derived from the
ledger, not stored here:

```bash
python3 <spectrum-playground>/scripts/ledger.py .
```

The conformance edge needs `DUT_CHECKOUT` set to this repository's root and a
live `mongod` on `localhost:27017`; see `edges/insert-cpp/README.md`. To
re-verify:

```bash
DUT_CHECKOUT=<repo-root> \
  python3 <spectrum-playground>/scripts/spectrum_check/spectrum_check.py .
```
