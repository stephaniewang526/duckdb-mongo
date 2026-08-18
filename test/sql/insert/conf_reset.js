// Reset the conformance collections to their schema-only baseline, so each
// replayed run inserts into a clean collection: repeats never collide on _id and
// never accumulate rows. Driven by the insert-cpp conformance edge before each
// unittest invocation. Not part of the shipped test suite.
const d = db.getSiblingDB("duckdb_mongo_test");
d.conformance_smoke.drop();
d.conformance_smoke.insertOne({ _id: 0, name: "schema" });
d.conf_nested.drop();
d.conf_nested.insertOne({ _id: ObjectId("000000000000000000000000"), address: { city: "seed", zip: 0 }, tag: "seed" });
d.conf_fail.drop();
d.conf_fail.insertOne({ _id: NumberLong(999), name: "seed" });
d.conf_typed.drop();
d.conf_typed.insertOne({ _id: ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"), n: NumberInt(1), label: "seed" });
d.ctas_out.drop();
d.copy_out.drop();
d.conf_types.drop();
d.conf_structured.drop();
