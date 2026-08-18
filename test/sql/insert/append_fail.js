// Harness-level capture of the ordered-failure abort, for the insert-cpp
// conformance edge. After conf_fail.test's insert_many aborts on a duplicate _id,
// this reads the durable documents back from MongoDB (the ordered prefix, sorted
// into insertion order by _id) and prints one FlushBatchFail event carrying them.
// The edge appends the printed line to the run's trace, so the replay can match
// the abort and validate the prefix against real execution. Not a shipped test.
const d = db.getSiblingDB("duckdb_mongo_test");
const docs = d.conf_fail.find({ _id: { $ne: NumberLong("999") } }).sort({ _id: 1 }).toArray();
const parts = docs.map((x) => EJSON.stringify(x, null, 0, { relaxed: false }));
print('{"action":"FlushBatchFail","w":"0","docs":[' + parts.join(",") + "]}");
