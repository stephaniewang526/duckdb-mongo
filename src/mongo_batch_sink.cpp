#include "mongo_batch_sink.hpp"
#include "mongo_instance.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/options/insert.hpp>
#include <cstdlib>

namespace duckdb {

MongoBatchSink::MongoBatchSink(idx_t batch_size_p) : batch_size(batch_size_p) {
}

bool MongoBatchSink::Stage(bsoncxx::document::value doc) {
	// spectrum: StageRow doc~=bsoncxx::to_json(doc.view(), bsoncxx::ExtendedJsonMode::k_canonical)
	buffer.push_back(std::move(doc));
	return buffer.size() >= batch_size;
}

bool MongoBatchSink::Empty() const {
	return buffer.empty();
}

idx_t MongoBatchSink::BufferedCount() const {
	return buffer.size();
}

void MongoBatchSink::Flush(mongocxx::collection &collection) {
	if (buffer.empty()) {
		return;
	}
	// The RFC's open question -- ordered vs unordered inserts -- is settled here, in the one place every write path
	// flushes. v1 uses unordered inserts (ordered:false) for throughput, as the RFC proposed: a batch that hits a
	// duplicate _id inserts every other document and then reports the failures, so on a partial failure the durable set
	// is the batch minus the rejected documents rather than the prefix before the first one. mongocxx still throws, so
	// the statement aborts with no count; the fail conformance scenario pins the durable set down.
	collection.insert_many(buffer, mongocxx::options::insert {}.ordered(false));
	// spectrum: FlushBatchOk
	buffer.clear();
}

void MongoBatchSink::EnsureCollection(const string &connection_string, const string &database_name,
                                      const string &collection_name) {
	GetMongoInstance();
	try {
		auto client = mongocxx::client(mongocxx::uri(connection_string));
		auto database = client[database_name];
		if (!database.has_collection(collection_name)) {
			database.create_collection(collection_name);
		}
	} catch (const std::exception &) {
		// A race or an already-existing collection is harmless; the caller's write still runs.
	}
}

idx_t MongoResolveBatchSize() {
	idx_t batch_size = 1000;
	const char *env_batch = std::getenv("MONGO_INSERT_BATCH_SIZE");
	if (env_batch != nullptr) {
		auto parsed = std::atoi(env_batch);
		if (parsed > 0) {
			batch_size = NumericCast<idx_t>(parsed);
		}
	}
	return batch_size;
}

} // namespace duckdb
