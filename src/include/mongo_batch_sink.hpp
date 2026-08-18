#pragma once

#include "duckdb/common/common.hpp"
#include <bsoncxx/document/value.hpp>
#include <mongocxx/collection.hpp>

namespace duckdb {

// Reusable batched writer to a MongoDB collection. It owns a staging buffer and flushes it with a single insert_many
// once batch_size documents accumulate. Both the INSERT sink operator and the COPY ... TO writer delegate their
// buffering, flushing, and collection-creation here, so the buffer/batch mechanics and the ordered-vs-unordered
// insert_many policy live in exactly one place. Callers layer their own count/lock/report on top (the INSERT operator
// tracks and reports the row count; COPY relies on DuckDB's rows_copied).
class MongoBatchSink {
public:
	explicit MongoBatchSink(idx_t batch_size);

	//! Stage one already-serialized document. Returns true once the buffer has reached batch_size, signalling the
	//! caller to Flush.
	bool Stage(bsoncxx::document::value doc);
	//! True when nothing is staged.
	bool Empty() const;
	//! Number of documents currently staged.
	idx_t BufferedCount() const;
	//! insert_many the staged documents (per the ordered policy fixed in the implementation) and clear the buffer.
	//! No-op when empty. Propagates any mongocxx exception (e.g. an ordered duplicate-key abort).
	void Flush(mongocxx::collection &collection);

	//! Create the collection if it does not yet exist, so a write that stages no documents still produces it. Swallows
	//! races and already-exists. Used by CREATE TABLE AS and COPY ... TO, which target a possibly-new collection.
	static void EnsureCollection(const string &connection_string, const string &database_name,
	                             const string &collection_name);

private:
	idx_t batch_size;
	vector<bsoncxx::document::value> buffer;
};

//! Resolve the insert/copy batch size: MONGO_INSERT_BATCH_SIZE when set to a positive integer, else 1000. A low value
//! lets a test drive several insert_many calls over a small input.
idx_t MongoResolveBatchSize();

} // namespace duckdb
