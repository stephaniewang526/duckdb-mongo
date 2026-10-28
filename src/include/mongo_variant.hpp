#pragma once

#include "duckdb.hpp"

#include <bsoncxx/types/bson_value/view.hpp>
#include <string>

namespace duckdb {

// MongoDB path that maps a VARIANT column to the whole document instead of one field.
extern const char *const MONGO_ROOT_PATH;

// Collects the BSON values of one VARIANT column while a chunk is being scanned. The cursor only keeps a document
// alive until it advances, so each cell is copied here and the VARIANT vector is built once per chunk.
class MongoVariantColumnBuffer {
public:
	void Set(idx_t row, const bsoncxx::types::bson_value::view &value);
	void SetNull(idx_t row);
	// Writes rows [0, count) to result and clears the buffer.
	void Build(idx_t count, Vector &result);

private:
	// One wrapper document {"": value} per row; an empty string is a SQL NULL.
	vector<std::string> cells;
};

} // namespace duckdb
