#pragma once

#include "duckdb/function/copy_function.hpp"

namespace duckdb {

// Build the `mongo` COPY function, so `COPY (SELECT ...) TO 'alias.db.collection' (FORMAT mongo)` bulk-exports a query's
// rows into a MongoDB collection. The target names an attached MongoDB catalog (alias) and reuses its connection string;
// rows flow through the same DuckDB-value->BSON serializer and batched insert_many path as INSERT / CREATE TABLE AS.
CopyFunction GetMongoCopyFunction();

} // namespace duckdb
