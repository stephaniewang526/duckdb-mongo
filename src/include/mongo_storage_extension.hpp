#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class MongoStorageExtension {
public:
	static unique_ptr<StorageExtension> Create();
};

} // namespace duckdb
