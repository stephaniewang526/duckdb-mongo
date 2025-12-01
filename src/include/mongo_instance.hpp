#pragma once

#include <mongocxx/instance.hpp>

namespace duckdb {

// Get MongoDB instance (initialized once)
// This must be a single instance per process - MongoDB driver only allows one
// Defined in mongo_instance.cpp to ensure only one instance exists
mongocxx::instance &GetMongoInstance();

} // namespace duckdb
