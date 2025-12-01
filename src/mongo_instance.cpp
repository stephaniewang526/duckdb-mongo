#include "mongo_instance.hpp"

namespace duckdb {

// Global MongoDB instance (initialized once at static initialization)
// This must be a single instance per process - MongoDB driver only allows one
// Using a static global variable in a single .cpp file ensures only one instance exists
static mongocxx::instance g_mongo_instance {};

mongocxx::instance &GetMongoInstance() {
	return g_mongo_instance;
}

} // namespace duckdb
