#define DUCKDB_EXTENSION_MAIN

#include "mongo_extension.hpp"
#include "mongo_storage_extension.hpp"
#include "mongo_instance.hpp"
#include "mongo_table_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

// Forward declarations (functions are defined in mongo_table_function.cpp)
unique_ptr<FunctionData> MongoScanBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names);
unique_ptr<LocalTableFunctionState> MongoScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                       GlobalTableFunctionState *global_state);
void MongoScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);

static void LoadInternal(ExtensionLoader &loader) {
	// Register MongoDB table function
	TableFunction mongo_scan("mongo_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                         MongoScanFunction, MongoScanBind, nullptr, MongoScanInitLocal);

	// Add optional parameters
	mongo_scan.named_parameters["filter"] = LogicalType::VARCHAR;
	mongo_scan.named_parameters["sample_size"] = LogicalType::BIGINT;

	// Register the table function
	loader.RegisterFunction(mongo_scan);

	// Register MongoDB clear cache function
	MongoClearCacheFunction clear_cache_func;
	loader.RegisterFunction(clear_cache_func);

	// Register MongoDB storage extension for ATTACH support
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.storage_extensions["mongo"] = MongoStorageExtension::Create();
}

void MongoExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string MongoExtension::Name() {
	return "mongo";
}

std::string MongoExtension::Version() const {
#ifdef EXT_VERSION_MONGO
	return EXT_VERSION_MONGO;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(mongo, loader) {
	duckdb::LoadInternal(loader);
}
}
