#define DUCKDB_EXTENSION_MAIN

#include "mongo_extension.hpp"
#include "mongo_storage_extension.hpp"
#include "mongo_instance.hpp"
#include "mongo_table_function.hpp"
#include "mongo_optimizer.hpp"
#include "mongo_secrets.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

// Forward declarations (functions are defined in mongo_table_function.cpp)
unique_ptr<FunctionData> MongoScanBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names);
unique_ptr<LocalTableFunctionState> MongoScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                       GlobalTableFunctionState *global_state);
void MongoScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
void MongoPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
                                vector<unique_ptr<Expression>> &filters);
InsertionOrderPreservingMap<string> MongoScanToString(TableFunctionToStringInput &input);

static void LoadInternal(ExtensionLoader &loader) {
	// Register MongoDB table function
	TableFunction mongo_scan("mongo_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                         MongoScanFunction, MongoScanBind, nullptr, MongoScanInitLocal);

	// Add optional parameters
	mongo_scan.named_parameters["filter"] = LogicalType::VARCHAR;
	mongo_scan.named_parameters["sample_size"] = LogicalType::BIGINT;
	mongo_scan.named_parameters["columns"] = LogicalType::ANY;
	mongo_scan.named_parameters["pipeline"] = LogicalType::VARCHAR;

	// Enable filter pushdown
	mongo_scan.filter_pushdown = true;
	// Enable projection pushdown
	mongo_scan.projection_pushdown = true;
	// Enable filter pruning: filter columns that aren't used elsewhere don't need to be fetched
	mongo_scan.filter_prune = true;
	// Enable complex filter pushdown
	mongo_scan.pushdown_complex_filter = MongoPushdownComplexFilter;
	// EXPLAIN visibility
	mongo_scan.to_string = MongoScanToString;

	// Create TableFunctionInfo with description and comment
	TableFunctionSet mongo_scan_set("mongo_scan");
	mongo_scan_set.AddFunction(std::move(mongo_scan));
	CreateTableFunctionInfo mongo_scan_info(std::move(mongo_scan_set));

	// Set description
	FunctionDescription mongo_scan_desc;
	mongo_scan_desc.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	mongo_scan_desc.parameter_names = {"connection_string", "database", "collection"};
	mongo_scan_desc.description = "Scans a MongoDB collection and returns its contents as a table. Supports optional "
	                              "filter and sample_size parameters.";
	mongo_scan_desc.examples.push_back("SELECT * FROM mongo_scan('mongodb://localhost:27017', 'mydb', 'mycollection')");
	mongo_scan_desc.examples.push_back("SELECT * FROM mongo_scan('mongodb://localhost:27017', 'mydb', 'mycollection', "
	                                   "filter := '{\"status\": \"active\"}')");
	mongo_scan_info.descriptions.push_back(std::move(mongo_scan_desc));

	// Set comment
	mongo_scan_info.comment = Value("Table function to query MongoDB collections directly. Use filter parameter for "
	                                "MongoDB-specific query operators.");

	// Register the table function
	loader.RegisterFunction(std::move(mongo_scan_info));

	// Register MongoDB clear cache function
	MongoClearCacheFunction clear_cache_func;
	TableFunctionSet clear_cache_set("mongo_clear_cache");
	clear_cache_set.AddFunction(std::move(clear_cache_func));
	CreateTableFunctionInfo clear_cache_info(std::move(clear_cache_set));

	// Set description
	FunctionDescription clear_cache_desc;
	clear_cache_desc.description =
	    "Clears the schema cache for all attached MongoDB databases. Useful when MongoDB schema changes.";
	clear_cache_desc.examples.push_back("SELECT * FROM mongo_clear_cache()");
	clear_cache_info.descriptions.push_back(std::move(clear_cache_desc));

	// Set comment
	clear_cache_info.comment = Value(
	    "Invalidates cached schema information for MongoDB collections. Call this after schema changes in MongoDB.");

	// Register the table function
	loader.RegisterFunction(std::move(clear_cache_info));

	// Register MongoDB secret type
	SecretType secret_type;
	secret_type.name = "mongo";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	loader.RegisterSecretType(secret_type);

	CreateSecretFunction mongo_secret_function = {"mongo", "config", CreateMongoSecretFunction};
	SetMongoSecretParameters(mongo_secret_function);
	loader.RegisterFunction(mongo_secret_function);

	// Register MongoDB storage extension for ATTACH support
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.storage_extensions["mongo"] = MongoStorageExtension::Create();

	// Register optimizer extension (runs after DuckDB built-in optimizers)
	OptimizerExtension opt_ext;
	opt_ext.optimize_function = MongoOptimizerOptimize;
	config.optimizer_extensions.push_back(opt_ext);
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
