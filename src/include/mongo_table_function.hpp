#pragma once

#include "duckdb.hpp"
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {

struct MongoConnection {
	std::string connection_string;
	mongocxx::client client;

	MongoConnection(const std::string &conn_str) : connection_string(conn_str), client(mongocxx::uri(conn_str)) {
	}
};

struct MongoScanData : public TableFunctionData {
	std::string connection_string;
	shared_ptr<MongoConnection> connection;
	std::string database_name;
	std::string collection_name;
	std::string filter_query;
	int64_t sample_size;

	// Schema information
	vector<string> column_names;
	vector<LogicalType> column_types;
	// Mapping from flattened column name to original MongoDB path (for filter pushdown)
	// e.g., "address_city" -> "address.city", "l_returnflag" -> "l_returnflag"
	unordered_map<string, string> column_name_to_mongo_path;

	MongoScanData() : sample_size(100) {
	}
};

struct MongoScanState : public LocalTableFunctionState {
	shared_ptr<MongoConnection> connection;
	std::string database_name;
	std::string collection_name;
	std::string filter_query;
	int64_t limit = -1;
	unique_ptr<mongocxx::cursor> cursor;
	unique_ptr<mongocxx::cursor::iterator> current;
	unique_ptr<mongocxx::cursor::iterator> end;
	bool finished = false;

	MongoScanState() : limit(-1), finished(false) {
	}
};

// Schema inference functions
void InferSchemaFromDocuments(mongocxx::collection &collection, int64_t sample_size,
                              std::vector<std::string> &column_names, std::vector<LogicalType> &column_types,
                              std::unordered_map<std::string, std::string> &column_name_to_mongo_path);

void CollectFieldPaths(const bsoncxx::document::view &doc, const std::string &prefix, int depth,
                       std::unordered_map<std::string, std::vector<LogicalType>> &field_types,
                       std::unordered_map<std::string, std::string> &flattened_to_mongo_path);

LogicalType InferTypeFromBSON(const bsoncxx::document::element &element);

LogicalType ResolveTypeConflict(const std::vector<LogicalType> &types);

void FlattenDocument(const bsoncxx::document::view &doc, const std::vector<std::string> &column_names,
                     const std::vector<LogicalType> &column_types, DataChunk &output, idx_t row_idx);

// Filter pushdown functions
bsoncxx::document::value ConvertFiltersToMongoQuery(optional_ptr<TableFilterSet> filters,
                                                    const vector<string> &column_names,
                                                    const vector<LogicalType> &column_types);

class MongoClearCacheFunction : public TableFunction {
public:
	MongoClearCacheFunction();

	static void ClearMongoCaches(ClientContext &context);
};

} // namespace duckdb
