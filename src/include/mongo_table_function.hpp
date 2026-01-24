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

// Schema enforcement mode for handling type mismatches between MongoDB documents and expected schema
enum class SchemaMode {
	PERMISSIVE,    // Default: set invalid fields to NULL, keep all rows
	DROPMALFORMED, // Skip entire rows that have any schema violation
	FAILFAST       // Throw error immediately on first mismatch
};

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
	//! Optional MongoDB aggregation pipeline (JSON array string). When set, the scan uses `aggregate(...)` instead of
	//! `find(...)`. Schema must be provided via `columns` for non-collection-shaped results.
	std::string pipeline_json;
	int64_t sample_size;
	//! Schema enforcement mode: controls behavior when document fields don't match expected types
	SchemaMode schema_mode;
	//! Whether an explicit schema was provided (only enforce schema_mode when true)
	bool has_explicit_schema;

	// Schema information
	vector<string> column_names;
	vector<LogicalType> column_types;
	// Mapping from flattened column name to original MongoDB path (for filter pushdown)
	// e.g., "address_city" -> "address.city", "l_returnflag" -> "l_returnflag"
	unordered_map<string, string> column_name_to_mongo_path;

	// Complex filter pushdown: MongoDB $expr queries for complex expressions
	bsoncxx::document::value complex_filter_expr;

	MongoScanData()
	    : sample_size(100), schema_mode(SchemaMode::PERMISSIVE), has_explicit_schema(false),
	      complex_filter_expr(bsoncxx::builder::basic::document {}.extract()) {
	}
};

struct MongoScanState : public LocalTableFunctionState {
	shared_ptr<MongoConnection> connection;
	std::string database_name;
	std::string collection_name;
	std::string filter_query;
	std::string pipeline_json;
	int64_t limit = -1;
	unique_ptr<mongocxx::cursor> cursor;
	unique_ptr<mongocxx::cursor::iterator> current;
	unique_ptr<mongocxx::cursor::iterator> end;
	bool finished = false;
	// Projection information: which columns are requested from MongoDB
	vector<idx_t> requested_column_indices;
	vector<string> requested_column_names;
	vector<LogicalType> requested_column_types;
	// Keep projection document alive for the lifetime of the cursor
	bsoncxx::document::value projection_document;
	// Keep pipeline document alive for the lifetime of the cursor (aggregate path)
	bsoncxx::document::value pipeline_document;

	MongoScanState()
	    : limit(-1), finished(false), projection_document(bsoncxx::builder::basic::document {}.extract()),
	      pipeline_document(bsoncxx::builder::basic::document {}.extract()) {
	}
};

// Schema inference functions
bool ParseSchemaFromAtlasDocument(mongocxx::collection &collection, std::vector<std::string> &column_names,
                                  std::vector<LogicalType> &column_types,
                                  std::unordered_map<std::string, std::string> &column_name_to_mongo_path);

void ParseSchemaFromColumnsParameter(ClientContext &context, const Value &columns_value,
                                     std::vector<std::string> &column_names, std::vector<LogicalType> &column_types,
                                     std::unordered_map<std::string, std::string> &column_name_to_mongo_path);

void InferSchemaFromDocuments(mongocxx::collection &collection, int64_t sample_size,
                              std::vector<std::string> &column_names, std::vector<LogicalType> &column_types,
                              std::unordered_map<std::string, std::string> &column_name_to_mongo_path);

void CollectFieldPaths(const bsoncxx::document::view &doc, const std::string &prefix, int depth,
                       std::unordered_map<std::string, std::vector<LogicalType>> &field_types,
                       std::unordered_map<std::string, std::string> &flattened_to_mongo_path,
                       const std::string &mongo_prefix = "");

LogicalType InferTypeFromBSON(const bsoncxx::document::element &element);

LogicalType ResolveTypeConflict(const std::vector<LogicalType> &types);

// Parse schema mode from string (case-insensitive)
SchemaMode ParseSchemaMode(const std::string &mode_str);

// Convert schema mode to string for display
std::string SchemaModeToString(SchemaMode mode);

// Returns true if row is valid, false if row should be skipped (DROPMALFORMED)
// Throws exception in FAILFAST mode on schema violation
bool FlattenDocument(const bsoncxx::document::view &doc, const std::vector<std::string> &column_names,
                     const std::vector<LogicalType> &column_types, DataChunk &output, idx_t row_idx,
                     const std::unordered_map<std::string, std::string> &column_name_to_mongo_path,
                     SchemaMode schema_mode = SchemaMode::PERMISSIVE, bool has_explicit_schema = false);

bool ValidateDocumentSchema(const bsoncxx::document::view &doc, const std::vector<std::string> &column_names,
                            const std::vector<LogicalType> &column_types,
                            const std::unordered_map<std::string, std::string> &column_name_to_mongo_path,
                            SchemaMode schema_mode);

// Projection pushdown function
bsoncxx::document::value BuildMongoProjection(const vector<column_t> &column_ids,
                                              const vector<string> &all_column_names,
                                              const unordered_map<string, string> &column_name_to_mongo_path);

class MongoClearCacheFunction : public TableFunction {
public:
	MongoClearCacheFunction();

	static void ClearMongoCaches(ClientContext &context);
};

} // namespace duckdb
