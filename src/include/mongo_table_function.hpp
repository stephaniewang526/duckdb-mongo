#pragma once

#include "duckdb.hpp"
#include "mongo_variant.hpp"
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace duckdb {

// Schema enforcement mode for handling type mismatches between MongoDB documents and expected schema
enum class SchemaMode {
	PERMISSIVE,    // Default: set invalid fields to NULL, keep all rows
	DROPMALFORMED, // Skip entire rows that have any schema violation
	FAILFAST       // Throw error immediately on first mismatch
};

// How schema inference represents values that do not fit a flat column: nested documents exposed as a whole,
// structures too deep or irregular to flatten, heterogeneous arrays, and unresolvable type conflicts.
enum class MongoNestedType {
	JSON,   // VARCHAR holding JSON text (default); conflicts resolve to the most frequent scalar type
	VARIANT // DuckDB VARIANT built from BSON; conflicts between unrelated types resolve to VARIANT
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
	//! Representation of nested/irregular values when the schema is inferred
	MongoNestedType nested_type;
	//! raw := true: no inference, the schema is (_id VARCHAR, doc VARIANT) with doc bound to the whole document
	bool raw;

	// Schema information
	vector<string> column_names;
	vector<LogicalType> column_types;
	// Mapping from flattened column name to original MongoDB path (for filter pushdown)
	// e.g., "address_city" -> "address.city", "l_returnflag" -> "l_returnflag"
	unordered_map<string, string> column_name_to_mongo_path;

	// Columns whose BSON type is k_oid (actual ObjectId), keyed by MongoDB path.
	// Used by filter pushdown to decide whether to send bsoncxx::oid vs plain string.
	std::unordered_set<std::string> objectid_columns;

	// Complex filter pushdown: MongoDB $expr queries for complex expressions
	bsoncxx::document::value complex_filter_expr;
	// Plain match terms consumed by pushdown_complex_filter (struct field predicates)
	bsoncxx::document::value match_filter;

	MongoScanData()
	    : sample_size(100), schema_mode(SchemaMode::PERMISSIVE), has_explicit_schema(false),
	      nested_type(MongoNestedType::JSON), raw(false),
	      complex_filter_expr(bsoncxx::builder::basic::document {}.extract()),
	      match_filter(bsoncxx::builder::basic::document {}.extract()) {
	}
};

// An output column DuckDB narrowed to a path inside a VARIANT column (SELECT doc.a.b), read straight from the
// document instead of materializing the whole VARIANT.
struct MongoExtractColumn {
	// Position in the output chunk
	idx_t output_idx;
	// BSON path: the column's MongoDB path segments followed by the extracted keys
	vector<string> path;
	// VARIANT, or the cast DuckDB pushed into the scan
	LogicalType type;
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
	// Output columns that are VARIANT paths (also listed in requested_column_* at their output position)
	vector<MongoExtractColumn> extract_columns;
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
bool ParseSchemaFromAtlasDocument(ClientContext &context, mongocxx::collection &collection,
                                  std::vector<std::string> &column_names, std::vector<LogicalType> &column_types,
                                  std::unordered_map<std::string, std::string> &column_name_to_mongo_path);

void ParseSchemaFromColumnsParameter(ClientContext &context, const Value &columns_value,
                                     std::vector<std::string> &column_names, std::vector<LogicalType> &column_types,
                                     std::unordered_map<std::string, std::string> &column_name_to_mongo_path);

void InferSchemaFromDocuments(mongocxx::collection &collection, int64_t sample_size,
                              std::vector<std::string> &column_names, std::vector<LogicalType> &column_types,
                              std::unordered_map<std::string, std::string> &column_name_to_mongo_path,
                              MongoNestedType nested_type = MongoNestedType::JSON);

void CollectFieldPaths(const bsoncxx::document::view &doc, const std::string &prefix, int depth,
                       std::unordered_map<std::string, std::vector<LogicalType>> &field_types,
                       std::unordered_map<std::string, std::string> &flattened_to_mongo_path,
                       const std::string &mongo_prefix = "", MongoNestedType nested_type = MongoNestedType::JSON);

LogicalType InferTypeFromBSON(const bsoncxx::document::element &element);

LogicalType ResolveTypeConflict(const std::vector<LogicalType> &types,
                                MongoNestedType nested_type = MongoNestedType::JSON);

// The raw := true schema: (_id VARCHAR, doc VARIANT) with doc mapped to $$ROOT.
void SetRawSchema(std::vector<std::string> &column_names, std::vector<LogicalType> &column_types,
                  std::unordered_map<std::string, std::string> &column_name_to_mongo_path);

// Parse the nested_type parameter / mongo_nested_type setting ('json' or 'variant', case-insensitive)
MongoNestedType ParseMongoNestedType(const std::string &value);

// Default nested representation for inferred schemas. Reads the `mongo_nested_type` setting; json when unset.
MongoNestedType MongoDefaultNestedType(ClientContext &context);

// SET callback for the `mongo_nested_type` extension option; rejects unknown values at SET time.
void MongoNestedTypeSetCallback(ClientContext &context, SetScope scope, Value &parameter);

// Parse schema mode from string (case-insensitive)
SchemaMode ParseSchemaMode(const std::string &mode_str);

// Convert schema mode to string for display
std::string SchemaModeToString(SchemaMode mode);

// Returns true if row is valid, false if row should be skipped (DROPMALFORMED)
// Throws exception in FAILFAST mode on schema violation
// variant_buffers (one per column) receives the cells of VARIANT columns; without it those cells are NULL
// skip_columns (one flag per column) marks output columns the caller fills itself
bool FlattenDocument(const bsoncxx::document::view &doc, const std::vector<std::string> &column_names,
                     const std::vector<LogicalType> &column_types, DataChunk &output, idx_t row_idx,
                     const std::unordered_map<std::string, std::string> &column_name_to_mongo_path,
                     SchemaMode schema_mode = SchemaMode::PERMISSIVE, bool has_explicit_schema = false,
                     vector<MongoVariantColumnBuffer> *variant_buffers = nullptr,
                     const vector<bool> *skip_columns = nullptr);

bool ValidateDocumentSchema(const bsoncxx::document::view &doc, const std::vector<std::string> &column_names,
                            const std::vector<LogicalType> &column_types,
                            const std::unordered_map<std::string, std::string> &column_name_to_mongo_path,
                            SchemaMode schema_mode);

// Probe one document to discover which fields have BSON ObjectId type
void DetectObjectIdColumns(mongocxx::collection &collection, std::unordered_set<std::string> &objectid_columns);

// Projection pushdown function
bsoncxx::document::value BuildMongoProjection(const vector<column_t> &column_ids,
                                              const vector<string> &all_column_names,
                                              const unordered_map<string, string> &column_name_to_mongo_path);

// find() projection for dotted MongoDB paths; a MONGO_ROOT_PATH entry (whole document) yields no projection.
bsoncxx::document::value BuildMongoProjectionFromPaths(vector<string> mongo_paths);

// Populate a MongoScanData's schema (column names/types, path map, ObjectId columns) and connection from its
// connection_string/database_name/collection_name. When schema_already_set is true, the columns are taken as given
// (e.g. from the `columns` parameter) and only the connection and ObjectId probe run. Shared by the mongo_scan bind
// and the catalog table entry.
void MongoBindSchema(ClientContext &context, MongoScanData &result, bool schema_already_set);

// Build the mongo_scan TableFunction with all pushdown callbacks wired. Used both for function registration and by the
// catalog table entry's GetScanFunction.
TableFunction GetMongoScanTableFunction();

// Default schema-inference sample size when mongo_scan is called without `sample_size` (including the views and table
// entries generated by ATTACH). Reads the `mongo_sample_size` setting; falls back to 100 when unset.
int64_t MongoDefaultSampleSize(ClientContext &context);

// Throws InvalidInputException unless 1 <= sample_size <= INT32_MAX ($sample takes an int32). `source` names the
// parameter or setting being validated for the error message.
void ValidateMongoSampleSize(int64_t sample_size, const char *source);

// SET callback for the `mongo_sample_size` extension option; rejects out-of-range values at SET time.
void MongoSampleSizeSetCallback(ClientContext &context, SetScope scope, Value &parameter);

class MongoClearCacheFunction : public TableFunction {
public:
	MongoClearCacheFunction();

	static void ClearMongoCaches(ClientContext &context);
};

} // namespace duckdb
