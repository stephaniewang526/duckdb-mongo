#include "mongo_table_function.hpp"
#include "mongo_duckdb_utils.hpp"
#include "schema/mongo_schema_inference_internal.hpp"

#include "duckdb/common/string_util.hpp"

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/collection.hpp>

#include <mongocxx/pipeline.hpp>

#include <algorithm>
#include <cctype>
#include <map>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace duckdb {

// Wrapper for document::element (backward compatibility)
LogicalType InferTypeFromBSON(const bsoncxx::document::element &element) {
	return InferTypeFromBSONElement(element);
}

// Parse schema mode from string (case-insensitive)
SchemaMode ParseSchemaMode(const std::string &mode_str) {
	std::string lower = StringUtil::Lower(mode_str);
	if (lower == "permissive") {
		return SchemaMode::PERMISSIVE;
	} else if (lower == "dropmalformed" || lower == "drop_malformed") {
		return SchemaMode::DROPMALFORMED;
	} else if (lower == "failfast" || lower == "fail_fast") {
		return SchemaMode::FAILFAST;
	}
	throw InvalidInputException("Invalid schema_mode '%s'. Valid options: 'permissive', 'dropmalformed', 'failfast'",
	                            mode_str);
}

// Convert schema mode to string for display
std::string SchemaModeToString(SchemaMode mode) {
	switch (mode) {
	case SchemaMode::PERMISSIVE:
		return "permissive";
	case SchemaMode::DROPMALFORMED:
		return "dropmalformed";
	case SchemaMode::FAILFAST:
		return "failfast";
	default:
		return "unknown";
	}
}

MongoNestedType ParseMongoNestedType(const std::string &value) {
	std::string lower = StringUtil::Lower(value);
	if (lower == "json") {
		return MongoNestedType::JSON;
	} else if (lower == "variant") {
		return MongoNestedType::VARIANT;
	}
	throw InvalidInputException("Invalid nested_type '%s'. Valid options: 'json', 'variant'", value);
}

void SetRawSchema(std::vector<std::string> &column_names, std::vector<LogicalType> &column_types,
                  std::unordered_map<std::string, std::string> &column_name_to_mongo_path) {
	column_names = {"_id", "doc"};
	column_types = {LogicalType::VARCHAR, LogicalType::VARIANT()};
	column_name_to_mongo_path = {{"_id", "_id"}, {"doc", MONGO_ROOT_PATH}};
}

namespace {

// True for the inference placeholders SQLNULL and LIST(...(SQLNULL)), which must never become a column type
bool IsUnknownPlaceholder(const LogicalType &type) {
	if (type.id() == LogicalTypeId::SQLNULL) {
		return true;
	}
	return type.id() == LogicalTypeId::LIST && IsUnknownPlaceholder(ListType::GetChildType(type));
}

// Variant-mode conflict resolution: only lossless merges stay typed, anything else becomes VARIANT.
// SQLNULL marks a null value (no type information); LIST(SQLNULL) marks an empty array (any element type).
LogicalType ResolveTypeConflictVariant(const std::vector<LogicalType> &types) {
	std::vector<LogicalType> typed;
	for (const auto &type : types) {
		if (type.id() != LogicalTypeId::SQLNULL) {
			typed.push_back(type);
		}
	}
	if (typed.empty()) {
		return LogicalType::VARIANT();
	}
	bool all_same = true;
	bool all_list = true;
	bool all_struct = true;
	for (const auto &type : typed) {
		all_same = all_same && type == typed[0];
		all_list = all_list && type.id() == LogicalTypeId::LIST;
		all_struct = all_struct && type.id() == LogicalTypeId::STRUCT;
	}
	if (all_same && !IsUnknownPlaceholder(typed[0])) {
		return typed[0];
	}
	if (all_struct) {
		// Union of the fields; a field seen with different types is resolved recursively
		std::map<std::string, std::vector<LogicalType>> merged;
		for (const auto &type : typed) {
			for (idx_t i = 0; i < StructType::GetChildCount(type); i++) {
				merged[StructType::GetChildName(type, i).GetIdentifierName()].push_back(
				    StructType::GetChildType(type, i));
			}
		}
		child_list_t<LogicalType> children;
		for (auto &pair : merged) {
			MongoChildListAppend(children, pair.first, ResolveTypeConflictVariant(pair.second));
		}
		return LogicalType::STRUCT(children);
	}
	if (all_list) {
		std::vector<LogicalType> children;
		for (const auto &type : typed) {
			children.push_back(ListType::GetChildType(type));
		}
		auto child = ResolveTypeConflictVariant(children);
		if (child.id() == LogicalTypeId::VARIANT) {
			return child;
		}
		return LogicalType::LIST(child);
	}

	bool only_numeric = true;
	bool only_temporal = true;
	for (const auto &type : typed) {
		if (type != LogicalType::BIGINT && type != LogicalType::DOUBLE) {
			only_numeric = false;
		}
		if (type != LogicalType::DATE && type != LogicalType::TIMESTAMP) {
			only_temporal = false;
		}
	}
	if (only_numeric) {
		return LogicalType::DOUBLE;
	}
	if (only_temporal) {
		return LogicalType::TIMESTAMP;
	}
	return LogicalType::VARIANT();
}

} // namespace

LogicalType ResolveTypeConflict(const std::vector<LogicalType> &types, MongoNestedType nested_type) {
	if (nested_type == MongoNestedType::VARIANT) {
		return ResolveTypeConflictVariant(types);
	}
	if (types.empty()) {
		return LogicalType::VARCHAR;
	}

	// If all types are the same, return that type
	bool all_same = true;
	for (size_t i = 1; i < types.size(); i++) {
		if (types[i] != types[0]) {
			all_same = false;
			break;
		}
	}
	if (all_same) {
		return types[0];
	}

	int list_count = 0;
	int struct_count = 0;
	for (const auto &type : types) {
		if (type.id() == LogicalTypeId::LIST) {
			list_count++;
		} else if (type.id() == LogicalTypeId::STRUCT) {
			struct_count++;
		}
	}

	if (list_count > 0) {
		LogicalType deepest_list_type;
		int max_depth = 0;
		for (const auto &type : types) {
			if (type.id() == LogicalTypeId::LIST) {
				int depth = 0;
				LogicalType current_type = type;
				while (current_type.id() == LogicalTypeId::LIST) {
					depth++;
					current_type = ListType::GetChildType(current_type);
				}
				if (depth > max_depth) {
					max_depth = depth;
					deepest_list_type = type;
				}
			}
		}
		if (max_depth > 0) {
			// For LIST(STRUCT) types, merge struct fields from all variants so that
			// optional fields present in only some documents are not lost.
			auto inner = ListType::GetChildType(deepest_list_type);
			if (inner.id() == LogicalTypeId::STRUCT) {
				std::map<std::string, LogicalType> merged;
				for (const auto &type : types) {
					if (type.id() != LogicalTypeId::LIST) {
						continue;
					}
					auto child = ListType::GetChildType(type);
					if (child.id() != LogicalTypeId::STRUCT) {
						continue;
					}
					for (idx_t i = 0; i < StructType::GetChildCount(child); i++) {
						auto &name = StructType::GetChildName(child, i).GetIdentifierName();
						if (merged.find(name) == merged.end()) {
							merged[name] = StructType::GetChildType(child, i);
						}
					}
				}
				child_list_t<LogicalType> children;
				for (auto &pair : merged) {
					MongoChildListAppend(children, pair.first, pair.second);
				}
				return LogicalType::LIST(LogicalType::STRUCT(children));
			}
			return deepest_list_type;
		}
		for (const auto &type : types) {
			if (type.id() == LogicalTypeId::LIST) {
				return type;
			}
		}
	}

	if (struct_count > 0) {
		for (const auto &type : types) {
			if (type.id() == LogicalTypeId::STRUCT) {
				return type;
			}
		}
	}

	// For mixed types, use a priority-based approach that considers both
	// the type's ability to represent other types and the frequency of each type
	// Count occurrences of each type
	int double_count = 0;
	int bigint_count = 0;
	int varchar_count = 0;
	int boolean_count = 0;
	int timestamp_count = 0;

	for (const auto &type : types) {
		if (type == LogicalType::DOUBLE) {
			double_count++;
		} else if (type == LogicalType::BIGINT) {
			bigint_count++;
		} else if (type == LogicalType::VARCHAR) {
			varchar_count++;
		} else if (type == LogicalType::BOOLEAN) {
			boolean_count++;
		} else if (type == LogicalType::TIMESTAMP) {
			timestamp_count++;
		}
	}

	size_t total_count = types.size();

	// Strategy: Prefer types that can represent other types, but also consider frequency
	// If VARCHAR is a strong majority (>70%), prefer VARCHAR (most flexible for truly mixed data)
	if (varchar_count > total_count * 7 / 10) {
		return LogicalType::VARCHAR;
	}

	// If DOUBLE is present and represents a significant portion (>=30%), prefer DOUBLE
	// DOUBLE can represent integers, so it's more flexible than BIGINT
	if (double_count > 0 && double_count >= total_count * 3 / 10) {
		return LogicalType::DOUBLE;
	}

	// If BIGINT is present and represents a significant portion (>=30%), prefer BIGINT
	if (bigint_count > 0 && bigint_count >= total_count * 3 / 10) {
		return LogicalType::BIGINT;
	}

	// For boolean and timestamp, require strong majority (>=70%) as they're less flexible
	if (boolean_count >= total_count * 7 / 10) {
		return LogicalType::BOOLEAN;
	}

	if (timestamp_count >= total_count * 7 / 10) {
		return LogicalType::TIMESTAMP;
	}

	// If we have any DOUBLE (even minority), prefer DOUBLE (can represent integers too)
	// This handles cases where most values are numeric but some are missing/null/string
	// This is a reasonable default since DOUBLE is more flexible than VARCHAR for numeric operations
	if (double_count > 0) {
		return LogicalType::DOUBLE;
	}

	// If we have any BIGINT, prefer BIGINT
	if (bigint_count > 0) {
		return LogicalType::BIGINT;
	}

	// If we have BOOLEAN, prefer BOOLEAN
	if (boolean_count > 0) {
		return LogicalType::BOOLEAN;
	}

	// If we have TIMESTAMP, prefer TIMESTAMP
	if (timestamp_count > 0) {
		return LogicalType::TIMESTAMP;
	}

	// Otherwise default to VARCHAR (most flexible)
	return LogicalType::VARCHAR;
}

void CollectFieldPaths(const bsoncxx::document::view &doc, const std::string &prefix, int depth,
                       std::unordered_map<std::string, std::vector<LogicalType>> &field_types,
                       std::unordered_map<std::string, std::string> &flattened_to_mongo_path,
                       const std::string &mongo_prefix, MongoNestedType nested_type) {
	const int MAX_DEPTH = 5;
	// Representation of anything that is not flattened: JSON string or VARIANT
	const LogicalType fallback = MongoFallbackType(nested_type);

	if (depth > MAX_DEPTH) {
		if (!prefix.empty()) {
			field_types[prefix].push_back(fallback);
		}
		return;
	}

	for (const auto &element : doc) {
		std::string field_name = std::string(element.key().data(), element.key().length());
		std::string full_path = prefix.empty() ? field_name : prefix + "_" + field_name;
		// Track original MongoDB path: use dots for nested fields, original name for top-level
		std::string mongo_path = mongo_prefix.empty() ? field_name : mongo_prefix + "." + field_name;
		flattened_to_mongo_path[full_path] = mongo_path;

		switch (element.type()) {
		case bsoncxx::type::k_document: {
			// Recursively process nested document to create flattened child columns.
			auto nested_doc = element.get_document().value;
			CollectFieldPaths(nested_doc, full_path, depth + 1, field_types, flattened_to_mongo_path, mongo_path,
			                  nested_type);
			// Also expose the parent document itself as a whole, so users can query the sub-document directly.
			field_types[full_path].push_back(fallback);
			break;
		}
		case bsoncxx::type::k_array: {
			auto array = element.get_array().value;
			if (array.begin() == array.end()) {
				// Empty array: element type unknown. Variant mode marks it compatible with any LIST.
				field_types[full_path].push_back(
				    nested_type == MongoNestedType::VARIANT ? LogicalType::LIST(LogicalType::SQLNULL) : fallback);
				break;
			}

			// Check first element to determine array type
			auto first_element = *array.begin();

			if (first_element.type() == bsoncxx::type::k_document) {
				// Array of objects - infer STRUCT type and create LIST(STRUCT(...))
				LogicalType struct_type = InferStructTypeFromArray(array, depth, nested_type);
				if (struct_type.id() == LogicalTypeId::STRUCT) {
					field_types[full_path].push_back(LogicalType::LIST(struct_type));
				} else {
					field_types[full_path].push_back(fallback);
				}
			} else if (first_element.type() == bsoncxx::type::k_array) {
				// Array of arrays - recursively infer nested array type
				LogicalType nested_list_type = InferNestedArrayType(array, depth, nested_type);
				if (nested_list_type.id() == LogicalTypeId::LIST) {
					field_types[full_path].push_back(LogicalType::LIST(nested_list_type));
				} else {
					field_types[full_path].push_back(fallback);
				}
			} else {
				field_types[full_path].push_back(InferPrimitiveArrayType(array, nested_type));
			}
			break;
		}
		case bsoncxx::type::k_null:
		case bsoncxx::type::k_undefined: {
			// In variant mode a null says nothing about the type; json mode keeps the VARCHAR placeholder
			field_types[full_path].push_back(nested_type == MongoNestedType::VARIANT ? LogicalType::SQLNULL
			                                                                         : InferTypeFromBSON(element));
			break;
		}
		default: {
			// Atomic type
			LogicalType type = InferTypeFromBSON(element);
			field_types[full_path].push_back(type);
			break;
		}
		}
	}
}

static LogicalType ParseLogicalTypeFromString(const std::string &type_str, ClientContext &context) {
	return TransformStringToLogicalType(type_str, context);
}

bool ParseSchemaFromAtlasDocument(ClientContext &context, mongocxx::collection &collection,
                                  std::vector<string> &column_names, std::vector<LogicalType> &column_types,
                                  std::unordered_map<string, string> &column_name_to_mongo_path) {
	// Check for __schema document in the collection (for Atlas SQL users)
	bsoncxx::builder::basic::document filter_builder;
	filter_builder.append(bsoncxx::builder::basic::kvp("_id", "__schema"));
	auto filter = filter_builder.extract();

	auto schema_doc = collection.find_one(filter.view());
	if (!schema_doc) {
		return false;
	}

	auto doc_view = schema_doc->view();
	bsoncxx::document::view schema_doc_view;

	// Check if schema is in a nested "schema" field, or directly in the document
	auto schema_element = doc_view["schema"];
	if (schema_element && schema_element.type() == bsoncxx::type::k_document) {
		// Schema is nested: { "_id": "__schema", "schema": { "field1": "VARCHAR", ... } }
		schema_doc_view = schema_element.get_document().value;
	} else {
		// Schema is directly in the document: { "_id": "__schema", "field1": "VARCHAR", ... }
		schema_doc_view = doc_view;
	}

	// Parse schema document - expected format: { "field1": "VARCHAR", "field2": "BIGINT", ... }
	// Or could be nested: { "field1": { "type": "VARCHAR", "path": "field1" }, ... }
	for (auto it = schema_doc_view.begin(); it != schema_doc_view.end(); ++it) {
		std::string field_name = std::string(it->key().data(), it->key().length());

		// Skip _id and "schema" fields (metadata, not actual schema fields)
		if (field_name == "_id" || field_name == "schema") {
			continue;
		}

		LogicalType field_type;
		std::string mongo_path = field_name;

		if (it->type() == bsoncxx::type::k_string) {
			// Simple format: "field": "VARCHAR"
			std::string type_str(it->get_string().value.data(), it->get_string().value.length());
			field_type = ParseLogicalTypeFromString(type_str, context);
		} else if (it->type() == bsoncxx::type::k_document) {
			// Nested format: "field": { "type": "VARCHAR", "path": "field.path" }
			auto field_doc = it->get_document().value;
			auto type_elem = field_doc["type"];
			if (type_elem && type_elem.type() == bsoncxx::type::k_string) {
				std::string type_str(type_elem.get_string().value.data(), type_elem.get_string().value.length());
				field_type = ParseLogicalTypeFromString(type_str, context);
			} else {
				continue; // Skip invalid entries
			}

			auto path_elem = field_doc["path"];
			if (path_elem && path_elem.type() == bsoncxx::type::k_string) {
				mongo_path = std::string(path_elem.get_string().value.data(), path_elem.get_string().value.length());
			}
		} else {
			continue; // Skip invalid entries
		}

		column_names.push_back(field_name);
		column_types.push_back(field_type);
		column_name_to_mongo_path[field_name] = mongo_path;
	}

	// Only ensure _id exists (add it if missing)
	bool has_id = false;
	for (size_t i = 0; i < column_names.size(); i++) {
		if (column_names[i] == "_id") {
			has_id = true;
			break;
		}
	}

	if (!has_id) {
		column_names.push_back("_id");
		column_types.push_back(LogicalType::VARCHAR);
		column_name_to_mongo_path["_id"] = "_id";
	}

	return !column_names.empty();
}

void ParseSchemaFromColumnsParameter(ClientContext &context, const Value &columns_value,
                                     std::vector<string> &column_names, std::vector<LogicalType> &column_types,
                                     std::unordered_map<string, string> &column_name_to_mongo_path) {
	auto &child_type = columns_value.type();
	if (child_type.id() != LogicalTypeId::STRUCT) {
		throw BinderException("mongo_scan \"columns\" parameter requires a struct as input.");
	}

	auto &struct_children = StructValue::GetChildren(columns_value);
	D_ASSERT(StructType::GetChildCount(child_type) == struct_children.size());

	for (idx_t i = 0; i < struct_children.size(); i++) {
		auto &name = StructType::GetChildName(child_type, i).GetIdentifierName();
		auto &val = struct_children[i];
		if (val.IsNull()) {
			throw BinderException("mongo_scan \"columns\" parameter type specification cannot be NULL.");
		}

		LogicalType field_type;
		std::string mongo_path = name;

		if (val.type().id() == LogicalTypeId::VARCHAR) {
			// Simple format: column name -> type string
			field_type = TransformStringToLogicalType(StringValue::Get(val), context);
		} else if (val.type().id() == LogicalTypeId::STRUCT) {
			// Nested format: column name -> { "type": "VARCHAR", "path": "field.path" }
			auto &nested_children = StructValue::GetChildren(val);
			auto &nested_type = val.type();

			// Look for "type" field
			bool found_type = false;
			for (idx_t j = 0; j < nested_children.size(); j++) {
				auto &nested_name = StructType::GetChildName(nested_type, j).GetIdentifierName();
				if (StringUtil::Lower(nested_name) == "type") {
					auto &type_val = nested_children[j];
					if (type_val.type().id() == LogicalTypeId::VARCHAR) {
						field_type = TransformStringToLogicalType(StringValue::Get(type_val), context);
						found_type = true;
					}
					break;
				}
			}

			if (!found_type) {
				throw BinderException("mongo_scan \"columns\" parameter nested struct must contain a \"type\" field.");
			}

			// Look for "path" field (optional)
			for (idx_t j = 0; j < nested_children.size(); j++) {
				auto &nested_name = StructType::GetChildName(nested_type, j).GetIdentifierName();
				if (StringUtil::Lower(nested_name) == "path") {
					auto &path_val = nested_children[j];
					if (path_val.type().id() == LogicalTypeId::VARCHAR) {
						mongo_path = StringValue::Get(path_val);
					}
					break;
				}
			}
		} else {
			throw BinderException("mongo_scan \"columns\" parameter type specification must be VARCHAR or STRUCT.");
		}

		column_names.push_back(name);
		column_types.push_back(field_type);
		column_name_to_mongo_path[name] = mongo_path;
	}

	D_ASSERT(column_names.size() == column_types.size());
	if (column_names.empty()) {
		throw BinderException("mongo_scan \"columns\" parameter needs at least one column.");
	}

	// Preserve DuckDB's struct order - don't reorder as DuckDB has already bound the query
	// Only ensure _id exists (add it if missing)
	bool has_id = false;
	for (size_t i = 0; i < column_names.size(); i++) {
		if (column_names[i] == "_id") {
			has_id = true;
			break;
		}
	}

	if (!has_id) {
		column_names.push_back("_id");
		column_types.push_back(LogicalType::VARCHAR);
		column_name_to_mongo_path["_id"] = "_id";
	}
}

void InferSchemaFromDocuments(mongocxx::collection &collection, int64_t sample_size, std::vector<string> &column_names,
                              std::vector<LogicalType> &column_types,
                              std::unordered_map<string, string> &column_name_to_mongo_path,
                              MongoNestedType nested_type) {
	std::unordered_map<std::string, std::vector<LogicalType>> field_types;

	// Use $sample aggregation for random sampling to better capture heterogeneous schemas.
	// Collections with optional fields (e.g., completion timestamps only on finished records)
	// need random sampling to discover all fields, since find().limit() only returns the
	// first N documents in natural order.
	int64_t count = 0;
	try {
		mongocxx::pipeline pipe;
		pipe.sample(static_cast<int32_t>(std::min(sample_size, static_cast<int64_t>(INT32_MAX))));
		auto cursor = collection.aggregate(pipe);
		for (const auto &doc : cursor) {
			CollectFieldPaths(doc, "", 0, field_types, column_name_to_mongo_path, "", nested_type);
			count++;
			if (count >= sample_size) {
				break;
			}
		}
	} catch (...) {
		// Fall back to find().limit() if $sample is unavailable (e.g., views, older MongoDB).
		count = 0;
		field_types.clear();
		column_name_to_mongo_path.clear();
		mongocxx::options::find opts;
		opts.limit(sample_size);
		auto cursor = collection.find({}, opts);
		for (const auto &doc : cursor) {
			CollectFieldPaths(doc, "", 0, field_types, column_name_to_mongo_path, "", nested_type);
			count++;
			if (count >= sample_size) {
				break;
			}
		}
	}

	// Always include _id column (present in all MongoDB documents)
	// If collection is empty, we still need at least one column
	if (field_types.find("_id") == field_types.end()) {
		field_types["_id"] = {LogicalType::VARCHAR}; // ObjectId as string
		column_name_to_mongo_path["_id"] = "_id";    // Map _id to itself
	}

	// Build column names and types from collected field paths.
	// Use case-insensitive deduplication: DuckDB's binder uses a case_insensitive_map for
	// column names, so "clientFullname" and "ClientFullname" would collide. MongoDB field
	// names are case-sensitive, so random sampling can yield both variants from the same
	// logical field — producing an intermittent "duplicate column name" binder error.
	// When a collision is detected, merge the types and keep the first-seen column name.
	std::unordered_map<std::string, size_t> lower_name_to_idx; // lowercase name -> index in column_names

	// Ensure _id is always first
	if (field_types.find("_id") != field_types.end()) {
		column_names.push_back("_id");
		LogicalType resolved_type = ResolveTypeConflict(field_types["_id"], nested_type);
		column_types.push_back(resolved_type);
		lower_name_to_idx["_id"] = 0;
	}

	// Add all other columns (excluding _id which we already added)
	for (const auto &pair : field_types) {
		if (pair.first == "_id") {
			continue;
		}
		std::string lower_name = StringUtil::Lower(pair.first);
		auto it = lower_name_to_idx.find(lower_name);
		if (it != lower_name_to_idx.end()) {
			// Case-insensitive collision: merge types into the existing column entry.
			size_t existing_idx = it->second;
			std::vector<LogicalType> merged = {column_types[existing_idx]};
			for (const auto &t : pair.second) {
				merged.push_back(t);
			}
			column_types[existing_idx] = ResolveTypeConflict(merged, nested_type);
			continue;
		}
		lower_name_to_idx[lower_name] = column_names.size();
		column_names.push_back(pair.first);
		LogicalType resolved_type = ResolveTypeConflict(pair.second, nested_type);
		column_types.push_back(resolved_type);
	}

	// Ensure we have at least one column (should always have _id, but double-check)
	if (column_names.empty()) {
		column_names.push_back("_id");
		column_types.push_back(LogicalType::VARCHAR);
	}
}

namespace {

void CollectObjectIdFieldsRecursive(const bsoncxx::document::view &doc, const std::string &mongo_prefix,
                                    std::unordered_set<std::string> &objectid_columns) {
	for (const auto &elem : doc) {
		std::string field_name(elem.key().data(), elem.key().length());
		std::string mongo_path = mongo_prefix.empty() ? field_name : mongo_prefix + "." + field_name;

		if (elem.type() == bsoncxx::type::k_oid) {
			objectid_columns.insert(mongo_path);
		} else if (elem.type() == bsoncxx::type::k_document) {
			CollectObjectIdFieldsRecursive(elem.get_document().value, mongo_path, objectid_columns);
		}
	}
}

} // namespace

void DetectObjectIdColumns(mongocxx::collection &collection, std::unordered_set<std::string> &objectid_columns) {
	mongocxx::options::find opts;
	opts.limit(1);
	auto cursor = collection.find({}, opts);
	for (const auto &doc : cursor) {
		CollectObjectIdFieldsRecursive(doc, "", objectid_columns);
	}
}

// Validation-only function that checks schema compatibility without writing to output
// Used for COUNT(*) queries where we need to validate but not materialize data
bool ValidateDocumentSchema(const bsoncxx::document::view &doc, const std::vector<string> &column_names,
                            const std::vector<LogicalType> &column_types,
                            const std::unordered_map<string, string> &column_name_to_mongo_path,
                            SchemaMode schema_mode) {
	for (idx_t col_idx = 0; col_idx < column_names.size(); col_idx++) {
		const auto &column_name = column_names[col_idx];
		const auto &column_type = column_types[col_idx];

		// Skip complex types (LIST, STRUCT) - they have their own conversion logic; VARIANT accepts any value
		if (column_type.id() == LogicalTypeId::LIST || column_type.id() == LogicalTypeId::STRUCT ||
		    column_type.id() == LogicalTypeId::VARIANT) {
			continue;
		}

		// Get the MongoDB path for this column
		std::string mongo_field_name = column_name;
		auto path_it = column_name_to_mongo_path.find(column_name);
		if (path_it != column_name_to_mongo_path.end()) {
			mongo_field_name = path_it->second;
		}

		// Find the element
		bsoncxx::document::element element;
		if (mongo_field_name.find('.') != std::string::npos) {
			// Nested path - navigate through
			std::vector<std::string> segments;
			std::istringstream iss(mongo_field_name);
			std::string segment;
			while (std::getline(iss, segment, '.')) {
				segments.push_back(segment);
			}
			bsoncxx::document::view current = doc;
			for (size_t i = 0; i < segments.size(); i++) {
				element = current[segments[i]];
				if (!element) {
					break;
				}
				if (i < segments.size() - 1 && element.type() == bsoncxx::type::k_document) {
					current = element.get_document().value;
				}
			}
		} else {
			element = doc[mongo_field_name];
		}

		// Missing field is OK (will be NULL)
		if (!element || element.type() == bsoncxx::type::k_null || element.type() == bsoncxx::type::k_undefined) {
			continue;
		}

		// Check type compatibility for scalar types
		if (!IsBSONTypeCompatible(element.type(), column_type.id())) {
			if (schema_mode == SchemaMode::FAILFAST) {
				std::string doc_id = "<unknown>";
				auto id_elem = doc["_id"];
				if (id_elem) {
					if (id_elem.type() == bsoncxx::type::k_oid) {
						doc_id = id_elem.get_oid().value.to_string();
					} else if (id_elem.type() == bsoncxx::type::k_string) {
						doc_id = std::string(id_elem.get_string().value);
					}
				}
				throw InvalidInputException(
				    "Schema violation in document _id='%s': Field '%s' expected type %s but found %s.\n"
				    "Hint: Use schema_mode='permissive' to replace with NULL, or 'dropmalformed' to skip bad rows.",
				    doc_id, column_name, column_type.ToString(), GetBSONTypeName(element.type()));
			}
			// DROPMALFORMED: signal that row should be skipped
			return false;
		}
	}
	return true;
}

bool FlattenDocument(const bsoncxx::document::view &doc, const std::vector<string> &column_names,
                     const std::vector<LogicalType> &column_types, DataChunk &output, idx_t row_idx,
                     const std::unordered_map<string, string> &column_name_to_mongo_path, SchemaMode schema_mode,
                     bool has_explicit_schema, vector<MongoVariantColumnBuffer> *variant_buffers,
                     const vector<bool> *skip_columns) {
	// Track if we've seen any schema violations in this row (for DROPMALFORMED)
	bool row_has_violation = false;

	// Helper to handle schema violation based on mode
	auto handleSchemaViolation = [&](const std::string &field_name, const std::string &expected_type,
	                                 bsoncxx::type actual_bson_type, idx_t col_idx) -> bool {
		// Only enforce if explicit schema was provided
		if (!has_explicit_schema) {
			return true; // Continue processing
		}

		std::string actual_type = GetBSONTypeName(actual_bson_type);

		switch (schema_mode) {
		case SchemaMode::FAILFAST: {
			// Try to get document _id for error context
			std::string doc_id = "<unknown>";
			auto id_elem = doc["_id"];
			if (id_elem) {
				if (id_elem.type() == bsoncxx::type::k_oid) {
					doc_id = id_elem.get_oid().value.to_string();
				} else if (id_elem.type() == bsoncxx::type::k_string) {
					doc_id = std::string(id_elem.get_string().value);
				}
			}
			throw InvalidInputException(
			    "Schema violation in document _id='%s': Field '%s' expected type %s but found %s.\n"
			    "Hint: Use schema_mode='permissive' to replace with NULL, or 'dropmalformed' to skip bad rows.",
			    doc_id, field_name, expected_type, actual_type);
		}
		case SchemaMode::DROPMALFORMED:
			row_has_violation = true;
			return false; // Stop processing this row
		case SchemaMode::PERMISSIVE:
		default:
			// Set to NULL and continue
			FlatVector::SetNull(output.data[col_idx], row_idx, true);
			return true; // Continue processing other columns
		}
	};
	// Helper to get element from document by MongoDB path (uses dot notation)
	auto getElementByMongoPath = [&](const std::string &mongo_path) -> bsoncxx::document::element {
		std::vector<std::string> segments;
		std::istringstream iss(mongo_path);
		std::string segment;

		// Split by dots
		while (std::getline(iss, segment, '.')) {
			segments.push_back(segment);
		}

		if (segments.empty()) {
			return bsoncxx::document::element {};
		}

		bsoncxx::document::view current = doc;
		bsoncxx::document::element result;

		// Navigate through segments
		for (size_t i = 0; i < segments.size(); i++) {
			const std::string &seg = segments[i];
			auto element = current[seg];
			if (!element) {
				return bsoncxx::document::element {};
			}
			result = element;

			// If this is not the last segment, it must be a document
			if (i < segments.size() - 1) {
				if (result.type() != bsoncxx::type::k_document) {
					// Path is invalid - non-document in the middle
					return bsoncxx::document::element {};
				}
				current = result.get_document().value;
			} else {
				// Last segment - return the element (even if null)
				return result;
			}
		}

		// Should never reach here, but return result if we do
		return result;
	};

	// Helper to get element from document by path (handles nested fields with _ separator)
	// This is a fallback for when MongoDB path is not available
	auto getElementByPath = [&](const std::string &path) -> bsoncxx::document::element {
		std::vector<std::string> segments;
		std::istringstream iss(path);
		std::string segment;

		// First, collect all segments
		while (std::getline(iss, segment, '_')) {
			segments.push_back(segment);
		}

		if (segments.empty()) {
			return bsoncxx::document::element {};
		}

		bsoncxx::document::view current = doc;
		bsoncxx::document::element result;

		// Navigate through segments
		for (size_t i = 0; i < segments.size(); i++) {
			const std::string &seg = segments[i];
			auto element = current[seg];
			if (!element) {
				return bsoncxx::document::element {};
			}
			result = element;

			// If this is not the last segment, it must be a document
			if (i < segments.size() - 1) {
				if (result.type() != bsoncxx::type::k_document) {
					// Path is invalid - non-document in the middle
					return bsoncxx::document::element {};
				}
				current = result.get_document().value;
			} else {
				// Last segment - return the element (even if null)
				return result;
			}
		}

		// Should never reach here, but return result if we do
		return result;
	};

	auto getArrayByMongoPath = [&](const std::string &mongo_path) -> bsoncxx::array::view {
		std::vector<std::string> segments;
		std::istringstream iss(mongo_path);
		std::string segment;

		// Split by dots (MongoDB path notation)
		while (std::getline(iss, segment, '.')) {
			segments.push_back(segment);
		}

		if (segments.empty()) {
			return bsoncxx::array::view {};
		}

		bsoncxx::document::view current = doc;
		bsoncxx::document::element result;

		// Navigate through segments
		for (size_t i = 0; i < segments.size(); i++) {
			const std::string &seg = segments[i];
			auto element = current[seg];
			if (!element) {
				return bsoncxx::array::view {};
			}
			result = element;

			// If this is not the last segment, it must be a document
			if (i < segments.size() - 1) {
				if (result.type() != bsoncxx::type::k_document) {
					return bsoncxx::array::view {};
				}
				current = result.get_document().value;
			} else {
				// Last segment - check if it's an array
				if (result.type() == bsoncxx::type::k_array) {
					return result.get_array().value;
				}
				return bsoncxx::array::view {};
			}
		}

		return bsoncxx::array::view {};
	};

	auto getArrayByPath = [&](const std::string &path) -> bsoncxx::array::view {
		std::istringstream iss(path);
		std::string segment;
		bsoncxx::document::view current = doc;
		bsoncxx::document::element result;

		while (std::getline(iss, segment, '_')) {
			auto element = current[segment];
			if (!element) {
				return bsoncxx::array::view {};
			}
			result = element;
			if (result.type() == bsoncxx::type::k_document) {
				current = result.get_document().value;
			} else if (result.type() == bsoncxx::type::k_array) {
				return result.get_array().value;
			} else {
				return bsoncxx::array::view {};
			}
		}

		if (result && result.type() == bsoncxx::type::k_array) {
			return result.get_array().value;
		}
		return bsoncxx::array::view {};
	};

	for (idx_t col_idx = 0; col_idx < column_names.size(); col_idx++) {
		if (skip_columns && col_idx < skip_columns->size() && (*skip_columns)[col_idx]) {
			continue;
		}
		const auto &column_name = column_names[col_idx];
		const auto &column_type = column_types[col_idx];

		if (column_type.id() == LogicalTypeId::VARIANT) {
			// Any BSON value fits; the cell is buffered and the vector built once the chunk is complete
			if (!variant_buffers) {
				FlatVector::SetNull(output.data[col_idx], row_idx, true);
				continue;
			}
			auto &buffer = (*variant_buffers)[col_idx];
			std::string mongo_path = column_name;
			auto path_it = column_name_to_mongo_path.find(column_name);
			if (path_it != column_name_to_mongo_path.end()) {
				mongo_path = path_it->second;
			}
			if (mongo_path == MONGO_ROOT_PATH) {
				buffer.Set(row_idx, bsoncxx::types::bson_value::view(bsoncxx::types::b_document {doc}));
				continue;
			}
			bsoncxx::document::element element;
			if (mongo_path.find('.') != std::string::npos) {
				element = getElementByMongoPath(mongo_path);
			} else {
				element = doc[mongo_path];
				if (!element) {
					element = getElementByPath(column_name);
				}
			}
			if (!element || element.type() == bsoncxx::type::k_null || element.type() == bsoncxx::type::k_undefined) {
				buffer.SetNull(row_idx);
			} else {
				buffer.Set(row_idx, element.get_value());
			}
			continue;
		}

		if (column_type.id() == LogicalTypeId::LIST) {
			auto element = doc[column_name];
			bsoncxx::array::view array_view;

			if (element && element.type() == bsoncxx::type::k_array) {
				array_view = element.get_array().value;
			} else {
				// Try MongoDB path-based lookup for nested fields
				auto path_it = column_name_to_mongo_path.find(column_name);
				if (path_it != column_name_to_mongo_path.end()) {
					// Use MongoDB dot-notation path
					array_view = getArrayByMongoPath(path_it->second);
				} else {
					// Fallback to underscore-based path splitting
					array_view = getArrayByPath(column_name);
				}
			}

			auto &vec = output.data[col_idx];
			if (array_view.begin() == array_view.end()) {
				Value empty_list = Value::LIST(ListType::GetChildType(column_type), vector<Value>());
				if (empty_list.type() != column_type) {
					Value casted_list;
					string error_msg;
					if (!MongoDefaultTryCastAs(empty_list, column_type, casted_list, &error_msg)) {
						FlatVector::SetNull(vec, row_idx, true);
						continue;
					}
					empty_list = casted_list;
				}
				vec.SetValue(row_idx, empty_list);
			} else {
				// BSONArrayToList now handles depth mismatches by wrapping shallower arrays
				Value list_value = BSONArrayToList(array_view, column_type);
				if (list_value.type() != column_type) {
					Value casted_list;
					string error_msg;
					if (!MongoDefaultTryCastAs(list_value, column_type, casted_list, &error_msg)) {
						FlatVector::SetNull(vec, row_idx, true);
					} else {
						vec.SetValue(row_idx, casted_list);
					}
				} else {
					vec.SetValue(row_idx, list_value);
				}
			}
			continue;
		}

		if (column_type.id() == LogicalTypeId::STRUCT) {
			auto element = doc[column_name];
			bsoncxx::document::view struct_doc;

			auto &vec = output.data[col_idx];
			if (element && element.type() == bsoncxx::type::k_document) {
				struct_doc = element.get_document().value;
			} else {
				// Try MongoDB path-based lookup for nested fields
				auto path_it = column_name_to_mongo_path.find(column_name);
				if (path_it != column_name_to_mongo_path.end()) {
					// Use MongoDB dot-notation path
					element = getElementByMongoPath(path_it->second);
					if (element && element.type() == bsoncxx::type::k_document) {
						struct_doc = element.get_document().value;
					} else {
						Value null_struct = Value(column_type);
						vec.SetValue(row_idx, null_struct);
						continue;
					}
				} else {
					Value null_struct = Value(column_type);
					vec.SetValue(row_idx, null_struct);
					continue;
				}
			}

			Value struct_value = BSONDocumentToStruct(struct_doc, column_type);
			if (struct_value.type() != column_type) {
				Value casted_struct;
				string error_msg;
				if (!MongoDefaultTryCastAs(struct_value, column_type, casted_struct, &error_msg)) {
					FlatVector::SetNull(vec, row_idx, true);
					continue;
				}
				struct_value = casted_struct;
			}
			vec.SetValue(row_idx, struct_value);
			continue;
		}

		// Get MongoDB path for this column
		std::string mongo_field_name = column_name;
		auto path_it = column_name_to_mongo_path.find(column_name);
		if (path_it != column_name_to_mongo_path.end()) {
			mongo_field_name = path_it->second;
		}

		bsoncxx::document::element element;

		// Check if this is a nested path (contains dots)
		if (mongo_field_name.find('.') != std::string::npos) {
			// Use MongoDB path-based lookup for nested fields
			element = getElementByMongoPath(mongo_field_name);
		} else {
			// Try direct field access first (O(1) for top-level fields)
			element = doc[mongo_field_name];

			// If direct access didn't find the field, try iteration as fallback
			if (!element) {
				bool found = false;
				for (auto it = doc.begin(); it != doc.end(); ++it) {
					std::string field_key(it->key().data(), it->key().length());
					if (field_key == mongo_field_name) {
						element = *it;
						found = true;
						break;
					}
				}
				if (!found) {
					// Fallback to underscore-based path splitting
					element = getElementByPath(column_name);
				}
			}
		}

		if (!element || element.type() == bsoncxx::type::k_null) {
			// Field not found - set to NULL
			FlatVector::SetNull(output.data[col_idx], row_idx, true);
			continue;
		}

		switch (column_type.id()) {
		case LogicalTypeId::VARCHAR: {
			std::string str_val;
			if (element.type() == bsoncxx::type::k_string) {
				str_val = std::string(element.get_string().value.data(), element.get_string().value.length());
			} else if (element.type() == bsoncxx::type::k_oid) {
				str_val = element.get_oid().value.to_string();
			} else if (element.type() == bsoncxx::type::k_document) {
				str_val = NormalizeJson(bsoncxx::to_json(element.get_document().value));
			} else if (element.type() == bsoncxx::type::k_array) {
				str_val = NormalizeJson(bsoncxx::to_json(element.get_array().value));
			} else if (element.type() == bsoncxx::type::k_int32) {
				str_val = std::to_string(element.get_int32().value);
			} else if (element.type() == bsoncxx::type::k_int64) {
				str_val = std::to_string(element.get_int64().value);
			} else if (element.type() == bsoncxx::type::k_double) {
				str_val = std::to_string(element.get_double().value);
			} else if (element.type() == bsoncxx::type::k_bool) {
				str_val = element.get_bool().value ? "true" : "false";
			} else if (element.type() == bsoncxx::type::k_date) {
				str_val = std::to_string(element.get_date().to_int64());
			} else if (element.type() == bsoncxx::type::k_null) {
				str_val = "null";
			} else if (element.type() == bsoncxx::type::k_binary) {
				str_val = "<binary data>";
			} else if (element.type() == bsoncxx::type::k_undefined) {
				str_val = "undefined";
			} else if (element.type() == bsoncxx::type::k_regex) {
				auto regex = element.get_regex();
				str_val = "/" + std::string(regex.regex.data(), regex.regex.length()) + "/" +
				          std::string(regex.options.data(), regex.options.length());
			} else if (element.type() == bsoncxx::type::k_dbpointer) {
				str_val = "<dbpointer>";
			} else if (element.type() == bsoncxx::type::k_code) {
				str_val = std::string(element.get_code().code.data(), element.get_code().code.length());
			} else if (element.type() == bsoncxx::type::k_codewscope) {
				str_val = std::string(element.get_codewscope().code.data(), element.get_codewscope().code.length());
			} else if (element.type() == bsoncxx::type::k_symbol) {
				str_val = std::string(element.get_symbol().symbol.data(), element.get_symbol().symbol.length());
			} else if (element.type() == bsoncxx::type::k_timestamp) {
				auto ts = element.get_timestamp();
				str_val = std::to_string(ts.timestamp) + ":" + std::to_string(ts.increment);
			} else if (element.type() == bsoncxx::type::k_decimal128) {
				str_val = element.get_decimal128().value.to_string();
			} else {
				// For unknown types, use a default representation
				str_val = "<unknown type>";
			}
			FlatVector::GetDataMutable<string_t>(output.data[col_idx])[row_idx] =
			    StringVector::AddString(output.data[col_idx], str_val);
			break;
		}
		case LogicalTypeId::BIGINT: {
			// Check type compatibility
			if (!IsBSONTypeCompatible(element.type(), LogicalTypeId::BIGINT)) {
				if (!handleSchemaViolation(column_name, "BIGINT", element.type(), col_idx)) {
					return false; // DROPMALFORMED: skip this row
				}
				break; // PERMISSIVE: already set to NULL
			}
			int64_t int_val = 0;
			if (element.type() == bsoncxx::type::k_int32) {
				int_val = element.get_int32().value;
			} else if (element.type() == bsoncxx::type::k_int64) {
				int_val = element.get_int64().value;
			} else if (element.type() == bsoncxx::type::k_double) {
				int_val = static_cast<int64_t>(element.get_double().value);
			}
			FlatVector::GetDataMutable<int64_t>(output.data[col_idx])[row_idx] = int_val;
			break;
		}
		case LogicalTypeId::HUGEINT: {
			// Check type compatibility
			if (!IsBSONTypeCompatible(element.type(), LogicalTypeId::HUGEINT)) {
				if (!handleSchemaViolation(column_name, "HUGEINT", element.type(), col_idx)) {
					return false;
				}
				break;
			}
			// MongoDB doesn't have 128-bit integers, so we convert from int32/int64/double/decimal128
			hugeint_t huge_val = 0;
			if (element.type() == bsoncxx::type::k_int32) {
				huge_val = hugeint_t(element.get_int32().value);
			} else if (element.type() == bsoncxx::type::k_int64) {
				huge_val = hugeint_t(element.get_int64().value);
			} else if (element.type() == bsoncxx::type::k_double) {
				huge_val = hugeint_t(static_cast<int64_t>(element.get_double().value));
			} else if (element.type() == bsoncxx::type::k_decimal128) {
				// Parse Decimal128 string and convert to hugeint (truncating decimal part)
				auto dec_str = element.get_decimal128().value.to_string();
				try {
					double d = std::stod(dec_str);
					huge_val = hugeint_t(static_cast<int64_t>(d));
				} catch (...) {
					huge_val = 0;
				}
			}
			FlatVector::GetDataMutable<hugeint_t>(output.data[col_idx])[row_idx] = huge_val;
			break;
		}
		case LogicalTypeId::DOUBLE: {
			// Check type compatibility
			if (!IsBSONTypeCompatible(element.type(), LogicalTypeId::DOUBLE)) {
				if (!handleSchemaViolation(column_name, "DOUBLE", element.type(), col_idx)) {
					return false;
				}
				break;
			}
			double double_val = 0.0;
			if (element.type() == bsoncxx::type::k_double) {
				double_val = element.get_double().value;
			} else if (element.type() == bsoncxx::type::k_int32) {
				double_val = static_cast<double>(element.get_int32().value);
			} else if (element.type() == bsoncxx::type::k_int64) {
				double_val = static_cast<double>(element.get_int64().value);
			} else if (element.type() == bsoncxx::type::k_decimal128) {
				auto dec_str = element.get_decimal128().value.to_string();
				try {
					double_val = std::stod(dec_str);
				} catch (...) {
					double_val = 0.0;
				}
			}
			FlatVector::GetDataMutable<double>(output.data[col_idx])[row_idx] = double_val;
			break;
		}
		case LogicalTypeId::BOOLEAN: {
			// Check type compatibility
			if (!IsBSONTypeCompatible(element.type(), LogicalTypeId::BOOLEAN)) {
				if (!handleSchemaViolation(column_name, "BOOLEAN", element.type(), col_idx)) {
					return false;
				}
				break;
			}
			bool bool_val = false;
			if (element.type() == bsoncxx::type::k_bool) {
				bool_val = element.get_bool().value;
			}
			FlatVector::GetDataMutable<bool>(output.data[col_idx])[row_idx] = bool_val;
			break;
		}
		case LogicalTypeId::DATE: {
			// Check type compatibility
			if (!IsBSONTypeCompatible(element.type(), LogicalTypeId::DATE)) {
				if (!handleSchemaViolation(column_name, "DATE", element.type(), col_idx)) {
					return false;
				}
				break;
			}
			date_t date_val;
			if (element.type() == bsoncxx::type::k_date) {
				auto mongo_date = element.get_date();
				auto ms_since_epoch = mongo_date.to_int64();
				// Convert milliseconds to timestamp_t, then to date_t
				timestamp_t ts_val = Timestamp::FromEpochMs(ms_since_epoch);
				date_val = Timestamp::GetDate(ts_val);
			} else {
				date_val = date_t(0);
			}
			FlatVector::GetDataMutable<date_t>(output.data[col_idx])[row_idx] = date_val;
			break;
		}
		case LogicalTypeId::TIMESTAMP: {
			// Check type compatibility
			if (!IsBSONTypeCompatible(element.type(), LogicalTypeId::TIMESTAMP)) {
				if (!handleSchemaViolation(column_name, "TIMESTAMP", element.type(), col_idx)) {
					return false;
				}
				break;
			}
			timestamp_t ts_val;
			if (element.type() == bsoncxx::type::k_date) {
				auto date_val = element.get_date();
				ts_val = Timestamp::FromEpochMs(date_val.to_int64());
			} else {
				ts_val = Timestamp::FromEpochMs(0);
			}
			FlatVector::GetDataMutable<timestamp_t>(output.data[col_idx])[row_idx] = ts_val;
			break;
		}
		default: {
			// Default to NULL for unsupported types
			FlatVector::SetNull(output.data[col_idx], row_idx, true);
			break;
		}
		}
	}

	return !row_has_violation;
}

} // namespace duckdb
