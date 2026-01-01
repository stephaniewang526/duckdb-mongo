#include "mongo_table_function.hpp"
#include "mongo_instance.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/execution/operator/helper/physical_limit.hpp"
#include "duckdb/execution/operator/helper/physical_streaming_limit.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/array.hpp>
#include <sstream>

namespace duckdb {

// Helper function to normalize JSON output
static std::string NormalizeJson(const std::string &json) {
	std::string normalized;
	normalized.reserve(json.length());
	bool in_string = false;
	bool escape_next = false;

	for (size_t i = 0; i < json.length(); i++) {
		char c = json[i];
		if (escape_next) {
			normalized += c;
			escape_next = false;
			continue;
		}
		if (c == '\\') {
			escape_next = true;
			normalized += c;
			continue;
		}
		if (c == '"') {
			in_string = !in_string;
			normalized += c;
			continue;
		}
		if (in_string) {
			normalized += c;
			continue;
		}
		// Outside strings, remove spaces after [ and before ], and after commas
		// This normalizes JSON arrays to compact format: ["a","b"] instead of [ "a", "b" ]
		if (c == ' ' && i > 0 && i < json.length() - 1) {
			char prev = json[i - 1];
			char next = json[i + 1];
			// Remove space after [ or , if followed by a valid JSON value start
			// Valid starts: ", [, {, digit, -, or boolean/null (t/f/n)
			if ((prev == '[' || prev == ',') &&
			    (next == '"' || next == '[' || next == '{' || (next >= '0' && next <= '9') || next == '-' ||
			     next == 't' || next == 'f' || next == 'n')) {
				continue; // Skip this space
			}
			// Remove space before ] or } if preceded by a valid JSON value end
			// Valid ends: ", ], }, or digit
			if ((next == ']' || next == '}') &&
			    (prev == '"' || prev == ']' || prev == '}' || (prev >= '0' && prev <= '9'))) {
				continue; // Skip this space
			}
		}
		normalized += c;
	}
	return normalized;
}

LogicalType InferTypeFromBSON(const bsoncxx::document::element &element) {
	switch (element.type()) {
	case bsoncxx::type::k_string:
		return LogicalType::VARCHAR;
	case bsoncxx::type::k_int32:
	case bsoncxx::type::k_int64:
		return LogicalType::BIGINT;
	case bsoncxx::type::k_double:
		return LogicalType::DOUBLE;
	case bsoncxx::type::k_bool:
		return LogicalType::BOOLEAN;
	case bsoncxx::type::k_date: {
		// Infer DATE if time component is 00:00:00 (midnight UTC), otherwise TIMESTAMP
		auto date_value = element.get_date();
		auto ms_since_epoch = date_value.to_int64();
		auto seconds = ms_since_epoch / 1000;
		auto time_of_day = seconds % 86400;
		if (time_of_day == 0) {
			return LogicalType::DATE;
		}
		return LogicalType::TIMESTAMP;
	}
	case bsoncxx::type::k_oid:
		return LogicalType::VARCHAR; // ObjectId as string
	case bsoncxx::type::k_binary:
		return LogicalType::BLOB;
	case bsoncxx::type::k_array:
		return LogicalType::VARCHAR; // Arrays stored as JSON string
	case bsoncxx::type::k_document:
		return LogicalType::VARCHAR; // Nested documents stored as JSON string
	default:
		return LogicalType::VARCHAR; // Default to VARCHAR for unknown types
	}
}

LogicalType ResolveTypeConflict(const std::vector<LogicalType> &types) {
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

	// For mixed types, use a priority-based approach that considers both
	// the type's ability to represent other types and the frequency of each type
	// Count occurrences of each type
	int double_count = 0;
	int bigint_count = 0;
	int varchar_count = 0;
	int boolean_count = 0;
	int timestamp_count = 0;
	int date_count = 0;

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
		} else if (type == LogicalType::DATE) {
			date_count++;
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
                       std::unordered_map<std::string, vector<LogicalType>> &field_types) {
	const int MAX_DEPTH = 5;

	if (depth > MAX_DEPTH) {
		// Store as JSON for deeply nested structures
		if (!prefix.empty()) {
			field_types[prefix].push_back(LogicalType::VARCHAR);
		}
		return;
	}

	for (const auto &element : doc) {
		std::string field_name = std::string(element.key().data(), element.key().length());
		std::string full_path = prefix.empty() ? field_name : prefix + "_" + field_name;

		switch (element.type()) {
		case bsoncxx::type::k_document: {
			// Recursively process nested document
			auto nested_doc = element.get_document().value;
			CollectFieldPaths(nested_doc, full_path, depth + 1, field_types);
			// Also store the document itself as JSON
			field_types[full_path].push_back(LogicalType::VARCHAR);
			break;
		}
		case bsoncxx::type::k_array: {
			// Arrays stored as JSON
			field_types[full_path].push_back(LogicalType::VARCHAR);
			// Try to infer array element type
			auto array = element.get_array().value;
			if (array.begin() != array.end()) {
				auto first_element = *array.begin();
				if (first_element.type() == bsoncxx::type::k_document) {
					// Array of objects - collect paths with array prefix
					auto nested_doc = first_element.get_document().value;
					CollectFieldPaths(nested_doc, full_path + "_item", depth + 1, field_types);
				}
			}
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

void InferSchemaFromDocuments(mongocxx::collection &collection, int64_t sample_size, vector<string> &column_names,
                              vector<LogicalType> &column_types) {
	std::unordered_map<std::string, vector<LogicalType>> field_types;

	// Sample documents
	mongocxx::options::find opts;
	opts.limit(sample_size);

	auto cursor = collection.find({}, opts);

	int64_t count = 0;
	for (const auto &doc : cursor) {
		CollectFieldPaths(doc, "", 0, field_types);
		count++;
		if (count >= sample_size) {
			break;
		}
	}

	// Always include _id column (present in all MongoDB documents)
	// If collection is empty, we still need at least one column
	if (field_types.find("_id") == field_types.end()) {
		field_types["_id"] = {LogicalType::VARCHAR}; // ObjectId as string
	}

	// Build column names and types from collected field paths
	// Ensure _id is always first
	if (field_types.find("_id") != field_types.end()) {
		column_names.push_back("_id");
		LogicalType resolved_type = ResolveTypeConflict(field_types["_id"]);
		column_types.push_back(resolved_type);
	}

	// Add all other columns (excluding _id which we already added)
	for (const auto &pair : field_types) {
		if (pair.first != "_id") {
			column_names.push_back(pair.first);
			LogicalType resolved_type = ResolveTypeConflict(pair.second);
			column_types.push_back(resolved_type);
		}
	}

	// Ensure we have at least one column (should always have _id, but double-check)
	if (column_names.empty()) {
		column_names.push_back("_id");
		column_types.push_back(LogicalType::VARCHAR);
	}
}

void FlattenDocument(const bsoncxx::document::view &doc, const vector<string> &column_names,
                     const vector<LogicalType> &column_types, DataChunk &output, idx_t row_idx) {
	// Helper to get element from document by path (handles nested fields with _ separator)
	auto getElementByPath = [&](const std::string &path) -> bsoncxx::document::element {
		std::istringstream iss(path);
		std::string segment;
		bsoncxx::document::view current = doc;
		bsoncxx::document::element result;
		bool found = false;

		while (std::getline(iss, segment, '_')) {
			auto element = current[segment];
			if (!element) {
				return bsoncxx::document::element {};
			}
			result = element;
			if (result.type() == bsoncxx::type::k_document) {
				current = result.get_document().value;
				found = true;
			} else {
				found = true;
				break;
			}
		}

		return found ? result : bsoncxx::document::element {};
	};

	for (idx_t col_idx = 0; col_idx < column_names.size(); col_idx++) {
		const auto &column_name = column_names[col_idx];
		const auto &column_type = column_types[col_idx];

		// Try direct lookup first
		auto element = doc[column_name];

		if (!element) {
			// Try path-based lookup for nested fields
			element = getElementByPath(column_name);
			if (!element || element.type() == bsoncxx::type::k_null) {
				// Field not found - set to NULL
				FlatVector::SetNull(output.data[col_idx], row_idx, true);
				continue;
			}
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
				// Binary data - convert to base64 or hex representation
				// For now, just indicate binary data exists
				// TODO: Could convert to base64 using the binary data if needed
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
			FlatVector::GetData<string_t>(output.data[col_idx])[row_idx] =
			    StringVector::AddString(output.data[col_idx], str_val);
			break;
		}
		case LogicalTypeId::BIGINT: {
			int64_t int_val = 0;
			if (element.type() == bsoncxx::type::k_int32) {
				int_val = element.get_int32().value;
			} else if (element.type() == bsoncxx::type::k_int64) {
				int_val = element.get_int64().value;
			} else if (element.type() == bsoncxx::type::k_double) {
				int_val = static_cast<int64_t>(element.get_double().value);
			}
			FlatVector::GetData<int64_t>(output.data[col_idx])[row_idx] = int_val;
			break;
		}
		case LogicalTypeId::DOUBLE: {
			double double_val = 0.0;
			if (element.type() == bsoncxx::type::k_double) {
				double_val = element.get_double().value;
			} else if (element.type() == bsoncxx::type::k_int32) {
				double_val = static_cast<double>(element.get_int32().value);
			} else if (element.type() == bsoncxx::type::k_int64) {
				double_val = static_cast<double>(element.get_int64().value);
			}
			FlatVector::GetData<double>(output.data[col_idx])[row_idx] = double_val;
			break;
		}
		case LogicalTypeId::BOOLEAN: {
			bool bool_val = false;
			if (element.type() == bsoncxx::type::k_bool) {
				bool_val = element.get_bool().value;
			}
			FlatVector::GetData<bool>(output.data[col_idx])[row_idx] = bool_val;
			break;
		}
		case LogicalTypeId::DATE: {
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
			FlatVector::GetData<date_t>(output.data[col_idx])[row_idx] = date_val;
			break;
		}
		case LogicalTypeId::TIMESTAMP: {
			timestamp_t ts_val;
			if (element.type() == bsoncxx::type::k_date) {
				auto date_val = element.get_date();
				ts_val = Timestamp::FromEpochMs(date_val.to_int64());
			} else {
				ts_val = Timestamp::FromEpochMs(0);
			}
			FlatVector::GetData<timestamp_t>(output.data[col_idx])[row_idx] = ts_val;
			break;
		}
		default: {
			// Default to NULL for unsupported types
			FlatVector::SetNull(output.data[col_idx], row_idx, true);
			break;
		}
		}
	}
}

unique_ptr<FunctionData> MongoScanBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<MongoScanData>();

	// Parse function arguments
	if (input.inputs.size() < 3) {
		throw InvalidInputException(
		    "mongo_scan requires at least 3 arguments: connection_string, database, collection");
	}

	result->connection_string = input.inputs[0].GetValue<string>();
	result->database_name = input.inputs[1].GetValue<string>();
	result->collection_name = input.inputs[2].GetValue<string>();

	// Parse named parameters
	if (input.named_parameters.find("filter") != input.named_parameters.end()) {
		result->filter_query = input.named_parameters["filter"].GetValue<string>();
	}

	if (input.named_parameters.find("sample_size") != input.named_parameters.end()) {
		result->sample_size = input.named_parameters["sample_size"].GetValue<int64_t>();
	}

	// Ensure MongoDB instance is initialized
	GetMongoInstance();

	// Create connection
	result->connection = make_shared_ptr<MongoConnection>(result->connection_string);

	// Get collection
	auto db = result->connection->client[result->database_name];
	auto collection = db[result->collection_name];

	// Infer schema
	InferSchemaFromDocuments(collection, result->sample_size, result->column_names, result->column_types);

	// Set return types and names
	return_types = result->column_types;
	names = result->column_names;

	return std::move(result);
}

// Helper function to append a DuckDB Value to a MongoDB basic array builder
void AppendValueToArray(bsoncxx::builder::basic::array &array_builder, const Value &value, const LogicalType &type) {
	if (value.IsNull()) {
		array_builder.append(bsoncxx::types::b_null {});
		return;
	}

	switch (type.id()) {
	case LogicalTypeId::VARCHAR: {
		array_builder.append(value.GetValue<string>());
		break;
	}
	case LogicalTypeId::BIGINT: {
		array_builder.append(value.GetValue<int64_t>());
		break;
	}
	case LogicalTypeId::INTEGER: {
		array_builder.append(value.GetValue<int32_t>());
		break;
	}
	case LogicalTypeId::DOUBLE: {
		array_builder.append(value.GetValue<double>());
		break;
	}
	case LogicalTypeId::BOOLEAN: {
		array_builder.append(value.GetValue<bool>());
		break;
	}
	case LogicalTypeId::DATE: {
		auto date_val = value.GetValue<date_t>();
		auto year = Date::ExtractYear(date_val);
		auto month = Date::ExtractMonth(date_val);
		auto day = Date::ExtractDay(date_val);
		auto date_obj = Date::FromDate(year, month, day);
		auto time_obj = Time::FromTime(0, 0, 0);
		auto timestamp_val = Timestamp::FromDatetime(date_obj, time_obj);
		auto ms_since_epoch = Timestamp::GetEpochMs(timestamp_val);
		array_builder.append(bsoncxx::types::b_date {std::chrono::milliseconds(ms_since_epoch)});
		break;
	}
	case LogicalTypeId::TIMESTAMP: {
		auto timestamp_val = value.GetValue<timestamp_t>();
		auto ms_since_epoch = Timestamp::GetEpochMs(timestamp_val);
		array_builder.append(bsoncxx::types::b_date {std::chrono::milliseconds(ms_since_epoch)});
		break;
	}
	default: {
		// For unknown types, convert to string
		array_builder.append(value.ToString());
		break;
	}
	}
}

// Helper function to append a DuckDB Value to a MongoDB basic document builder
void AppendValueToDocument(bsoncxx::builder::basic::document &doc_builder, const string &key, const Value &value, const LogicalType &type) {
	if (value.IsNull()) {
		doc_builder.append(bsoncxx::builder::basic::kvp(key, bsoncxx::types::b_null {}));
		return;
	}

	switch (type.id()) {
	case LogicalTypeId::VARCHAR: {
		doc_builder.append(bsoncxx::builder::basic::kvp(key, value.GetValue<string>()));
		break;
	}
	case LogicalTypeId::BIGINT: {
		doc_builder.append(bsoncxx::builder::basic::kvp(key, value.GetValue<int64_t>()));
		break;
	}
	case LogicalTypeId::INTEGER: {
		doc_builder.append(bsoncxx::builder::basic::kvp(key, value.GetValue<int32_t>()));
		break;
	}
	case LogicalTypeId::DOUBLE: {
		doc_builder.append(bsoncxx::builder::basic::kvp(key, value.GetValue<double>()));
		break;
	}
	case LogicalTypeId::BOOLEAN: {
		doc_builder.append(bsoncxx::builder::basic::kvp(key, value.GetValue<bool>()));
		break;
	}
	case LogicalTypeId::DATE: {
		auto date_val = value.GetValue<date_t>();
		auto year = Date::ExtractYear(date_val);
		auto month = Date::ExtractMonth(date_val);
		auto day = Date::ExtractDay(date_val);
		auto date_obj = Date::FromDate(year, month, day);
		auto time_obj = Time::FromTime(0, 0, 0);
		auto timestamp_val = Timestamp::FromDatetime(date_obj, time_obj);
		auto ms_since_epoch = Timestamp::GetEpochMs(timestamp_val);
		doc_builder.append(bsoncxx::builder::basic::kvp(key, bsoncxx::types::b_date {std::chrono::milliseconds(ms_since_epoch)}));
		break;
	}
	case LogicalTypeId::TIMESTAMP: {
		auto timestamp_val = value.GetValue<timestamp_t>();
		auto ms_since_epoch = Timestamp::GetEpochMs(timestamp_val);
		doc_builder.append(bsoncxx::builder::basic::kvp(key, bsoncxx::types::b_date {std::chrono::milliseconds(ms_since_epoch)}));
		break;
	}
	default: {
		// For unknown types, convert to string
		doc_builder.append(bsoncxx::builder::basic::kvp(key, value.ToString()));
		break;
	}
	}
}

// Convert a single filter to MongoDB query document
bsoncxx::document::value ConvertSingleFilterToMongo(const TableFilter &filter, const string &column_name,
                                                               const LogicalType &column_type) {
	bsoncxx::builder::basic::document doc;

	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		const auto &constant_filter = filter.Cast<ConstantFilter>();
		string mongo_op;

		switch (constant_filter.comparison_type) {
		case ExpressionType::COMPARE_EQUAL:
			// For equality, we can use direct field matching
			AppendValueToDocument(doc, column_name, constant_filter.constant, column_type);
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			mongo_op = "$ne";
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			mongo_op = "$lt";
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			mongo_op = "$lte";
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			mongo_op = "$gt";
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			mongo_op = "$gte";
			break;
		default:
			// Unsupported comparison type, return empty filter
			return doc.extract();
		}

		if (!mongo_op.empty()) {
			bsoncxx::builder::basic::document op_doc;
			AppendValueToDocument(op_doc, mongo_op, constant_filter.constant, column_type);
			doc.append(bsoncxx::builder::basic::kvp(column_name, op_doc.extract()));
		}
		break;
	}
	case TableFilterType::IN_FILTER: {
		const auto &in_filter = filter.Cast<InFilter>();
		bsoncxx::builder::basic::array in_array;
		for (const auto &val : in_filter.values) {
			AppendValueToArray(in_array, val, column_type);
		}
		bsoncxx::builder::basic::document in_doc;
		in_doc.append(bsoncxx::builder::basic::kvp("$in", in_array.extract()));
		doc.append(bsoncxx::builder::basic::kvp(column_name, in_doc.extract()));
		break;
	}
	case TableFilterType::IS_NULL: {
		doc.append(bsoncxx::builder::basic::kvp(column_name, bsoncxx::types::b_null {}));
		break;
	}
	case TableFilterType::IS_NOT_NULL: {
		bsoncxx::builder::basic::document ne_doc;
		ne_doc.append(bsoncxx::builder::basic::kvp("$ne", bsoncxx::types::b_null {}));
		doc.append(bsoncxx::builder::basic::kvp(column_name, ne_doc.extract()));
		break;
	}
	case TableFilterType::CONJUNCTION_AND: {
		// For AND filters, combine conditions on the same column
		const auto &conj_filter = static_cast<const ConjunctionFilter &>(filter);
		bsoncxx::builder::basic::document merged_doc;
		for (const auto &child_filter : conj_filter.child_filters) {
			auto child_doc = ConvertSingleFilterToMongo(*child_filter, column_name, column_type);
			// Extract conditions from child_doc and merge
			for (auto it = child_doc.view().begin(); it != child_doc.view().end(); ++it) {
				if (it->key() == column_name && it->type() == bsoncxx::type::k_document) {
					// Merge nested document conditions
					for (auto nested_it = it->get_document().value.begin();
					     nested_it != it->get_document().value.end(); ++nested_it) {
						merged_doc.append(bsoncxx::builder::basic::kvp(nested_it->key(), nested_it->get_value()));
					}
				}
			}
		}
		doc.append(bsoncxx::builder::basic::kvp(column_name, merged_doc.extract()));
		break;
	}
	case TableFilterType::CONJUNCTION_OR: {
		const auto &conj_filter = static_cast<const ConjunctionFilter &>(filter);
		// Check if all child filters are equality comparisons - if so, use $in
		bool all_equality = true;
		vector<Value> equality_values;
		
		for (const auto &child_filter : conj_filter.child_filters) {
			if (child_filter->filter_type == TableFilterType::CONSTANT_COMPARISON) {
				const auto &cf = child_filter->Cast<ConstantFilter>();
				if (cf.comparison_type == ExpressionType::COMPARE_EQUAL) {
					equality_values.push_back(cf.constant);
				} else {
					all_equality = false;
					break;
				}
			} else {
				all_equality = false;
				break;
			}
		}
		
		if (all_equality && equality_values.size() > 1) {
			// Use $in for multiple equality checks
			bsoncxx::builder::basic::array in_array;
			for (const auto &val : equality_values) {
				AppendValueToArray(in_array, val, column_type);
			}
			bsoncxx::builder::basic::document in_doc;
			in_doc.append(bsoncxx::builder::basic::kvp("$in", in_array.extract()));
			doc.append(bsoncxx::builder::basic::kvp(column_name, in_doc.extract()));
		} else {
			// For complex OR filters, use $or at top level
			bsoncxx::builder::basic::array or_array;
			for (const auto &child_filter : conj_filter.child_filters) {
				auto child_doc = ConvertSingleFilterToMongo(*child_filter, column_name, column_type);
				or_array.append(child_doc.view());
			}
			doc.append(bsoncxx::builder::basic::kvp("$or", or_array.extract()));
		}
		break;
	}
	default:
		// Unsupported filter type, return empty filter
		break;
	}

	return doc.extract();
}

// Convert DuckDB filters to MongoDB query
bsoncxx::document::value ConvertFiltersToMongoQuery(optional_ptr<TableFilterSet> filters,
                                                    const vector<string> &column_names,
                                                    const vector<LogicalType> &column_types) {
	bsoncxx::builder::basic::document query_builder;

	if (!filters || filters->filters.empty()) {
		return query_builder.extract();
	}

	// Group filters by column name to merge multiple filters on same column
	map<string, bsoncxx::builder::basic::document> column_filters;
	
	for (const auto &filter_pair : filters->filters) {
		idx_t col_idx = filter_pair.first;
		if (col_idx >= column_names.size()) {
			continue;
		}

		const auto &filter = filter_pair.second;
		const string &column_name = column_names[col_idx];
		const LogicalType &column_type = col_idx < column_types.size() ? column_types[col_idx] : LogicalType::VARCHAR;

		auto filter_doc = ConvertSingleFilterToMongo(*filter, column_name, column_type);
		if (filter_doc.view().empty()) {
			continue;
		}

		// Extract column filter from document
		bsoncxx::types::bson_value::view column_filter_value;
		bool has_column_filter = false;
		for (auto doc_it = filter_doc.view().begin(); doc_it != filter_doc.view().end(); ++doc_it) {
			if (doc_it->key() == column_name) {
				column_filter_value = doc_it->get_value();
				has_column_filter = true;
				break;
			}
		}
		
		if (!has_column_filter) {
			// Top-level operator like $or - add directly
			for (auto doc_it = filter_doc.view().begin(); doc_it != filter_doc.view().end(); ++doc_it) {
				query_builder.append(bsoncxx::builder::basic::kvp(doc_it->key(), doc_it->get_value()));
			}
			continue;
		}

		// Merge filters for same column
		auto it = column_filters.find(column_name);
		if (it == column_filters.end()) {
			bsoncxx::builder::basic::document col_doc;
			if (column_filter_value.type() == bsoncxx::type::k_document) {
				// Copy operators
				for (auto nested_it = column_filter_value.get_document().value.begin();
				     nested_it != column_filter_value.get_document().value.end(); ++nested_it) {
					col_doc.append(bsoncxx::builder::basic::kvp(nested_it->key(), nested_it->get_value()));
				}
			} else {
				// Equality - use $eq
				col_doc.append(bsoncxx::builder::basic::kvp("$eq", column_filter_value));
			}
			column_filters[column_name] = std::move(col_doc);
		} else {
			if (column_filter_value.type() == bsoncxx::type::k_document) {
				// Merge operators
				for (auto nested_it = column_filter_value.get_document().value.begin();
				     nested_it != column_filter_value.get_document().value.end(); ++nested_it) {
					it->second.append(bsoncxx::builder::basic::kvp(nested_it->key(), nested_it->get_value()));
				}
			} else {
				// Add as $eq
				it->second.append(bsoncxx::builder::basic::kvp("$eq", column_filter_value));
			}
		}
	}

	// Add column filters to query
	for (auto &col_filter_pair : column_filters) {
		query_builder.append(bsoncxx::builder::basic::kvp(col_filter_pair.first, col_filter_pair.second.extract()));
	}

	return query_builder.extract();
}

unique_ptr<LocalTableFunctionState> MongoScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                       GlobalTableFunctionState *global_state) {
	const auto &data = dynamic_cast<const MongoScanData &>(*input.bind_data);
	auto result = make_uniq<MongoScanState>();

	// Store connection info in state
	result->connection = data.connection;
	result->database_name = data.database_name;
	result->collection_name = data.collection_name;
	result->filter_query = data.filter_query;

	// Get collection
	auto db = result->connection->client[result->database_name];
	auto collection = db[result->collection_name];

	// Build query from pushed-down filters
	bsoncxx::document::view_or_value query_filter;
	if (input.filters) {
		// Convert DuckDB filters to MongoDB query
		auto mongo_filter = ConvertFiltersToMongoQuery(input.filters, data.column_names, data.column_types);
		query_filter = std::move(mongo_filter);
	} else if (!result->filter_query.empty()) {
		// Fall back to manual filter query if provided
		query_filter = bsoncxx::from_json(result->filter_query);
	} else {
		query_filter = bsoncxx::builder::stream::document {} << bsoncxx::builder::stream::finalize;
	}

	// Build MongoDB find options
	mongocxx::options::find opts;
	
	// LIMIT pushdown: Push constant LIMIT values to MongoDB
	// Only works when LIMIT is directly above table scan (simple queries, not Q3/Q10 with joins)
	if (input.op) {
		if (input.op->type == PhysicalOperatorType::LIMIT) {
			const auto &limit_op = input.op->Cast<PhysicalLimit>();
			if (limit_op.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
				idx_t limit_value = limit_op.limit_val.GetConstantValue();
				if (limit_value > 0 && limit_value < PhysicalLimit::MAX_LIMIT_VALUE) {
					opts.limit(limit_value);
					result->limit = limit_value;
				}
			}
		} else if (input.op->type == PhysicalOperatorType::STREAMING_LIMIT) {
			const auto &streaming_limit_op = input.op->Cast<PhysicalStreamingLimit>();
			if (streaming_limit_op.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
				idx_t limit_value = streaming_limit_op.limit_val.GetConstantValue();
				if (limit_value > 0 && limit_value < PhysicalLimit::MAX_LIMIT_VALUE) {
					opts.limit(limit_value);
					result->limit = limit_value;
				}
			}
		}
	}
	
	// Create cursor
	result->cursor = make_uniq<mongocxx::cursor>(collection.find(query_filter, opts));
	result->current = make_uniq<mongocxx::cursor::iterator>(result->cursor->begin());
	result->end = make_uniq<mongocxx::cursor::iterator>(result->cursor->end());

	return std::move(result);
}

void MongoScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	const auto &bind_data = dynamic_cast<const MongoScanData &>(*data_p.bind_data);
	auto &state = dynamic_cast<MongoScanState &>(*data_p.local_state);

	if (state.finished) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = 0;
	const idx_t max_count = STANDARD_VECTOR_SIZE;

	// Initialize output vectors
	for (idx_t col_idx = 0; col_idx < bind_data.column_names.size(); col_idx++) {
		output.data[col_idx].SetVectorType(VectorType::FLAT_VECTOR);
	}

	while (count < max_count && *state.current != *state.end) {
		auto doc = **state.current;
		FlattenDocument(doc, bind_data.column_names, bind_data.column_types, output, count);
		++(*state.current);
		count++;
	}

	output.SetCardinality(count);

	if (*state.current == *state.end) {
		state.finished = true;
	}
}

void RegisterMongoTableFunction(DatabaseInstance &db) {
	TableFunction mongo_scan("mongo_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                         MongoScanFunction, MongoScanBind, nullptr, MongoScanInitLocal);

	// Add optional parameters
	mongo_scan.named_parameters["filter"] = LogicalType::VARCHAR;
	mongo_scan.named_parameters["sample_size"] = LogicalType::BIGINT;

	// Register the table function using ExtensionLoader
	// Note: This should be called from ExtensionLoader::Load, not directly
	// The ExtensionLoader will handle registration
}

} // namespace duckdb
