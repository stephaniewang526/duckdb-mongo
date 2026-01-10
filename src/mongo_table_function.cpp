#include "mongo_table_function.hpp"
#include "mongo_instance.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types.hpp"
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
#include <bsoncxx/json.hpp>
#include <algorithm>
#include <sstream>
#include <cctype>
#include <iostream>
#include <map>
#include <unordered_set>

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

// Helper to infer type from BSON element (works with both document::element and array::element)
template <typename ElementType>
LogicalType InferTypeFromBSONElement(const ElementType &element) {
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

// Wrapper for document::element (backward compatibility)
LogicalType InferTypeFromBSON(const bsoncxx::document::element &element) {
	return InferTypeFromBSONElement(element);
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

LogicalType InferStructTypeFromArray(bsoncxx::array::view array, int depth);

LogicalType InferNestedArrayType(bsoncxx::array::view array, int depth) {
	const int MAX_DEPTH = 5;

	if (depth > MAX_DEPTH) {
		return LogicalType::VARCHAR;
	}

	if (array.begin() == array.end()) {
		return LogicalType::VARCHAR;
	}

	auto first_element = *array.begin();

	if (first_element.type() == bsoncxx::type::k_array) {
		auto nested_array = first_element.get_array().value;
		if (nested_array.begin() == nested_array.end()) {
			return LogicalType::VARCHAR;
		}

		auto first_inner_element = *nested_array.begin();

		if (first_inner_element.type() == bsoncxx::type::k_document) {
			LogicalType struct_type = InferStructTypeFromArray(nested_array, depth + 1);
			if (struct_type.id() == LogicalTypeId::STRUCT) {
				return LogicalType::LIST(struct_type);
			}
			return LogicalType::VARCHAR;
		} else if (first_inner_element.type() == bsoncxx::type::k_array) {
			LogicalType deeper_type = InferNestedArrayType(nested_array, depth + 1);
			if (deeper_type.id() == LogicalTypeId::LIST || deeper_type.id() == LogicalTypeId::VARCHAR) {
				return LogicalType::LIST(deeper_type);
			}
			return LogicalType::VARCHAR;
		} else {
			LogicalType element_type = InferTypeFromBSONElement(first_inner_element);
			return LogicalType::LIST(element_type);
		}
	} else {
		LogicalType element_type = InferTypeFromBSONElement(first_element);
		return LogicalType::LIST(element_type);
	}
}

// Helper function to infer STRUCT type from array of objects
// Scans multiple array elements to discover all fields
LogicalType InferStructTypeFromArray(bsoncxx::array::view array, int depth) {
	const int MAX_DEPTH = 5;
	const int MAX_ARRAY_ELEMENTS_TO_SCAN = 10;

	if (depth > MAX_DEPTH) {
		return LogicalType::VARCHAR; // Fallback to JSON for deeply nested
	}

	std::map<std::string, std::vector<LogicalType>> struct_fields;

	int element_count = 0;
	for (auto it = array.begin(); it != array.end() && element_count < MAX_ARRAY_ELEMENTS_TO_SCAN;
	     ++it, ++element_count) {
		bsoncxx::array::element array_element = *it;
		if (array_element.type() != bsoncxx::type::k_document) {
			// Not an array of objects - return VARCHAR
			return LogicalType::VARCHAR;
		}

		auto nested_doc = array_element.get_document().value;
		for (const auto &field : nested_doc) {
			std::string field_name = std::string(field.key().data(), field.key().length());

			LogicalType field_type;
			switch (field.type()) {
			case bsoncxx::type::k_document: {
				// Nested document - recursively infer STRUCT
				// For nested documents in arrays, we'll create nested STRUCT
				// For now, store as VARCHAR to avoid complexity
				field_type = LogicalType::VARCHAR;
				break;
			}
			case bsoncxx::type::k_array: {
				// Nested array - store as VARCHAR for now
				// TODO: Support nested arrays (LIST(LIST(...)) or LIST(STRUCT(...)))
				field_type = LogicalType::VARCHAR;
				break;
			}
			default: {
				field_type = InferTypeFromBSONElement(field);
				break;
			}
			}

			struct_fields[field_name].push_back(field_type);
		}
	}

	if (struct_fields.empty()) {
		return LogicalType::VARCHAR;
	}

	// Build STRUCT type from collected fields
	child_list_t<LogicalType> struct_children;
	for (const auto &pair : struct_fields) {
		LogicalType resolved_type = ResolveTypeConflict(pair.second);
		struct_children.push_back({pair.first, resolved_type});
	}

	return LogicalType::STRUCT(struct_children);
}

// Helper function to convert BSON document element to DuckDB Value
Value BSONElementToValue(const bsoncxx::document::element &element, const LogicalType &target_type) {
	if (!element || element.type() == bsoncxx::type::k_null) {
		return Value(target_type);
	}

	switch (target_type.id()) {
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
		} else {
			str_val = "<unknown>";
		}
		return Value(str_val);
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
		return Value::BIGINT(int_val);
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
		return Value::DOUBLE(double_val);
	}
	case LogicalTypeId::BOOLEAN: {
		bool bool_val = false;
		if (element.type() == bsoncxx::type::k_bool) {
			bool_val = element.get_bool().value;
		}
		return Value::BOOLEAN(bool_val);
	}
	case LogicalTypeId::DATE: {
		date_t date_val;
		if (element.type() == bsoncxx::type::k_date) {
			auto mongo_date = element.get_date();
			auto ms_since_epoch = mongo_date.to_int64();
			timestamp_t ts_val = Timestamp::FromEpochMs(ms_since_epoch);
			date_val = Timestamp::GetDate(ts_val);
		} else {
			date_val = date_t(0);
		}
		return Value::DATE(date_val);
	}
	case LogicalTypeId::TIMESTAMP: {
		timestamp_t ts_val;
		if (element.type() == bsoncxx::type::k_date) {
			auto date_val = element.get_date();
			ts_val = Timestamp::FromEpochMs(date_val.to_int64());
		} else {
			ts_val = Timestamp::FromEpochMs(0);
		}
		return Value::TIMESTAMP(ts_val);
	}
	default:
		return Value(target_type);
	}
}

// Helper function to convert BSON document to DuckDB STRUCT Value
Value BSONDocumentToStruct(const bsoncxx::document::view &doc, const LogicalType &struct_type) {
	if (struct_type.id() != LogicalTypeId::STRUCT) {
		return Value(struct_type);
	}

	auto child_types = StructType::GetChildTypes(struct_type);
	vector<Value> struct_values;

	for (const auto &child_type_pair : child_types) {
		const std::string &field_name = child_type_pair.first;
		const LogicalType &field_type = child_type_pair.second;

		auto field_element = doc[field_name];
		if (!field_element || field_element.type() == bsoncxx::type::k_null) {
			struct_values.push_back(Value(field_type));
		} else {
			struct_values.push_back(BSONElementToValue(field_element, field_type));
		}
	}

	return Value::STRUCT(struct_type, std::move(struct_values));
}

int GetBSONArrayDepth(const bsoncxx::array::view &array, int max_depth = 10) {
	if (max_depth <= 0 || array.begin() == array.end()) {
		return 0;
	}

	int max_element_depth = 0;
	for (const auto &element : array) {
		if (element.type() == bsoncxx::type::k_array) {
			auto nested_array = element.get_array().value;
			int nested_depth = 1 + GetBSONArrayDepth(nested_array, max_depth - 1);
			if (nested_depth > max_element_depth) {
				max_element_depth = nested_depth;
			}
		} else {
			if (max_element_depth < 1) {
				max_element_depth = 1;
			}
		}
	}
	return max_element_depth;
}

int GetListTypeDepth(const LogicalType &list_type) {
	int depth = 0;
	LogicalType current_type = list_type;
	while (current_type.id() == LogicalTypeId::LIST) {
		depth++;
		current_type = ListType::GetChildType(current_type);
	}
	return depth;
}

Value BSONArrayToList(const bsoncxx::array::view &array, const LogicalType &list_type) {
	if (list_type.id() != LogicalTypeId::LIST) {
		return Value(list_type);
	}

	D_ASSERT(list_type.id() == LogicalTypeId::LIST);

	int expected_depth = GetListTypeDepth(list_type);
	int actual_depth = GetBSONArrayDepth(array);

	auto child_type = ListType::GetChildType(list_type);

	// If actual depth is less than expected, wrap elements to match expected depth
	if (actual_depth < expected_depth) {
		// Determine the base element type (the innermost type)
		LogicalType base_type = child_type;
		for (int i = 1; i < expected_depth; i++) {
			if (base_type.id() == LogicalTypeId::LIST) {
				base_type = ListType::GetChildType(base_type);
			}
		}

		// Build the type that matches the actual depth of nested arrays
		LogicalType actual_nested_type = LogicalType::LIST(base_type);
		for (int i = 1; i < actual_depth - 1; i++) {
			actual_nested_type = LogicalType::LIST(actual_nested_type);
		}

		// Process elements directly and wrap them appropriately
		vector<Value> list_values;
		int depth_diff = expected_depth - actual_depth;

		for (const auto &array_element : array) {
			if (array_element.type() == bsoncxx::type::k_null) {
				list_values.push_back(Value(child_type));
			} else if (array_element.type() == bsoncxx::type::k_array) {
				// Convert nested array to its actual depth first
				auto nested_array = array_element.get_array().value;

				// Convert nested array elements directly to avoid depth check issues
				vector<Value> nested_list_values;
				for (const auto &nested_elem : nested_array) {
					if (nested_elem.type() == bsoncxx::type::k_null) {
						nested_list_values.push_back(Value(base_type));
					} else {
						Value elem_val;
						switch (nested_elem.type()) {
						case bsoncxx::type::k_string: {
							std::string str_val(nested_elem.get_string().value.data(),
							                    nested_elem.get_string().value.length());
							elem_val = Value(str_val);
							break;
						}
						case bsoncxx::type::k_int32:
							elem_val = Value::BIGINT(nested_elem.get_int32().value);
							break;
						case bsoncxx::type::k_int64:
							elem_val = Value::BIGINT(nested_elem.get_int64().value);
							break;
						case bsoncxx::type::k_double:
							elem_val = Value::DOUBLE(nested_elem.get_double().value);
							break;
						case bsoncxx::type::k_bool:
							elem_val = Value::BOOLEAN(nested_elem.get_bool().value);
							break;
						default:
							elem_val = Value(base_type);
							break;
						}
						Value casted_val;
						string error_msg;
						if (elem_val.DefaultTryCastAs(base_type, casted_val, &error_msg)) {
							nested_list_values.push_back(casted_val);
						} else {
							nested_list_values.push_back(Value(base_type));
						}
					}
				}

				// Create the nested list value at actual depth
				Value nested_value;
				try {
					nested_value = Value::LIST(base_type, std::move(nested_list_values));
				} catch (...) {
					list_values.push_back(Value(child_type));
					continue;
				}

				// Wrap the nested value to match child_type (wrap depth_diff times)
				// The nested array is at depth (actual_depth - 1), and we need it at depth (expected_depth - 1)
				// So we need to wrap it depth_diff times
				Value wrapped = nested_value;
				for (int i = 0; i < depth_diff; i++) {
					vector<Value> wrapper_list;
					wrapper_list.push_back(wrapped);
					LogicalType wrapper_child_type = wrapped.type();
					wrapped = Value::LIST(wrapper_child_type, std::move(wrapper_list));
				}
				list_values.push_back(wrapped);
			} else {
				// Non-array element - wrap it to match expected depth
				Value elem_val;
				switch (array_element.type()) {
				case bsoncxx::type::k_string: {
					std::string str_val(array_element.get_string().value.data(),
					                    array_element.get_string().value.length());
					elem_val = Value(str_val);
					break;
				}
				case bsoncxx::type::k_int32:
					elem_val = Value::BIGINT(array_element.get_int32().value);
					break;
				case bsoncxx::type::k_int64:
					elem_val = Value::BIGINT(array_element.get_int64().value);
					break;
				case bsoncxx::type::k_double:
					elem_val = Value::DOUBLE(array_element.get_double().value);
					break;
				case bsoncxx::type::k_bool:
					elem_val = Value::BOOLEAN(array_element.get_bool().value);
					break;
				default:
					elem_val = Value(base_type);
					break;
				}

				// Wrap element to match expected depth
				Value wrapped_elem = elem_val;
				for (int i = 0; i < depth_diff; i++) {
					vector<Value> wrapper_list;
					wrapper_list.push_back(wrapped_elem);
					LogicalType wrapper_child_type = wrapped_elem.type();
					wrapped_elem = Value::LIST(wrapper_child_type, std::move(wrapper_list));
				}
				list_values.push_back(wrapped_elem);
			}
		}

		try {
			return Value::LIST(child_type, std::move(list_values));
		} catch (...) {
			return Value(list_type);
		}
	}

	// If actual depth is greater than expected, return NULL (can't safely truncate)
	if (actual_depth > expected_depth) {
		return Value(list_type);
	}

	// Depths match - proceed with normal conversion
	vector<Value> list_values;

	for (const auto &array_element : array) {
		if (array_element.type() == bsoncxx::type::k_null) {
			list_values.push_back(Value(child_type));
		} else if (child_type.id() == LogicalTypeId::STRUCT && array_element.type() == bsoncxx::type::k_document) {
			auto doc = array_element.get_document().value;
			list_values.push_back(BSONDocumentToStruct(doc, child_type));
		} else if (child_type.id() == LogicalTypeId::LIST && array_element.type() == bsoncxx::type::k_array) {
			auto nested_array = array_element.get_array().value;
			int expected_nested_depth = GetListTypeDepth(child_type);
			int actual_nested_depth = GetBSONArrayDepth(nested_array);

			if (actual_nested_depth != expected_nested_depth) {
				// Try to convert shallower arrays by wrapping
				if (actual_nested_depth < expected_nested_depth) {
					Value nested_value = BSONArrayToList(nested_array, child_type);
					list_values.push_back(nested_value);
				} else {
					list_values.push_back(Value(child_type));
				}
			} else {
				list_values.push_back(BSONArrayToList(nested_array, child_type));
			}
		} else {
			if (child_type.id() == LogicalTypeId::LIST) {
				list_values.push_back(Value(child_type));
			} else {
				Value elem_val;
				switch (array_element.type()) {
				case bsoncxx::type::k_string: {
					std::string str_val(array_element.get_string().value.data(),
					                    array_element.get_string().value.length());
					elem_val = Value(str_val);
					break;
				}
				case bsoncxx::type::k_int32:
					elem_val = Value::BIGINT(array_element.get_int32().value);
					break;
				case bsoncxx::type::k_int64:
					elem_val = Value::BIGINT(array_element.get_int64().value);
					break;
				case bsoncxx::type::k_double:
					elem_val = Value::DOUBLE(array_element.get_double().value);
					break;
				case bsoncxx::type::k_bool:
					elem_val = Value::BOOLEAN(array_element.get_bool().value);
					break;
				default:
					elem_val = Value(child_type);
					break;
				}
				Value casted_val;
				string error_msg;
				if (elem_val.DefaultTryCastAs(child_type, casted_val, &error_msg)) {
					list_values.push_back(casted_val);
				} else {
					list_values.push_back(Value(child_type));
				}
			}
		}
	}

	try {
		return Value::LIST(child_type, std::move(list_values));
	} catch (...) {
		return Value(list_type);
	}
}

void CollectFieldPaths(const bsoncxx::document::view &doc, const std::string &prefix, int depth,
                       std::unordered_map<std::string, vector<LogicalType>> &field_types,
                       std::unordered_map<std::string, std::string> &flattened_to_mongo_path,
                       const std::string &mongo_prefix = "") {
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
		// Track original MongoDB path: use dots for nested fields, original name for top-level
		std::string mongo_path = mongo_prefix.empty() ? field_name : mongo_prefix + "." + field_name;
		flattened_to_mongo_path[full_path] = mongo_path;

		switch (element.type()) {
		case bsoncxx::type::k_document: {
			// Recursively process nested document
			auto nested_doc = element.get_document().value;
			CollectFieldPaths(nested_doc, full_path, depth + 1, field_types, flattened_to_mongo_path, mongo_path);
			// Don't store the document itself as JSON when we have nested fields
			// This prevents VARCHAR from overriding nested STRUCT types
			break;
		}
		case bsoncxx::type::k_array: {
			auto array = element.get_array().value;
			if (array.begin() == array.end()) {
				// Empty array - store as VARCHAR
				field_types[full_path].push_back(LogicalType::VARCHAR);
				break;
			}

			// Check first element to determine array type
			auto first_element = *array.begin();

			if (first_element.type() == bsoncxx::type::k_document) {
				// Array of objects - infer STRUCT type and create LIST(STRUCT(...))
				LogicalType struct_type = InferStructTypeFromArray(array, depth);
				if (struct_type.id() == LogicalTypeId::STRUCT) {
					// Create LIST(STRUCT(...)) type
					LogicalType list_type = LogicalType::LIST(struct_type);
					field_types[full_path].push_back(list_type);
				} else {
					// Fallback to VARCHAR if struct inference failed
					field_types[full_path].push_back(LogicalType::VARCHAR);
				}
			} else if (first_element.type() == bsoncxx::type::k_array) {
				// Array of arrays - recursively infer nested array type
				LogicalType nested_list_type = InferNestedArrayType(array, depth);
				if (nested_list_type.id() == LogicalTypeId::LIST) {
					// Create LIST(LIST(...)) type
					LogicalType list_type = LogicalType::LIST(nested_list_type);
					field_types[full_path].push_back(list_type);
				} else {
					// Fallback to VARCHAR if inference failed
					field_types[full_path].push_back(LogicalType::VARCHAR);
				}
			} else {
				// Array of primitives - infer element type and create LIST
				LogicalType element_type = InferTypeFromBSONElement(first_element);
				LogicalType list_type = LogicalType::LIST(element_type);
				field_types[full_path].push_back(list_type);
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
                              vector<LogicalType> &column_types,
                              unordered_map<string, string> &column_name_to_mongo_path) {
	std::unordered_map<std::string, vector<LogicalType>> field_types;

	// Sample documents
	mongocxx::options::find opts;
	opts.limit(sample_size);

	auto cursor = collection.find({}, opts);

	int64_t count = 0;
	for (const auto &doc : cursor) {
		CollectFieldPaths(doc, "", 0, field_types, column_name_to_mongo_path, "");
		count++;
		if (count >= sample_size) {
			break;
		}
	}

	// Always include _id column (present in all MongoDB documents)
	// If collection is empty, we still need at least one column
	if (field_types.find("_id") == field_types.end()) {
		field_types["_id"] = {LogicalType::VARCHAR}; // ObjectId as string
		column_name_to_mongo_path["_id"] = "_id";    // Map _id to itself
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
                     const vector<LogicalType> &column_types, DataChunk &output, idx_t row_idx,
                     const unordered_map<string, string> &column_name_to_mongo_path) {
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
		const auto &column_name = column_names[col_idx];
		const auto &column_type = column_types[col_idx];

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
					if (!empty_list.DefaultTryCastAs(column_type, casted_list, &error_msg)) {
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
					if (!list_value.DefaultTryCastAs(column_type, casted_list, &error_msg)) {
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
				if (!struct_value.DefaultTryCastAs(column_type, casted_struct, &error_msg)) {
					FlatVector::SetNull(vec, row_idx, true);
					continue;
				}
				struct_value = casted_struct;
			}
			vec.SetValue(row_idx, struct_value);
			continue;
		}

		auto element = doc[column_name];

		if (!element) {
			// Try MongoDB path-based lookup for nested fields
			auto path_it = column_name_to_mongo_path.find(column_name);
			if (path_it != column_name_to_mongo_path.end()) {
				element = getElementByMongoPath(path_it->second);
			} else {
				element = getElementByPath(column_name);
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
	InferSchemaFromDocuments(collection, result->sample_size, result->column_names, result->column_types,
	                         result->column_name_to_mongo_path);

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
void AppendValueToDocument(bsoncxx::builder::basic::document &doc_builder, const string &key, const Value &value,
                           const LogicalType &type) {
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
		doc_builder.append(
		    bsoncxx::builder::basic::kvp(key, bsoncxx::types::b_date {std::chrono::milliseconds(ms_since_epoch)}));
		break;
	}
	case LogicalTypeId::TIMESTAMP: {
		auto timestamp_val = value.GetValue<timestamp_t>();
		auto ms_since_epoch = Timestamp::GetEpochMs(timestamp_val);
		doc_builder.append(
		    bsoncxx::builder::basic::kvp(key, bsoncxx::types::b_date {std::chrono::milliseconds(ms_since_epoch)}));
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
		if (in_filter.values.empty()) {
			break;
		}
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
					for (auto nested_it = it->get_document().value.begin(); nested_it != it->get_document().value.end();
					     ++nested_it) {
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
			if (conj_filter.child_filters.empty()) {
				break;
			}
			bsoncxx::builder::basic::array or_array;
			int appended_count = 0;
			for (const auto &child_filter : conj_filter.child_filters) {
				auto child_doc = ConvertSingleFilterToMongo(*child_filter, column_name, column_type);
				// Only append non-empty documents to avoid BSON validation warnings
				if (!child_doc.view().empty()) {
					or_array.append(child_doc.view());
					appended_count++;
				}
			}
			if (appended_count > 0) {
				doc.append(bsoncxx::builder::basic::kvp("$or", or_array.extract()));
			}
		}
		break;
	}
	default:
		break;
	}

	return doc.extract();
}

bsoncxx::document::value ConvertFiltersToMongoQuery(optional_ptr<TableFilterSet> filters,
                                                    const vector<string> &column_names,
                                                    const vector<LogicalType> &column_types,
                                                    const unordered_map<string, string> &column_name_to_mongo_path) {
	bsoncxx::builder::basic::document query_builder;

	if (!filters || filters->filters.empty()) {
		return query_builder.extract();
	}

	map<string, bsoncxx::builder::basic::document> column_filters;

	for (const auto &filter_pair : filters->filters) {
		idx_t col_idx = filter_pair.first;
		if (col_idx >= column_names.size()) {
			continue;
		}

		const auto &filter = filter_pair.second;
		const string &column_name = column_names[col_idx];
		const LogicalType &column_type = col_idx < column_types.size() ? column_types[col_idx] : LogicalType::VARCHAR;

		string mongo_column_name;
		auto path_it = column_name_to_mongo_path.find(column_name);
		if (path_it != column_name_to_mongo_path.end()) {
			mongo_column_name = path_it->second;
		} else {
			mongo_column_name = column_name;
		}

		bsoncxx::document::value filter_doc = bsoncxx::builder::basic::document {}.extract();

		if (column_type.id() == LogicalTypeId::LIST) {
			auto child_type = ListType::GetChildType(column_type);
			if (child_type.id() == LogicalTypeId::STRUCT) {
				string field_path = mongo_column_name;
				auto elem_match_doc = ConvertSingleFilterToMongo(*filter, field_path, child_type);
				if (!elem_match_doc.view().empty()) {
					bsoncxx::builder::basic::document elem_match_builder;
					for (auto doc_it = elem_match_doc.view().begin(); doc_it != elem_match_doc.view().end(); ++doc_it) {
						elem_match_builder.append(bsoncxx::builder::basic::kvp(doc_it->key(), doc_it->get_value()));
					}
					bsoncxx::builder::basic::document array_filter_builder;
					array_filter_builder.append(
					    bsoncxx::builder::basic::kvp("$elemMatch", elem_match_builder.extract()));
					bsoncxx::builder::basic::document final_doc;
					final_doc.append(bsoncxx::builder::basic::kvp(mongo_column_name, array_filter_builder.extract()));
					filter_doc = final_doc.extract();
				} else {
					continue;
				}
			} else {
				filter_doc = ConvertSingleFilterToMongo(*filter, mongo_column_name, child_type);
			}
		} else {
			filter_doc = ConvertSingleFilterToMongo(*filter, mongo_column_name, column_type);
		}

		if (filter_doc.view().empty()) {
			continue;
		}

		if (column_type.id() == LogicalTypeId::LIST) {
			auto child_type = ListType::GetChildType(column_type);
			if (child_type.id() == LogicalTypeId::STRUCT) {
				for (auto doc_it = filter_doc.view().begin(); doc_it != filter_doc.view().end(); ++doc_it) {
					query_builder.append(bsoncxx::builder::basic::kvp(doc_it->key(), doc_it->get_value()));
				}
				continue;
			}
		}

		// Extract column filter from document
		bsoncxx::types::bson_value::view column_filter_value;
		bool has_column_filter = false;
		for (auto doc_it = filter_doc.view().begin(); doc_it != filter_doc.view().end(); ++doc_it) {
			if (doc_it->key() == mongo_column_name) {
				column_filter_value = doc_it->get_value();
				has_column_filter = true;
				break;
			}
		}

		if (!has_column_filter) {
			// Top-level operator like $or - add directly to query
			for (auto doc_it = filter_doc.view().begin(); doc_it != filter_doc.view().end(); ++doc_it) {
				query_builder.append(bsoncxx::builder::basic::kvp(doc_it->key(), doc_it->get_value()));
			}
			continue;
		}

		// Merge filters for same column (use MongoDB path as key)
		auto it = column_filters.find(mongo_column_name);
		if (it == column_filters.end()) {
			if (column_filter_value.type() == bsoncxx::type::k_document) {
				// Already has operators like {$lt: value} - wrap in document
				bsoncxx::builder::basic::document col_doc;
				for (auto nested_it = column_filter_value.get_document().value.begin();
				     nested_it != column_filter_value.get_document().value.end(); ++nested_it) {
					col_doc.append(bsoncxx::builder::basic::kvp(nested_it->key(), nested_it->get_value()));
				}
				column_filters[mongo_column_name] = std::move(col_doc);
			} else {
				bsoncxx::builder::basic::document col_doc;
				col_doc.append(bsoncxx::builder::basic::kvp(mongo_column_name, column_filter_value));
				column_filters[mongo_column_name] = std::move(col_doc);
			}
		} else {
			if (column_filter_value.type() == bsoncxx::type::k_document) {
				for (auto nested_it = column_filter_value.get_document().value.begin();
				     nested_it != column_filter_value.get_document().value.end(); ++nested_it) {
					it->second.append(bsoncxx::builder::basic::kvp(nested_it->key(), nested_it->get_value()));
				}
			} else {
				it->second = bsoncxx::builder::basic::document {};
				it->second.append(bsoncxx::builder::basic::kvp(mongo_column_name, column_filter_value));
			}
		}
	}

	// Add column filters to query
	for (auto &col_filter_pair : column_filters) {
		auto col_doc = col_filter_pair.second.extract();
		auto col_doc_view = col_doc.view();
		auto it = col_doc_view.begin();
		if (it != col_doc_view.end() && it->key() == col_filter_pair.first) {
			auto next_it = it;
			++next_it;
			if (next_it == col_doc_view.end()) {
				query_builder.append(bsoncxx::builder::basic::kvp(col_filter_pair.first, it->get_value()));
				continue;
			}
		}
		query_builder.append(bsoncxx::builder::basic::kvp(col_filter_pair.first, col_doc));
	}

	return query_builder.extract();
}

bsoncxx::document::value BuildMongoProjection(const vector<column_t> &column_ids,
                                              const vector<string> &all_column_names,
                                              const unordered_map<string, string> &column_name_to_mongo_path) {
	bsoncxx::builder::basic::document projection_builder;

	// Use a set to track which fields we've already added (avoid duplicates)
	unordered_set<string> added_fields;
	bool has_id = false;

	for (column_t col_id : column_ids) {
		// Skip virtual columns (like ROWID) - virtual columns start at VIRTUAL_COLUMN_START
		if (col_id >= VIRTUAL_COLUMN_START) {
			continue;
		}

		idx_t col_idx = col_id;
		if (col_idx >= all_column_names.size()) {
			continue;
		}

		const string &column_name = all_column_names[col_idx];

		// Get MongoDB path for this column
		string mongo_path;
		auto path_it = column_name_to_mongo_path.find(column_name);
		if (path_it != column_name_to_mongo_path.end()) {
			mongo_path = path_it->second;
		} else {
			mongo_path = column_name;
		}

		// Skip if we've already added this field
		if (added_fields.find(mongo_path) != added_fields.end()) {
			continue;
		}

		// Check for path collisions: if we're adding a nested field (e.g., "address.zip"),
		// we cannot also have the parent field ("address") in the projection
		// MongoDB will automatically include parent documents when projecting nested fields
		size_t dot_pos = mongo_path.find('.');
		if (dot_pos != string::npos) {
			string parent_path = mongo_path.substr(0, dot_pos);
			// If parent path was already added, skip to avoid collision
			if (added_fields.find(parent_path) != added_fields.end()) {
				continue;
			}
		} else {
			// If we're adding a parent field, check if any nested children are already added
			// If so, skip the parent to avoid collision
			bool has_nested_child = false;
			for (const auto &added : added_fields) {
				if (added.find(mongo_path + ".") == 0) {
					has_nested_child = true;
					break;
				}
			}
			if (has_nested_child) {
				continue; // Skip parent if nested child exists
			}
		}

		// Track if _id is included
		if (mongo_path == "_id") {
			has_id = true;
		}

		// Add field to projection (1 means include)
		projection_builder.append(bsoncxx::builder::basic::kvp(mongo_path, 1));
		added_fields.insert(mongo_path);
	}

	// If we have no real fields, return empty document (no projection = return all fields)
	if (added_fields.empty()) {
		return bsoncxx::builder::basic::document {}.extract();
	}

	// Include _id if it wasn't already included (MongoDB typically includes _id by default)
	// Only add _id if we have other fields to project
	if (!has_id) {
		projection_builder.append(bsoncxx::builder::basic::kvp("_id", 1));
	}

	return projection_builder.extract();
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

	// Projection pushdown: collect columns needed (selected + filter columns that couldn't be pushed down)
	unordered_set<idx_t> needed_column_indices;

	// Add selected columns
	for (column_t col_id : input.column_ids) {
		if (col_id < VIRTUAL_COLUMN_START && col_id < data.column_names.size()) {
			needed_column_indices.insert(col_id);
		}
	}

	// Get collection
	auto db = result->connection->client[result->database_name];
	auto collection = db[result->collection_name];

	// Build query from pushed-down filters first to determine which filters were successfully pushed down
	bsoncxx::document::view_or_value query_filter;
	bool filters_pushed_down = false;
	if (input.filters) {
		// Map filter column indices from column_ids space to schema space
		unordered_map<idx_t, idx_t> filter_index_map;
		for (size_t i = 0; i < input.column_ids.size(); i++) {
			column_t col_id = input.column_ids[i];
			if (col_id < VIRTUAL_COLUMN_START && col_id < data.column_names.size()) {
				filter_index_map[i] = col_id;
			}
		}

		// Create a new filter set with remapped indices
		auto remapped_filters = make_uniq<TableFilterSet>();
		for (const auto &filter_pair : input.filters->filters) {
			idx_t filter_col_idx = filter_pair.first;
			auto it = filter_index_map.find(filter_col_idx);
			if (it != filter_index_map.end()) {
				idx_t schema_col_idx = it->second;
				remapped_filters->filters[schema_col_idx] = filter_pair.second->Copy();
			}
		}

		// Only attempt conversion if we successfully remapped at least one filter
		if (!remapped_filters->filters.empty()) {
			// Convert DuckDB filters to MongoDB query using remapped indices
			auto mongo_filter = ConvertFiltersToMongoQuery(remapped_filters.get(), data.column_names, data.column_types,
			                                               data.column_name_to_mongo_path);
			
			// Check if filters were successfully pushed down (non-empty MongoDB query)
			// If filters are pushed down to MongoDB, MongoDB filters server-side and we don't need filter columns
			auto filter_view = mongo_filter.view();
			// Count non-empty fields in the filter document
			idx_t filter_field_count = 0;
			for (auto it = filter_view.begin(); it != filter_view.end(); ++it) {
				filter_field_count++;
			}
			// Filters were pushed down if we have a non-empty filter document
			// (empty document means conversion failed or filters couldn't be converted)
			filters_pushed_down = (filter_field_count > 0);
			
			query_filter = std::move(mongo_filter);
		} else {
			// No filters could be remapped, so they can't be pushed down
			filters_pushed_down = false;
			query_filter = bsoncxx::builder::stream::document {} << bsoncxx::builder::stream::finalize;
		}
	} else if (!result->filter_query.empty()) {
		query_filter = bsoncxx::from_json(result->filter_query);
		filters_pushed_down = true; // Manual filter query means filters are pushed down
	} else {
		query_filter = bsoncxx::builder::stream::document {} << bsoncxx::builder::stream::finalize;
	}

	// Only add filter columns if filters couldn't be pushed down (for post-scan filtering in DuckDB)
	// If filters are successfully pushed down to MongoDB, MongoDB filters server-side before returning
	// results, so we don't need those filter columns in the projection.
	// TODO: Once pushdown_complex_filter is implemented, we can track which specific filters
	//       failed to push down and only add columns for those filters (some filters may push down
	//       while others remain in DuckDB)
	if (input.filters && !input.filters->filters.empty() && !input.column_ids.empty() && !filters_pushed_down) {
		// Map filter indices from column_ids space to schema space
		unordered_map<idx_t, idx_t> filter_index_map;
		for (size_t i = 0; i < input.column_ids.size(); i++) {
			column_t col_id = input.column_ids[i];
			if (col_id < VIRTUAL_COLUMN_START && col_id < data.column_names.size()) {
				filter_index_map[i] = col_id;
			}
		}
		// Add filter columns to needed set (only if filters weren't pushed down)
		for (const auto &filter_pair : input.filters->filters) {
			idx_t filter_col_idx = filter_pair.first;
			auto it = filter_index_map.find(filter_col_idx);
			if (it != filter_index_map.end()) {
				needed_column_indices.insert(it->second);
			}
		}
	}

	// Store requested columns in schema order for consistent projection
	vector<idx_t> sorted_indices(needed_column_indices.begin(), needed_column_indices.end());
	std::sort(sorted_indices.begin(), sorted_indices.end());

	for (idx_t col_idx : sorted_indices) {
		result->requested_column_indices.push_back(col_idx);
		result->requested_column_names.push_back(data.column_names[col_idx]);
		result->requested_column_types.push_back(data.column_types[col_idx]);
	}

	// Build MongoDB find options
	mongocxx::options::find opts;

	// Build MongoDB projection from requested columns
	if (!result->requested_column_indices.empty()) {
		vector<column_t> projection_column_ids(result->requested_column_indices.begin(),
		                                       result->requested_column_indices.end());
		auto projection_doc =
		    BuildMongoProjection(projection_column_ids, data.column_names, data.column_name_to_mongo_path);

		// Check if projection document has fields (empty means return all fields)
		auto proj_view = projection_doc.view();
		int field_count = 0;
		for (auto it = proj_view.begin(); it != proj_view.end(); ++it) {
			field_count++;
		}

		if (field_count > 0) {
			// Store projection document to keep it alive for cursor lifetime
			result->projection_document = std::move(projection_doc);
			opts.projection(result->projection_document.view());
		}
	}

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

	// Create cursor with query filter and options (including projection if set)
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

	// Handle COUNT(*) queries: output has 1 column but may have filter columns in requested_column_names
	if (output.ColumnCount() == 1 && state.requested_column_names.size() > 1) {
		state.requested_column_names.clear();
		state.requested_column_types.clear();
		state.requested_column_indices.clear();

		while (count < max_count && *state.current != *state.end) {
			++(*state.current);
			count++;
		}
		output.SetCardinality(count);
		if (*state.current == *state.end) {
			state.finished = true;
		}
		return;
	}

	// Use requested columns if projection pushdown is active, otherwise use all columns
	const vector<string> *column_names;
	const vector<LogicalType> *column_types;

	if (!state.requested_column_names.empty()) {
		column_names = &state.requested_column_names;
		column_types = &state.requested_column_types;
	} else {
		column_names = &bind_data.column_names;
		column_types = &bind_data.column_types;
	}

	if (output.data.empty()) {
		output.Initialize(context, *column_types);
	}

	idx_t num_cols_to_use = MinValue<idx_t>(column_names->size(), output.ColumnCount());

	// Initialize vectors for scanning
	for (idx_t col_idx = 0; col_idx < num_cols_to_use; col_idx++) {
		auto &vec = output.data[col_idx];
		vec.SetVectorType(VectorType::FLAT_VECTOR);
		if (((*column_types)[col_idx].id() == LogicalTypeId::LIST ||
		     (*column_types)[col_idx].id() == LogicalTypeId::STRUCT) &&
		    !vec.GetAuxiliary()) {
			vec.Initialize(false, STANDARD_VECTOR_SIZE);
		}
	}

	// Scan documents and flatten into output
	while (count < max_count && *state.current != *state.end) {
		auto doc = **state.current;
		vector<string> trunc_names(column_names->begin(), column_names->begin() + num_cols_to_use);
		vector<LogicalType> trunc_types(column_types->begin(), column_types->begin() + num_cols_to_use);
		FlattenDocument(doc, trunc_names, trunc_types, output, count, bind_data.column_name_to_mongo_path);
		++(*state.current);
		count++;
	}

	output.SetCardinality(count);

	if (state.current && state.end && *state.current == *state.end) {
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
