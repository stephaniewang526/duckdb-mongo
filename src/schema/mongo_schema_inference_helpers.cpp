#include "mongo_schema_inference_internal.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <map>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace duckdb {

std::string NormalizeJson(const std::string &json) {
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

std::string GetBSONTypeName(bsoncxx::type type) {
	switch (type) {
	case bsoncxx::type::k_double:
		return "double";
	case bsoncxx::type::k_string:
		return "string";
	case bsoncxx::type::k_document:
		return "document";
	case bsoncxx::type::k_array:
		return "array";
	case bsoncxx::type::k_binary:
		return "binary";
	case bsoncxx::type::k_undefined:
		return "undefined";
	case bsoncxx::type::k_oid:
		return "objectId";
	case bsoncxx::type::k_bool:
		return "bool";
	case bsoncxx::type::k_date:
		return "date";
	case bsoncxx::type::k_null:
		return "null";
	case bsoncxx::type::k_regex:
		return "regex";
	case bsoncxx::type::k_dbpointer:
		return "dbPointer";
	case bsoncxx::type::k_code:
		return "javascript";
	case bsoncxx::type::k_symbol:
		return "symbol";
	case bsoncxx::type::k_codewscope:
		return "javascriptWithScope";
	case bsoncxx::type::k_int32:
		return "int32";
	case bsoncxx::type::k_timestamp:
		return "timestamp";
	case bsoncxx::type::k_int64:
		return "int64";
	case bsoncxx::type::k_decimal128:
		return "decimal128";
	case bsoncxx::type::k_minkey:
		return "minKey";
	case bsoncxx::type::k_maxkey:
		return "maxKey";
	default:
		return "unknown";
	}
}

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

int GetBSONArrayDepth(const bsoncxx::array::view &array, int max_depth) {
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
				Value casted_val;
				string error_msg;
				if (elem_val.DefaultTryCastAs(base_type, casted_val, &error_msg)) {
					// Wrap the casted value to match expected depth
					Value wrapped = casted_val;
					for (int i = 0; i < depth_diff; i++) {
						vector<Value> wrapper_list;
						wrapper_list.push_back(wrapped);
						LogicalType wrapper_child_type = wrapped.type();
						wrapped = Value::LIST(wrapper_child_type, std::move(wrapper_list));
					}
					list_values.push_back(wrapped);
				} else {
					list_values.push_back(Value(child_type));
				}
			}
		}

		try {
			return Value::LIST(child_type, std::move(list_values));
		} catch (...) {
			return Value(list_type);
		}
	}

	auto child_type_id = child_type.id();
	vector<Value> list_values;

	for (const auto &array_element : array) {
		if (array_element.type() == bsoncxx::type::k_null) {
			list_values.push_back(Value(child_type));
		} else if (array_element.type() == bsoncxx::type::k_array) {
			auto nested_array = array_element.get_array().value;
			Value nested_list_value = BSONArrayToList(nested_array, child_type);
			list_values.push_back(nested_list_value);
		} else if (array_element.type() == bsoncxx::type::k_document && child_type_id == LogicalTypeId::STRUCT) {
			auto nested_doc = array_element.get_document().value;
			list_values.push_back(BSONDocumentToStruct(nested_doc, child_type));
		} else {
			Value elem_val;
			switch (array_element.type()) {
			case bsoncxx::type::k_string: {
				std::string str_val(array_element.get_string().value.data(), array_element.get_string().value.length());
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

	try {
		return Value::LIST(child_type, std::move(list_values));
	} catch (...) {
		return Value(list_type);
	}
}

bool IsBSONTypeCompatible(bsoncxx::type bson_type, LogicalTypeId expected_type) {
	switch (expected_type) {
	case LogicalTypeId::VARCHAR:
		return true; // Everything can be converted to string

	case LogicalTypeId::BIGINT:
		// Accept int32, int64, double (if within range)
		return bson_type == bsoncxx::type::k_int32 || bson_type == bsoncxx::type::k_int64 ||
		       bson_type == bsoncxx::type::k_double;

	case LogicalTypeId::HUGEINT:
		// Accept int32, int64, double, decimal128
		return bson_type == bsoncxx::type::k_int32 || bson_type == bsoncxx::type::k_int64 ||
		       bson_type == bsoncxx::type::k_double || bson_type == bsoncxx::type::k_decimal128;

	case LogicalTypeId::DOUBLE:
		// Accept int32, int64, double, decimal128
		return bson_type == bsoncxx::type::k_int32 || bson_type == bsoncxx::type::k_int64 ||
		       bson_type == bsoncxx::type::k_double || bson_type == bsoncxx::type::k_decimal128;

	case LogicalTypeId::BOOLEAN:
		return bson_type == bsoncxx::type::k_bool;

	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
		return bson_type == bsoncxx::type::k_date;

	case LogicalTypeId::STRUCT:
		return bson_type == bsoncxx::type::k_document;

	case LogicalTypeId::LIST:
		return bson_type == bsoncxx::type::k_array;

	default:
		return false;
	}
}

} // namespace duckdb
