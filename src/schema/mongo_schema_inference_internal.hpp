#pragma once

#include "mongo_table_function.hpp"

#include <bsoncxx/array/view.hpp>
#include <bsoncxx/document/element.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/types.hpp>

namespace duckdb {

std::string NormalizeJson(const std::string &json);

template <typename ElementType>
LogicalType InferTypeFromBSONElement(const ElementType &element) {
	switch (element.type()) {
	case bsoncxx::type::k_string:
		return LogicalType::VARCHAR;
	case bsoncxx::type::k_int32:
	case bsoncxx::type::k_int64:
		return LogicalType::BIGINT;
	case bsoncxx::type::k_double:
	case bsoncxx::type::k_decimal128:
		// Decimal128 maps to DOUBLE (accepts precision loss for numeric operations)
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
	case bsoncxx::type::k_null:
	case bsoncxx::type::k_undefined:
		return LogicalType::VARCHAR; // NULL/undefined - type will be refined from other documents
	case bsoncxx::type::k_regex:
	case bsoncxx::type::k_code:
	case bsoncxx::type::k_codewscope:
	case bsoncxx::type::k_symbol:
	case bsoncxx::type::k_timestamp:
	case bsoncxx::type::k_dbpointer:
	case bsoncxx::type::k_minkey:
	case bsoncxx::type::k_maxkey:
		return LogicalType::VARCHAR; // Special types as string representation
	default:
		return LogicalType::VARCHAR;
	}
}

std::string GetBSONTypeName(bsoncxx::type type);

LogicalType InferStructTypeFromArray(bsoncxx::array::view array, int depth);
LogicalType InferNestedArrayType(bsoncxx::array::view array, int depth);

Value BSONElementToValue(const bsoncxx::document::element &element, const LogicalType &target_type);
Value BSONDocumentToStruct(const bsoncxx::document::view &doc, const LogicalType &struct_type);

int GetBSONArrayDepth(const bsoncxx::array::view &array, int max_depth = 10);
int GetListTypeDepth(const LogicalType &list_type);
Value BSONArrayToList(const bsoncxx::array::view &array, const LogicalType &list_type);

bool IsBSONTypeCompatible(bsoncxx::type bson_type, LogicalTypeId expected_type);

} // namespace duckdb
