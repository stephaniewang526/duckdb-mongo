#include "mongo_variant.hpp"

#include "duckdb/common/types/variant/variant_builder.hpp"

#include <bsoncxx/array/view.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/types.hpp>
#include <algorithm>
#include <cstring>

namespace duckdb {

const char *const MONGO_ROOT_PATH = "$$ROOT";

namespace {

// Emit a value with no payload (booleans, null).
void EmitTag(VariantBuilder &builder, VariantLogicalType type) {
	builder.type_ids.push_back(static_cast<uint8_t>(type));
	builder.byte_offsets.push_back(static_cast<uint32_t>(builder.blob.size()));
}

template <class T>
void EmitFixed(VariantBuilder &builder, VariantLogicalType type, T value) {
	EmitTag(builder, type);
	VariantBuilderAppendFixed(builder.blob, value);
}

void EmitString(VariantBuilder &builder, VariantLogicalType type, const char *data, idx_t size) {
	EmitTag(builder, type);
	VariantBuilderAppendVarint(builder.blob, NumericCast<uint32_t>(size));
	VariantBuilderAppendBytes(builder.blob, const_data_ptr_cast(data), size);
}

void EmitString(VariantBuilder &builder, const std::string &str) {
	EmitString(builder, VariantLogicalType::VARCHAR, str.data(), str.size());
}

void EmitString(VariantBuilder &builder, bsoncxx::stdx::string_view str) {
	EmitString(builder, VariantLogicalType::VARCHAR, str.data(), str.size());
}

// Decimal128 renders as plain digits ("123.45", "-0.001") or scientific notation ("1.5E+3", "NaN"). The plain form
// becomes a VARIANT DECIMAL when it fits width 38; anything else falls back to DOUBLE.
bool TryEmitDecimal(VariantBuilder &builder, const std::string &str) {
	idx_t pos = 0;
	bool negative = false;
	if (pos < str.size() && (str[pos] == '-' || str[pos] == '+')) {
		negative = str[pos] == '-';
		pos++;
	}
	hugeint_t value = 0;
	uint32_t digits = 0;
	uint32_t scale = 0;
	bool seen_dot = false;
	bool seen_digit = false;
	for (; pos < str.size(); pos++) {
		char c = str[pos];
		if (c == '.') {
			if (seen_dot) {
				return false;
			}
			seen_dot = true;
			continue;
		}
		if (c < '0' || c > '9') {
			return false;
		}
		seen_digit = true;
		value = value * hugeint_t(10) + hugeint_t(c - '0');
		if (digits > 0 || c != '0') {
			digits++;
		}
		if (seen_dot) {
			scale++;
		}
	}
	if (!seen_digit) {
		return false;
	}
	uint32_t width = MaxValue<uint32_t>(MaxValue<uint32_t>(digits, scale), 1);
	if (width > DecimalType::MaxWidth()) {
		return false;
	}
	if (negative) {
		value = -value;
	}
	EmitTag(builder, VariantLogicalType::DECIMAL);
	VariantBuilderAppendVarint(builder.blob, width);
	VariantBuilderAppendVarint(builder.blob, scale);
	switch (VariantDecimalPhysicalType(width)) {
	case PhysicalType::INT16:
		VariantBuilderAppendFixed(builder.blob, static_cast<int16_t>(value.lower));
		break;
	case PhysicalType::INT32:
		VariantBuilderAppendFixed(builder.blob, static_cast<int32_t>(value.lower));
		break;
	case PhysicalType::INT64:
		VariantBuilderAppendFixed(builder.blob, static_cast<int64_t>(value.lower));
		break;
	default:
		VariantBuilderAppendFixed(builder.blob, value);
		break;
	}
	return true;
}

void EmitBsonValue(const bsoncxx::types::bson_value::view &value, VariantBuilder &builder);

void EmitDocument(const bsoncxx::document::view &doc, VariantBuilder &builder) {
	vector<bsoncxx::document::element> children;
	for (auto element : doc) {
		children.push_back(element);
	}
	// VARIANT objects keep their children in lexicographic key order
	std::sort(children.begin(), children.end(),
	          [](const bsoncxx::document::element &a, const bsoncxx::document::element &b) {
		          auto ka = a.key();
		          auto kb = b.key();
		          auto cmp = std::memcmp(ka.data(), kb.data(), MinValue<size_t>(ka.size(), kb.size()));
		          return cmp < 0 || (cmp == 0 && ka.size() < kb.size());
	          });
	builder.EmitObject(
	    children.size(),
	    [&](idx_t i) {
		    auto key = children[i].key();
		    return string_t(key.data(), NumericCast<uint32_t>(key.size()));
	    },
	    [&](idx_t i) { EmitBsonValue(children[i].get_value(), builder); });
}

void EmitArray(const bsoncxx::array::view &array, VariantBuilder &builder) {
	vector<bsoncxx::array::element> items;
	for (auto element : array) {
		items.push_back(element);
	}
	builder.EmitArray(items.size(), [&](idx_t i) { EmitBsonValue(items[i].get_value(), builder); });
}

void EmitBsonValue(const bsoncxx::types::bson_value::view &value, VariantBuilder &builder) {
	using bsoncxx::type;
	switch (value.type()) {
	case type::k_document:
		EmitDocument(value.get_document().value, builder);
		break;
	case type::k_array:
		EmitArray(value.get_array().value, builder);
		break;
	case type::k_string:
		EmitString(builder, value.get_string().value);
		break;
	case type::k_int32:
		EmitFixed(builder, VariantLogicalType::INT32, value.get_int32().value);
		break;
	case type::k_int64:
		EmitFixed(builder, VariantLogicalType::INT64, value.get_int64().value);
		break;
	case type::k_double:
		EmitFixed(builder, VariantLogicalType::DOUBLE, value.get_double().value);
		break;
	case type::k_bool:
		EmitTag(builder, value.get_bool().value ? VariantLogicalType::BOOL_TRUE : VariantLogicalType::BOOL_FALSE);
		break;
	case type::k_date:
		EmitFixed(builder, VariantLogicalType::TIMESTAMP_MILIS, timestamp_ms_t(value.get_date().to_int64()));
		break;
	case type::k_oid:
		EmitString(builder, value.get_oid().value.to_string());
		break;
	case type::k_binary: {
		auto binary = value.get_binary();
		EmitString(builder, VariantLogicalType::BLOB, reinterpret_cast<const char *>(binary.bytes), binary.size);
		break;
	}
	case type::k_decimal128: {
		auto str = value.get_decimal128().value.to_string();
		if (!TryEmitDecimal(builder, str)) {
			double parsed = 0.0;
			try {
				parsed = std::stod(str);
			} catch (...) {
				parsed = std::numeric_limits<double>::quiet_NaN();
			}
			EmitFixed(builder, VariantLogicalType::DOUBLE, parsed);
		}
		break;
	}
	case type::k_null:
	case type::k_undefined:
		EmitTag(builder, VariantLogicalType::VARIANT_NULL);
		break;
	case type::k_regex: {
		auto regex = value.get_regex();
		EmitString(builder, "/" + std::string(regex.regex) + "/" + std::string(regex.options));
		break;
	}
	case type::k_code:
		EmitString(builder, value.get_code().code);
		break;
	case type::k_codewscope:
		EmitString(builder, value.get_codewscope().code);
		break;
	case type::k_symbol:
		EmitString(builder, value.get_symbol().symbol);
		break;
	case type::k_timestamp: {
		auto ts = value.get_timestamp();
		EmitString(builder, std::to_string(ts.timestamp) + ":" + std::to_string(ts.increment));
		break;
	}
	case type::k_dbpointer:
		EmitString(builder, std::string("<dbpointer>"));
		break;
	case type::k_minkey:
		EmitString(builder, std::string("MinKey"));
		break;
	case type::k_maxkey:
		EmitString(builder, std::string("MaxKey"));
		break;
	default:
		EmitString(builder, std::string("<unknown type>"));
		break;
	}
}

// BuildVariant source over the buffered wrapper documents.
struct BsonCellSource {
	const vector<std::string> &cells;

	bool Emit(idx_t row, VariantBuilder &builder) const {
		if (row >= cells.size() || cells[row].empty()) {
			return true;
		}
		bsoncxx::document::view wrapper(reinterpret_cast<const uint8_t *>(cells[row].data()), cells[row].size());
		EmitBsonValue((*wrapper.begin()).get_value(), builder);
		return false;
	}
};

} // namespace

void MongoVariantColumnBuffer::Set(idx_t row, const bsoncxx::types::bson_value::view &value) {
	if (cells.size() <= row) {
		cells.resize(row + 1);
	}
	bsoncxx::builder::basic::document wrapper;
	wrapper.append(bsoncxx::builder::basic::kvp("", value));
	auto view = wrapper.view();
	cells[row].assign(reinterpret_cast<const char *>(view.data()), view.length());
}

void MongoVariantColumnBuffer::SetNull(idx_t row) {
	if (cells.size() <= row) {
		cells.resize(row + 1);
	}
	cells[row].clear();
}

void MongoVariantColumnBuffer::Build(idx_t count, Vector &result) {
	if (cells.size() < count) {
		cells.resize(count);
	}
	BsonCellSource source {cells};
	BuildVariant(source, count, result);
	cells.clear();
}

} // namespace duckdb
