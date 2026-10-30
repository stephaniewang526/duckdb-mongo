#include "mongo_variant_pushdown.hpp"

#include "mongo_table_function.hpp"

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/oid.hpp>
#include <bsoncxx/types.hpp>
#include <cctype>
#include <chrono>
#include <cstring>

namespace duckdb {

using bsoncxx::builder::basic::kvp;
using bson_doc = bsoncxx::builder::basic::document;
using bson_array = bsoncxx::builder::basic::array;

bool MongoVariantExtractKeys(const ColumnIndex &index, vector<string> &keys) {
	if (!index.IsPushdownExtract()) {
		return false;
	}
	reference<const ColumnIndex> current(index.GetChildIndex(0));
	while (true) {
		if (current.get().HasPrimaryIndex()) {
			return false;
		}
		keys.push_back(current.get().GetFieldName());
		if (!current.get().HasChildren()) {
			return true;
		}
		current = current.get().GetChildIndex(0);
	}
}

bool MongoIsPlainFieldKey(const std::string &key) {
	return !key.empty() && key[0] != '$' && key.find('.') == string::npos;
}

namespace {

// The field side of a predicate: a dotted path below a VARIANT column, optionally wrapped in a cast.
struct VariantOperand {
	string path;
	// Dotted paths of every proper ancestor of `path`, nearest last. MongoDB walks arrays along a path where DuckDB
	// yields NULL, so null checks must also match documents with an array at any of these.
	vector<string> ancestors;
	// Cast target; INVALID when the VARIANT is used as is.
	LogicalType cast_type = LogicalType::INVALID;
};

bool ResolveVariantPath(const Expression &expr, const LogicalGet &get, const MongoScanData &data, string &path) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		if (col_ref.Binding().table_index != get.table_index) {
			return false;
		}
		auto &col_ids = get.GetColumnIds();
		idx_t position = col_ref.Binding().column_index;
		if (position >= col_ids.size()) {
			return false;
		}
		auto &col_id = col_ids[position];
		idx_t schema_idx = col_id.GetPrimaryIndex();
		if (schema_idx >= data.column_types.size() || data.column_types[schema_idx].id() != LogicalTypeId::VARIANT) {
			return false;
		}
		auto &column_name = data.column_names[schema_idx];
		auto path_it = data.column_name_to_mongo_path.find(column_name);
		string mongo_path = path_it != data.column_name_to_mongo_path.end() ? path_it->second : column_name;
		path = mongo_path == MONGO_ROOT_PATH ? "" : mongo_path;
		vector<string> keys;
		if (col_id.IsPushdownExtract()) {
			if (!MongoVariantExtractKeys(col_id, keys)) {
				return false;
			}
		}
		for (auto &key : keys) {
			if (!MongoIsPlainFieldKey(key)) {
				return false;
			}
			path = path.empty() ? key : path + "." + key;
		}
		return true;
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &func = expr.Cast<BoundFunctionExpression>();
	if (func.Function().GetName() != "variant_extract" || func.GetChildren().empty() || !func.BindInfo()) {
		return false;
	}
	auto &child = *func.GetChildren()[0];
	if (child.GetReturnType().id() != LogicalTypeId::VARIANT) {
		return false;
	}
	auto &component = func.BindInfo()->Cast<VariantExtractBindData>().component;
	if (component.lookup_mode != VariantChildLookupMode::BY_KEY || !MongoIsPlainFieldKey(component.key)) {
		return false;
	}
	string parent;
	if (!ResolveVariantPath(child, get, data, parent)) {
		return false;
	}
	path = parent.empty() ? component.key : parent + "." + component.key;
	return true;
}

struct VariantPredicateTranslator {
	const LogicalGet &get;
	const MongoScanData &data;

	bool ResolveOperand(const Expression &expr, VariantOperand &operand) const {
		const Expression *field = &expr;
		if (BoundCastExpression::IsCast(expr)) {
			auto &cast = expr.Cast<BoundFunctionExpression>();
			field = &BoundCastExpression::Child(cast);
			if (BoundCastExpression::IsCast(*field)) {
				return false;
			}
			operand.cast_type = cast.GetReturnType();
		}
		if (!ResolveVariantPath(*field, get, data, operand.path) || operand.path.empty()) {
			return false;
		}
		auto &col_ids = get.GetColumnIds();
		if (field->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			// After projection pushdown the cast may already live in the column (ColumnIndex::GetScanType)
			auto &binding = field->Cast<BoundColumnRefExpression>().Binding();
			auto &col_id = col_ids[binding.column_index];
			if (col_id.IsPushdownExtract() && col_id.GetScanType().id() != LogicalTypeId::VARIANT) {
				if (operand.cast_type.id() != LogicalTypeId::INVALID) {
					return false;
				}
				operand.cast_type = col_id.GetScanType();
			}
		}
		size_t pos = 0;
		while ((pos = operand.path.find('.', pos)) != string::npos) {
			operand.ancestors.push_back(operand.path.substr(0, pos));
			pos++;
		}
		return true;
	}

	static bsoncxx::document::value Term(const string &path, bson_doc &op_doc) {
		bson_doc doc;
		doc.append(kvp(path, op_doc.extract()));
		return doc.extract();
	}

	static bsoncxx::document::value Term(const string &path, const bsoncxx::types::bson_value::view &value) {
		bson_doc doc;
		doc.append(kvp(path, value));
		return doc.extract();
	}

	static bsoncxx::document::value Or(vector<bsoncxx::document::value> terms) {
		// IN lists repeat the escape term for every value
		vector<bsoncxx::document::value> unique;
		for (auto &term : terms) {
			bool seen = false;
			for (auto &existing : unique) {
				auto a = existing.view();
				auto b = term.view();
				seen |= a.length() == b.length() && memcmp(a.data(), b.data(), a.length()) == 0;
			}
			if (!seen) {
				unique.push_back(std::move(term));
			}
		}
		if (unique.size() == 1) {
			return std::move(unique[0]);
		}
		bson_array array;
		for (auto &term : unique) {
			array.append(term.view());
		}
		bson_doc doc;
		doc.append(kvp("$or", array.extract()));
		return doc.extract();
	}

	// Present, non-null values outside the BSON types the cast compares exactly; DuckDB may still convert them.
	static bsoncxx::document::value Escape(const string &path, const vector<string> &bracket) {
		bson_array types;
		for (auto &type : bracket) {
			types.append(type);
		}
		bson_doc not_doc;
		not_doc.append(kvp("$type", types.extract()));
		bson_doc op_doc;
		op_doc.append(kvp("$not", not_doc.extract()));
		op_doc.append(kvp("$ne", bsoncxx::types::b_null {}));
		return Term(path, op_doc);
	}

	static bsoncxx::document::value TypeTerm(const string &path, const vector<string> &types) {
		bson_doc op_doc;
		if (types.size() == 1) {
			op_doc.append(kvp("$type", types[0]));
		} else {
			bson_array array;
			for (auto &type : types) {
				array.append(type);
			}
			op_doc.append(kvp("$type", array.extract()));
		}
		return Term(path, op_doc);
	}

	static bool IsIntegerTarget(const LogicalType &type) {
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
			return true;
		default:
			return false;
		}
	}

	static const vector<string> &Bracket(const LogicalType &type) {
		static const vector<string> numeric {"int", "long", "double", "decimal"};
		static const vector<string> floating {"int", "long", "double"};
		static const vector<string> string_types {"string"};
		static const vector<string> boolean {"bool"};
		static const vector<string> date {"date"};
		static const vector<string> none;
		if (IsIntegerTarget(type)) {
			return numeric;
		}
		switch (type.id()) {
		case LogicalTypeId::DOUBLE:
			return floating;
		case LogicalTypeId::VARCHAR:
			return string_types;
		case LogicalTypeId::BOOLEAN:
			return boolean;
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIMESTAMP:
			return date;
		default:
			return none;
		}
	}

	static bsoncxx::types::b_date Millis(int64_t ms) {
		return bsoncxx::types::b_date {std::chrono::milliseconds(ms)};
	}

	static bool IsObjectIdHex(const string &str) {
		if (str.size() != 24) {
			return false;
		}
		for (char c : str) {
			if (!std::isxdigit(static_cast<unsigned char>(c))) {
				return false;
			}
		}
		return true;
	}

	// `path op constant` where the path is cast to the constant's type. Each case widens enough to cover DuckDB's
	// conversion of the BSON types in the bracket; everything else goes through Escape.
	bool Comparison(const VariantOperand &operand, ExpressionType op, const Value &constant,
	                vector<bsoncxx::document::value> &terms) const {
		auto &type = operand.cast_type;
		if (type.id() == LogicalTypeId::INVALID || constant.IsNull() || constant.type() != type) {
			return false;
		}
		auto &bracket = Bracket(type);
		if (bracket.empty()) {
			return false;
		}
		bson_doc op_doc;
		if (IsIntegerTarget(type)) {
			auto as_bigint = constant.DefaultTryCastAs(LogicalType::BIGINT);
			if (!as_bigint) {
				return false;
			}
			int64_t c = BigIntValue::Get(*as_bigint);
			if (c == NumericLimits<int64_t>::Minimum() || c == NumericLimits<int64_t>::Maximum()) {
				return false;
			}
			// Doubles and decimals round to the target: 4.5 and 5.4 both become 5
			switch (op) {
			case ExpressionType::COMPARE_EQUAL:
				op_doc.append(kvp("$gt", c - 1));
				op_doc.append(kvp("$lt", c + 1));
				break;
			case ExpressionType::COMPARE_LESSTHAN:
				op_doc.append(kvp("$lt", c));
				break;
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
				op_doc.append(kvp("$lt", c + 1));
				break;
			case ExpressionType::COMPARE_GREATERTHAN:
				op_doc.append(kvp("$gt", c));
				break;
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				op_doc.append(kvp("$gt", c - 1));
				break;
			case ExpressionType::COMPARE_NOTEQUAL:
				return NotEqual(operand.path, bsoncxx::types::bson_value::view(bsoncxx::types::b_int64 {c}), terms);
			default:
				return false;
			}
		} else if (type.id() == LogicalTypeId::DOUBLE) {
			double c = constant.GetValue<double>();
			if (Value::IsNan(c)) {
				return false;
			}
			if (op == ExpressionType::COMPARE_NOTEQUAL) {
				return NotEqual(operand.path, bsoncxx::types::bson_value::view(bsoncxx::types::b_double {c}), terms);
			}
			auto mongo_op = SimpleOperator(op);
			if (!mongo_op) {
				return false;
			}
			op_doc.append(kvp(string(mongo_op), c));
		} else if (type.id() == LogicalTypeId::VARCHAR) {
			string c = StringValue::Get(constant);
			if (op == ExpressionType::COMPARE_NOTEQUAL) {
				return NotEqual(operand.path, bsoncxx::types::bson_value::view(bsoncxx::types::b_string {c}), terms);
			}
			auto mongo_op = SimpleOperator(op);
			if (!mongo_op) {
				return false;
			}
			op_doc.append(kvp(string(mongo_op), c));
			if (op == ExpressionType::COMPARE_EQUAL) {
				// ObjectIds read as their hex text; a 24-hex constant may name one
				terms.push_back(Term(operand.path, op_doc));
				if (IsObjectIdHex(c)) {
					bson_doc oid_doc;
					oid_doc.append(kvp("$eq", bsoncxx::oid(c)));
					terms.push_back(Term(operand.path, oid_doc));
				}
				terms.push_back(Escape(operand.path, {"string", "objectId"}));
				return true;
			}
		} else if (type.id() == LogicalTypeId::BOOLEAN) {
			bool c = BooleanValue::Get(constant);
			if (op == ExpressionType::COMPARE_EQUAL) {
				op_doc.append(kvp("$eq", c));
			} else if (op == ExpressionType::COMPARE_NOTEQUAL) {
				return NotEqual(operand.path, bsoncxx::types::bson_value::view(bsoncxx::types::b_bool {c}), terms);
			} else {
				return false;
			}
		} else if (type.id() == LogicalTypeId::DATE) {
			date_t c = DateValue::Get(constant);
			if (!c.IsFinite()) {
				return false;
			}
			// A BSON date casts to the DATE of its day, so a day is the unit of comparison
			int64_t day_start = Date::Epoch(c) * 1000;
			int64_t next_day = Date::Epoch(date_t(c.days + 1)) * 1000;
			switch (op) {
			case ExpressionType::COMPARE_EQUAL:
				op_doc.append(kvp("$gte", Millis(day_start)));
				op_doc.append(kvp("$lt", Millis(next_day)));
				break;
			case ExpressionType::COMPARE_LESSTHAN:
				op_doc.append(kvp("$lt", Millis(day_start)));
				break;
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
				op_doc.append(kvp("$lt", Millis(next_day)));
				break;
			case ExpressionType::COMPARE_GREATERTHAN:
				op_doc.append(kvp("$gte", Millis(next_day)));
				break;
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				op_doc.append(kvp("$gte", Millis(day_start)));
				break;
			case ExpressionType::COMPARE_NOTEQUAL:
				return NotEqual(operand.path, bsoncxx::types::bson_value::view(Millis(day_start)), terms);
			default:
				return false;
			}
		} else if (type.id() == LogicalTypeId::TIMESTAMP) {
			timestamp_t c = TimestampValue::Get(constant);
			if (!c.IsFinite()) {
				return false;
			}
			// BSON dates have millisecond precision; the constant has microseconds
			int64_t floor_ms = c.value / 1000 - (c.value % 1000 < 0 ? 1 : 0);
			int64_t ceil_ms = floor_ms + (c.value % 1000 != 0 ? 1 : 0);
			switch (op) {
			case ExpressionType::COMPARE_EQUAL:
				op_doc.append(kvp("$gte", Millis(floor_ms)));
				op_doc.append(kvp("$lte", Millis(ceil_ms)));
				break;
			case ExpressionType::COMPARE_LESSTHAN:
				op_doc.append(kvp("$lt", Millis(ceil_ms)));
				break;
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
				op_doc.append(kvp("$lte", Millis(floor_ms)));
				break;
			case ExpressionType::COMPARE_GREATERTHAN:
				op_doc.append(kvp("$gt", Millis(floor_ms)));
				break;
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				op_doc.append(kvp("$gte", Millis(ceil_ms)));
				break;
			case ExpressionType::COMPARE_NOTEQUAL:
				if (floor_ms != ceil_ms) {
					return NotNull(operand.path, terms);
				}
				return NotEqual(operand.path, bsoncxx::types::bson_value::view(Millis(floor_ms)), terms);
			default:
				return false;
			}
		} else {
			return false;
		}
		terms.push_back(Term(operand.path, op_doc));
		terms.push_back(Escape(operand.path, bracket));
		return true;
	}

	static const char *SimpleOperator(ExpressionType op) {
		switch (op) {
		case ExpressionType::COMPARE_EQUAL:
			return "$eq";
		case ExpressionType::COMPARE_LESSTHAN:
			return "$lt";
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			return "$lte";
		case ExpressionType::COMPARE_GREATERTHAN:
			return "$gt";
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return "$gte";
		default:
			return nullptr;
		}
	}

	// BSON equality is exact, so only the constant itself (and null, which SQL excludes) can fail `<>`.
	static bool NotEqual(const string &path, const bsoncxx::types::bson_value::view &value,
	                     vector<bsoncxx::document::value> &terms) {
		bson_array excluded;
		excluded.append(value);
		excluded.append(bsoncxx::types::b_null {});
		bson_doc op_doc;
		op_doc.append(kvp("$nin", excluded.extract()));
		terms.push_back(Term(path, op_doc));
		return true;
	}

	// `$ne: null` skips arrays holding a null element, which DuckDB sees as a non-null array.
	static bool NotNull(const string &path, vector<bsoncxx::document::value> &terms) {
		bson_doc op_doc;
		op_doc.append(kvp("$ne", bsoncxx::types::b_null {}));
		terms.push_back(Term(path, op_doc));
		terms.push_back(TypeTerm(path, {"array"}));
		return true;
	}

	// Missing, null, or unreachable because an ancestor is not a document.
	static bool IsNull(const VariantOperand &operand, vector<bsoncxx::document::value> &terms) {
		terms.push_back(Term(operand.path, bsoncxx::types::bson_value::view(bsoncxx::types::b_null {})));
		for (auto &ancestor : operand.ancestors) {
			terms.push_back(TypeTerm(ancestor, {"array"}));
		}
		if (operand.cast_type.id() != LogicalTypeId::INVALID) {
			// A failed or lossy cast also yields NULL
			auto &bracket = Bracket(operand.cast_type);
			if (bracket.empty()) {
				return false;
			}
			terms.push_back(Escape(operand.path, bracket));
		}
		return true;
	}

	bool TypeOf(const VariantOperand &operand, const string &type_name, vector<bsoncxx::document::value> &terms) const {
		if (operand.cast_type.id() != LogicalTypeId::INVALID) {
			return false;
		}
		auto &path = operand.path;
		if (type_name == "VARIANT_NULL") {
			return IsNull(operand, terms);
		}
		if (type_name == "BOOL_TRUE" || type_name == "BOOL_FALSE") {
			terms.push_back(
			    Term(path, bsoncxx::types::bson_value::view(bsoncxx::types::b_bool {type_name == "BOOL_TRUE"})));
			return true;
		}
		vector<string> types;
		if (type_name == "INT32") {
			types = {"int"};
		} else if (type_name == "INT64") {
			types = {"long"};
		} else if (type_name == "DOUBLE") {
			// Decimal128 values the VARIANT cannot hold are read as DOUBLE
			types = {"double", "decimal"};
		} else if (StringUtil::StartsWith(type_name, "DECIMAL(")) {
			types = {"decimal"};
		} else if (type_name == "VARCHAR") {
			types = {"string", "objectId",  "regex",     "javascript", "javascriptWithScope",
			         "symbol", "timestamp", "dbPointer", "minKey",     "maxKey"};
		} else if (type_name == "TIMESTAMP_MILIS") {
			types = {"date"};
		} else if (type_name == "BLOB") {
			types = {"binData"};
		} else if (StringUtil::StartsWith(type_name, "OBJECT(")) {
			types = {"object"};
		} else if (StringUtil::StartsWith(type_name, "ARRAY(")) {
			types = {"array"};
		} else {
			return false;
		}
		terms.push_back(TypeTerm(path, types));
		return true;
	}

	bool TranslateComparison(const Expression &expr, vector<bsoncxx::document::value> &terms) const {
		auto &cmp = expr.Cast<BoundFunctionExpression>();
		auto &left = BoundComparisonExpression::Left(cmp);
		auto &right = BoundComparisonExpression::Right(cmp);
		auto op = expr.GetExpressionType();
		const Expression *field = &left;
		const Expression *constant = &right;
		if (right.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			if (left.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
				return false;
			}
			field = &right;
			constant = &left;
			op = FlipComparisonExpression(op);
		}
		auto &value = constant->Cast<BoundConstantExpression>().GetValue();
		// variant_typeof(path) = 'TYPE'
		if (field->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		    field->Cast<BoundFunctionExpression>().Function().GetName() == "variant_typeof") {
			auto &children = field->Cast<BoundFunctionExpression>().GetChildren();
			VariantOperand operand;
			if (op != ExpressionType::COMPARE_EQUAL || children.size() != 1 || value.IsNull() ||
			    value.type().id() != LogicalTypeId::VARCHAR || !ResolveOperand(*children[0], operand)) {
				return false;
			}
			return TypeOf(operand, StringValue::Get(value), terms);
		}
		VariantOperand operand;
		if (!ResolveOperand(*field, operand)) {
			return false;
		}
		return Comparison(operand, op, value, terms);
	}

	bool TranslateIn(const Expression &expr, vector<bsoncxx::document::value> &terms) const {
		auto &children = expr.Cast<BoundOperatorExpression>().GetChildren();
		VariantOperand operand;
		if (children.size() < 2 || !ResolveOperand(*children[0], operand)) {
			return false;
		}
		for (idx_t i = 1; i < children.size(); i++) {
			if (children[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
				return false;
			}
			auto &value = children[i]->Cast<BoundConstantExpression>().GetValue();
			if (!Comparison(operand, ExpressionType::COMPARE_EQUAL, value, terms)) {
				return false;
			}
		}
		return true;
	}

	bool TranslateBetween(const Expression &expr, bsoncxx::document::value &result) const {
		auto &between = expr.Cast<BoundFunctionExpression>();
		if (!BoundBetweenExpression::HasValidBindData(between)) {
			return false;
		}
		auto &lower = BoundBetweenExpression::LowerBound(between);
		auto &upper = BoundBetweenExpression::UpperBound(between);
		VariantOperand operand;
		if (lower.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT ||
		    upper.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT ||
		    !ResolveOperand(BoundBetweenExpression::Input(between), operand)) {
			return false;
		}
		vector<bsoncxx::document::value> lower_terms;
		vector<bsoncxx::document::value> upper_terms;
		if (!Comparison(operand, BoundBetweenExpression::LowerComparisonType(between),
		                lower.Cast<BoundConstantExpression>().GetValue(), lower_terms) ||
		    !Comparison(operand, BoundBetweenExpression::UpperComparisonType(between),
		                upper.Cast<BoundConstantExpression>().GetValue(), upper_terms)) {
			return false;
		}
		bson_array both;
		both.append(Or(std::move(lower_terms)).view());
		both.append(Or(std::move(upper_terms)).view());
		bson_doc doc;
		doc.append(kvp("$and", both.extract()));
		result = doc.extract();
		return true;
	}

	bool TranslateConjunction(const Expression &expr, bsoncxx::document::value &result) const {
		bool is_and = expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND;
		bson_array array;
		idx_t translated = 0;
		for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			bsoncxx::document::value term = bson_doc {}.extract();
			if (!Translate(*child, term)) {
				// Dropping an AND term only widens the match; dropping an OR term would lose rows
				if (is_and) {
					continue;
				}
				return false;
			}
			array.append(term.view());
			translated++;
		}
		if (translated == 0) {
			return false;
		}
		bson_doc doc;
		doc.append(kvp(string(is_and ? "$and" : "$or"), array.extract()));
		result = doc.extract();
		return true;
	}

	bool Translate(const Expression &expr, bsoncxx::document::value &result) const {
		auto optional_child = ExpressionFilter::GetOptionalFilterChild(expr);
		if (optional_child) {
			return Translate(*optional_child, result);
		}
		vector<bsoncxx::document::value> terms;
		if (BoundComparisonExpression::IsComparison(expr)) {
			if (!TranslateComparison(expr, terms)) {
				return false;
			}
			result = Or(std::move(terms));
			return true;
		}
		switch (expr.GetExpressionType()) {
		case ExpressionType::COMPARE_BETWEEN:
			return TranslateBetween(expr, result);
		case ExpressionType::COMPARE_IN:
			if (!TranslateIn(expr, terms)) {
				return false;
			}
			result = Or(std::move(terms));
			return true;
		case ExpressionType::CONJUNCTION_AND:
		case ExpressionType::CONJUNCTION_OR:
			return TranslateConjunction(expr, result);
		case ExpressionType::OPERATOR_IS_NULL:
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			auto &children = expr.Cast<BoundOperatorExpression>().GetChildren();
			VariantOperand operand;
			if (children.size() != 1 || !ResolveOperand(*children[0], operand)) {
				return false;
			}
			bool is_null = expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL;
			if (!(is_null ? IsNull(operand, terms) : NotNull(operand.path, terms))) {
				return false;
			}
			result = Or(std::move(terms));
			return true;
		}
		default:
			return false;
		}
	}
};

} // namespace

bool TryTranslateVariantPredicate(const Expression &expr, const LogicalGet &get, const MongoScanData &data,
                                  bsoncxx::document::value &result) {
	VariantPredicateTranslator translator {get, data};
	return translator.Translate(expr, result);
}

} // namespace duckdb
