#include "mongo_filter_pushdown.hpp"
#include "mongo_duckdb_utils.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/decimal128.hpp>
#include <bsoncxx/oid.hpp>
#include <bsoncxx/types/bson_value/value.hpp>

namespace duckdb {

bool MongoCanPushdownFilterType(const LogicalType &type) {
	// JSON columns hold documents, so a string constant never compares equal to them server-side.
	if (type.IsJSONType()) {
		return false;
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
		return true;
	case LogicalTypeId::DECIMAL:
		// Decimal128 carries 34 significant digits.
		return DecimalType::GetWidth(type) <= 34;
	default:
		return false;
	}
}

namespace {

using bsoncxx::builder::basic::kvp;
using bson_value_t = bsoncxx::types::bson_value::value;

static bool IsValidObjectIdHex(const string &str) {
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

// Encodes a constant by its own type, which the binder already aligned with the field it is compared against.
// NULL constants are refused: SQL comparisons with NULL never match, while MongoDB's null matches missing fields.
static bool TryToBsonValue(const Value &value, bool objectid_field, unique_ptr<bson_value_t> &result) {
	if (value.IsNull() || !MongoCanPushdownFilterType(value.type())) {
		return false;
	}
	switch (value.type().id()) {
	case LogicalTypeId::VARCHAR: {
		auto str = value.GetValue<string>();
		if (objectid_field && IsValidObjectIdHex(str)) {
			result = make_uniq<bson_value_t>(bsoncxx::oid(str));
		} else {
			result = make_uniq<bson_value_t>(std::move(str));
		}
		return true;
	}
	case LogicalTypeId::BOOLEAN:
		result = make_uniq<bson_value_t>(value.GetValue<bool>());
		return true;
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
		result = make_uniq<bson_value_t>(value.GetValue<int32_t>());
		return true;
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UINTEGER:
		result = make_uniq<bson_value_t>(value.GetValue<int64_t>());
		return true;
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		result = make_uniq<bson_value_t>(value.GetValue<double>());
		return true;
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::DECIMAL:
		// MongoDB compares decimal128 numerically against every numeric BSON type.
		result = make_uniq<bson_value_t>(bsoncxx::decimal128(value.ToString()));
		return true;
	case LogicalTypeId::DATE: {
		auto ts = Timestamp::FromDatetime(value.GetValue<date_t>(), dtime_t(0));
		result = make_uniq<bson_value_t>(std::chrono::milliseconds(Timestamp::GetEpochMs(ts)));
		return true;
	}
	case LogicalTypeId::TIMESTAMP:
		result =
		    make_uniq<bson_value_t>(std::chrono::milliseconds(Timestamp::GetEpochMs(value.GetValue<timestamp_t>())));
		return true;
	default:
		return false;
	}
}

static const char *MongoComparisonOperator(ExpressionType cmp_type) {
	switch (cmp_type) {
	case ExpressionType::COMPARE_EQUAL:
		return "$eq";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "$ne";
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

static bsoncxx::document::value EmptyDocument() {
	return bsoncxx::builder::basic::document {}.extract();
}

static MongoPredicateTranslation Untranslated() {
	return {EmptyDocument(), false};
}

static MongoPredicateTranslation FieldTerm(const string &path, bsoncxx::builder::basic::document &op_doc) {
	bsoncxx::builder::basic::document doc;
	doc.append(kvp(path, op_doc.extract()));
	return {doc.extract(), true};
}

struct PredicateTranslator {
	const MongoFieldPathResolver &resolve;
	const unordered_set<string> &objectid_columns;

	bool ResolvePath(const Expression &expr, string &path) const {
		return resolve(expr, path);
	}

	bool EncodeConstant(const Expression &expr, const string &path, unique_ptr<bson_value_t> &out) const {
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			return false;
		}
		return TryToBsonValue(expr.Cast<BoundConstantExpression>().GetValue(), objectid_columns.count(path) > 0, out);
	}

	MongoPredicateTranslation Comparison(const Expression &expr) const {
		auto &cmp = expr.Cast<BoundFunctionExpression>();
		auto &left = BoundComparisonExpression::Left(cmp);
		auto &right = BoundComparisonExpression::Right(cmp);
		auto cmp_type = expr.GetExpressionType();
		string path;
		const Expression *constant = nullptr;
		if (ResolvePath(left, path)) {
			constant = &right;
		} else if (ResolvePath(right, path)) {
			constant = &left;
			cmp_type = FlipComparisonExpression(cmp_type);
		} else {
			return Untranslated();
		}
		auto mongo_op = MongoComparisonOperator(cmp_type);
		unique_ptr<bson_value_t> value;
		if (!mongo_op || !EncodeConstant(*constant, path, value)) {
			return Untranslated();
		}
		bsoncxx::builder::basic::document op_doc;
		if (cmp_type == ExpressionType::COMPARE_NOTEQUAL) {
			// $ne alone also matches null and missing fields, which SQL's <> must not return.
			bsoncxx::builder::basic::array excluded;
			excluded.append(value->view());
			excluded.append(bsoncxx::types::b_null {});
			op_doc.append(kvp("$nin", excluded.extract()));
		} else {
			op_doc.append(kvp(string(mongo_op), value->view()));
		}
		return FieldTerm(path, op_doc);
	}

	// BETWEEN reaches pushdown_complex_filter before the filter combiner splits it into two comparisons.
	MongoPredicateTranslation Between(const Expression &expr) const {
		auto &between = expr.Cast<BoundFunctionExpression>();
		string path;
		unique_ptr<bson_value_t> lower;
		unique_ptr<bson_value_t> upper;
		if (!BoundBetweenExpression::HasValidBindData(between) ||
		    !ResolvePath(BoundBetweenExpression::Input(between), path) ||
		    !EncodeConstant(BoundBetweenExpression::LowerBound(between), path, lower) ||
		    !EncodeConstant(BoundBetweenExpression::UpperBound(between), path, upper)) {
			return Untranslated();
		}
		bsoncxx::builder::basic::document op_doc;
		op_doc.append(
		    kvp(string(MongoComparisonOperator(BoundBetweenExpression::LowerComparisonType(between))), lower->view()));
		op_doc.append(
		    kvp(string(MongoComparisonOperator(BoundBetweenExpression::UpperComparisonType(between))), upper->view()));
		return FieldTerm(path, op_doc);
	}

	MongoPredicateTranslation InList(const Expression &expr) const {
		auto &children = expr.Cast<BoundOperatorExpression>().GetChildren();
		string path;
		if (children.empty() || !ResolvePath(*children[0], path)) {
			return Untranslated();
		}
		bsoncxx::builder::basic::array values;
		for (idx_t i = 1; i < children.size(); i++) {
			unique_ptr<bson_value_t> value;
			if (!EncodeConstant(*children[i], path, value)) {
				return Untranslated();
			}
			values.append(value->view());
		}
		bsoncxx::builder::basic::document op_doc;
		op_doc.append(kvp("$in", values.extract()));
		return FieldTerm(path, op_doc);
	}

	MongoPredicateTranslation Conjunction(const Expression &expr) const {
		bool is_and = expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND;
		bool complete = true;
		bsoncxx::builder::basic::array terms;
		for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			auto term = Translate(*child);
			complete &= term.complete;
			if (term.doc.view().empty()) {
				// Dropping an AND term only widens the match; dropping an OR term would lose rows.
				if (is_and) {
					continue;
				}
				return Untranslated();
			}
			terms.append(term.doc.view());
		}
		if (terms.view().empty()) {
			return {EmptyDocument(), complete};
		}
		bsoncxx::builder::basic::document doc;
		doc.append(kvp(string(is_and ? "$and" : "$or"), terms.extract()));
		return {doc.extract(), complete};
	}

	MongoPredicateTranslation NullCheck(const Expression &expr) const {
		auto &children = expr.Cast<BoundOperatorExpression>().GetChildren();
		string path;
		if (children.size() != 1 || !ResolvePath(*children[0], path)) {
			return Untranslated();
		}
		bool is_null = expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL;
		bsoncxx::builder::basic::document op_doc;
		op_doc.append(kvp(string(is_null ? "$eq" : "$ne"), bsoncxx::types::b_null {}));
		return FieldTerm(path, op_doc);
	}

	MongoPredicateTranslation Translate(const Expression &expr) const {
		// DuckDB keeps optional filters (non-dense IN lists, OR chains, relaxed temporal casts, dynamic and bloom
		// filters) in the plan, so their child is pushed as a hint and never makes the translation incomplete.
		auto optional_child = ExpressionFilter::GetOptionalFilterChild(expr);
		if (optional_child) {
			auto result = Translate(*optional_child);
			return {std::move(result.doc), true};
		}
		if (BoundComparisonExpression::IsComparison(expr)) {
			return Comparison(expr);
		}
		switch (expr.GetExpressionType()) {
		case ExpressionType::COMPARE_BETWEEN:
			return Between(expr);
		case ExpressionType::COMPARE_IN:
			return InList(expr);
		case ExpressionType::CONJUNCTION_AND:
		case ExpressionType::CONJUNCTION_OR:
			return Conjunction(expr);
		case ExpressionType::OPERATOR_IS_NULL:
		case ExpressionType::OPERATOR_IS_NOT_NULL:
			return NullCheck(expr);
		default:
			return Untranslated();
		}
	}
};

} // namespace

MongoPredicateTranslation TranslatePredicateToMongo(const Expression &expr, const MongoFieldPathResolver &resolve,
                                                    const unordered_set<string> &objectid_columns) {
	PredicateTranslator translator {resolve, objectid_columns};
	return translator.Translate(expr);
}

bsoncxx::document::value MongoAndDocuments(std::vector<bsoncxx::document::value> conjuncts) {
	vector<bsoncxx::document::value> non_empty;
	for (auto &doc : conjuncts) {
		if (!doc.view().empty()) {
			non_empty.push_back(std::move(doc));
		}
	}
	if (non_empty.empty()) {
		return EmptyDocument();
	}
	if (non_empty.size() == 1) {
		return std::move(non_empty[0]);
	}
	bsoncxx::builder::basic::array and_terms;
	for (auto &doc : non_empty) {
		and_terms.append(doc.view());
	}
	bsoncxx::builder::basic::document result;
	result.append(kvp("$and", and_terms.extract()));
	return result.extract();
}

bsoncxx::document::value ConvertFiltersToMongoQuery(optional_ptr<TableFilterSet> filters,
                                                    const std::vector<string> &column_names,
                                                    const std::vector<LogicalType> &column_types,
                                                    const std::unordered_map<string, string> &column_name_to_mongo_path,
                                                    const std::unordered_set<string> &objectid_columns) {
	if (!filters || !MongoHasFilters(*filters)) {
		return EmptyDocument();
	}

	vector<bsoncxx::document::value> conjuncts;
	MongoForEachFilter(*filters, [&](idx_t col_idx, TableFilter &filter) {
		if (col_idx >= column_names.size()) {
			return;
		}
		auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "ConvertFiltersToMongoQuery");
		if (!expr_filter.expr) {
			return;
		}

		const string &column_name = column_names[col_idx];
		auto path_it = column_name_to_mongo_path.find(column_name);
		const string &mongo_path = path_it != column_name_to_mongo_path.end() ? path_it->second : column_name;

		// Table filters reference their column as BoundReferenceExpression #0.
		MongoFieldPathResolver resolve = [&](const Expression &expr, string &path) {
			if (!ExpressionFilter::IsSimpleFilterColumnRef(expr)) {
				return false;
			}
			path = mongo_path;
			return true;
		};
		auto translation = TranslatePredicateToMongo(*expr_filter.expr, resolve, objectid_columns);
		if (!translation.complete) {
			// DuckDB already removed the filter from the plan; silently widening the scan would return wrong rows.
			throw NotImplementedException("mongo_scan: filter \"%s\" on column \"%s\" cannot be translated to a "
			                              "MongoDB query",
			                              expr_filter.expr->ToString(), column_name);
		}
		conjuncts.push_back(std::move(translation.doc));
	});
	return MongoAndDocuments(std::move(conjuncts));
}

} // namespace duckdb
