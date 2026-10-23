#include "mongo_filter_pushdown.hpp"
#include "mongo_duckdb_utils.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/oid.hpp>
#include <bsoncxx/types/bson_value/value.hpp>

namespace duckdb {
namespace {

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

static bool IsActualObjectIdColumn(const string &column_name, const unordered_set<string> &objectid_columns) {
	return objectid_columns.count(column_name) > 0;
}

static void AppendValueToDocument(bsoncxx::builder::basic::document &doc_builder, const string &key, const Value &value,
                                  const LogicalType &type, const string &column_name,
                                  const unordered_set<string> &objectid_columns) {
	if (value.IsNull()) {
		doc_builder.append(bsoncxx::builder::basic::kvp(key, bsoncxx::types::b_null {}));
		return;
	}

	// Use column_name for ObjectID detection if provided, otherwise use key
	const string &col_for_oid_check = column_name.empty() ? key : column_name;

	switch (type.id()) {
	case LogicalTypeId::VARCHAR: {
		auto str_val = value.GetValue<string>();
		if (IsActualObjectIdColumn(col_for_oid_check, objectid_columns) && IsValidObjectIdHex(str_val)) {
			doc_builder.append(bsoncxx::builder::basic::kvp(key, bsoncxx::oid(str_val)));
		} else {
			doc_builder.append(bsoncxx::builder::basic::kvp(key, str_val));
		}
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

// Every column term is {column: {$op: value}} so that terms on the same column can sit side by side in an $and.
// Returns an empty document when the expression can't be translated.
static bsoncxx::document::value ConvertExpressionToMongo(const Expression &expr, const string &column_name,
                                                         const LogicalType &column_type,
                                                         const unordered_set<string> &objectid_columns) {
	bsoncxx::builder::basic::document doc;

	// Optional filters (non-dense IN lists, OR chains, relaxed temporal casts) are also kept in DuckDB, so their
	// child is pushed as-is. Dynamic, bloom and prefix children don't translate and are left out.
	auto optional_child = ExpressionFilter::GetOptionalFilterChild(expr);
	if (optional_child) {
		return ConvertExpressionToMongo(*optional_child, column_name, column_type, objectid_columns);
	}

	if (BoundComparisonExpression::IsComparison(expr)) {
		// Only `column op constant`: DuckDB 2.0 also pushes `struct_extract(column, ...) op constant` on the parent
		// column, which must not be matched against the whole column.
		ExpressionType cmp_type;
		auto constant =
		    ExpressionFilter::TryGetColumnConstantComparison(expr.Cast<BoundFunctionExpression>(), cmp_type);
		auto mongo_op = MongoComparisonOperator(cmp_type);
		if (constant && mongo_op) {
			bsoncxx::builder::basic::document op_doc;
			AppendValueToDocument(op_doc, mongo_op, constant->GetValue(), column_type, column_name, objectid_columns);
			doc.append(bsoncxx::builder::basic::kvp(column_name, op_doc.extract()));
		}
		return doc.extract();
	}

	if (expr.GetExpressionType() == ExpressionType::COMPARE_IN) {
		auto &children = expr.Cast<BoundOperatorExpression>().GetChildren();
		if (children.empty() || !ExpressionFilter::IsSimpleFilterColumnRef(*children[0])) {
			return doc.extract();
		}
		bsoncxx::builder::basic::array values;
		for (idx_t i = 1; i < children.size(); i++) {
			if (children[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
				return doc.extract();
			}
			bsoncxx::builder::basic::document value_doc;
			AppendValueToDocument(value_doc, "v", children[i]->Cast<BoundConstantExpression>().GetValue(), column_type,
			                      column_name, objectid_columns);
			values.append(bsoncxx::types::bson_value::value(value_doc.view()["v"].get_value()));
		}
		bsoncxx::builder::basic::document in_doc;
		in_doc.append(bsoncxx::builder::basic::kvp("$in", values.extract()));
		doc.append(bsoncxx::builder::basic::kvp(column_name, in_doc.extract()));
		return doc.extract();
	}

	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		bool is_and = expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND;
		bsoncxx::builder::basic::array terms;
		for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			auto child_doc = ConvertExpressionToMongo(*child, column_name, column_type, objectid_columns);
			if (child_doc.view().empty()) {
				// Leaving out an AND term widens the match; leaving out an OR term would lose rows.
				if (is_and) {
					continue;
				}
				return doc.extract();
			}
			terms.append(child_doc.view());
		}
		if (!terms.view().empty()) {
			doc.append(bsoncxx::builder::basic::kvp(string(is_and ? "$and" : "$or"), terms.extract()));
		}
		return doc.extract();
	}

	if (expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL ||
	    expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) {
		bsoncxx::builder::basic::document op_doc;
		op_doc.append(bsoncxx::builder::basic::kvp(
		    string(expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL ? "$eq" : "$ne"),
		    bsoncxx::types::b_null {}));
		doc.append(bsoncxx::builder::basic::kvp(column_name, op_doc.extract()));
	}
	return doc.extract();
}

} // namespace

bsoncxx::document::value ConvertFiltersToMongoQuery(optional_ptr<TableFilterSet> filters,
                                                    const std::vector<string> &column_names,
                                                    const std::vector<LogicalType> &column_types,
                                                    const std::unordered_map<string, string> &column_name_to_mongo_path,
                                                    const std::unordered_set<string> &objectid_columns) {
	if (!filters || !MongoHasFilters(*filters)) {
		return bsoncxx::builder::basic::document {}.extract();
	}

	vector<bsoncxx::document::value> conjuncts;
	MongoForEachFilter(*filters, [&](idx_t col_idx, TableFilter &filter) {
		if (col_idx >= column_names.size() || filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
			return;
		}
		auto &expr_filter = filter.Cast<ExpressionFilter>();
		if (!expr_filter.expr) {
			return;
		}

		const string &column_name = column_names[col_idx];
		auto path_it = column_name_to_mongo_path.find(column_name);
		const string &mongo_column_name = path_it != column_name_to_mongo_path.end() ? path_it->second : column_name;

		auto filter_doc =
		    ConvertExpressionToMongo(*expr_filter.expr, mongo_column_name, column_types[col_idx], objectid_columns);
		if (!filter_doc.view().empty()) {
			conjuncts.push_back(std::move(filter_doc));
		}
	});

	if (conjuncts.empty()) {
		return bsoncxx::builder::basic::document {}.extract();
	}
	if (conjuncts.size() == 1) {
		return std::move(conjuncts[0]);
	}

	bsoncxx::builder::basic::array and_terms;
	for (auto &term : conjuncts) {
		and_terms.append(term.view());
	}
	bsoncxx::builder::basic::document and_query;
	and_query.append(bsoncxx::builder::basic::kvp("$and", and_terms.extract()));
	return and_query.extract();
}

} // namespace duckdb
