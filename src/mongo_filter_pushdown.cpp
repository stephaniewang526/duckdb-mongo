#include "mongo_filter_pushdown.hpp"
#include "mongo_compat.hpp"

#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

// DuckDB main uses ExpressionFilter wrapping BoundComparisonExpression instead of ConstantFilter.
// These headers only exist in their current form on DuckDB main (v1.5.x also has expression_filter.hpp
// but lacks the static helpers IsComparison/Left/Right on BoundComparisonExpression).
#ifdef DUCKDB_MAIN_VECTOR_API
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#endif

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

// Helper function to append a DuckDB Value to a MongoDB basic array builder
static void AppendValueToArray(bsoncxx::builder::basic::array &array_builder, const Value &value,
                               const LogicalType &type, const string &column_name,
                               const unordered_set<string> &objectid_columns) {
	if (value.IsNull()) {
		array_builder.append(bsoncxx::types::b_null {});
		return;
	}

	switch (type.id()) {
	case LogicalTypeId::VARCHAR: {
		auto str_val = value.GetValue<string>();
		if (IsActualObjectIdColumn(column_name, objectid_columns) && IsValidObjectIdHex(str_val)) {
			array_builder.append(bsoncxx::oid(str_val));
		} else {
			array_builder.append(str_val);
		}
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

// Build a comparison MongoDB filter document from a comparison type and constant value.
// Used by both the legacy ConstantFilter path and the DuckDB main ExpressionFilter path.
static bsoncxx::document::value BuildComparisonFilterDoc(ExpressionType cmp_type, const Value &constant,
                                                         const string &column_name, const LogicalType &column_type,
                                                         const unordered_set<string> &objectid_columns) {
	bsoncxx::builder::basic::document doc;
	string mongo_op;
	switch (cmp_type) {
	case ExpressionType::COMPARE_EQUAL:
		AppendValueToDocument(doc, column_name, constant, column_type, column_name, objectid_columns);
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
		return doc.extract();
	}
	if (!mongo_op.empty()) {
		bsoncxx::builder::basic::document op_doc;
		AppendValueToDocument(op_doc, mongo_op, constant, column_type, column_name, objectid_columns);
		doc.append(bsoncxx::builder::basic::kvp(column_name, op_doc.extract()));
	}
	return doc.extract();
}

static bsoncxx::document::value ConvertSingleFilterToMongo(const TableFilter &filter, const string &column_name,
                                                           const LogicalType &column_type,
                                                           const unordered_set<string> &objectid_columns) {
	bsoncxx::builder::basic::document doc;

	switch (filter.filter_type) {
#ifndef DUCKDB_MAIN_VECTOR_API
	// DuckDB v1.5.x uses typed TableFilter subclasses. DuckDB main uses ExpressionFilter for all scan filters.
	case TableFilterType::CONSTANT_COMPARISON: {
		const auto &constant_filter = filter.Cast<ConstantFilter>();
		return BuildComparisonFilterDoc(constant_filter.comparison_type, constant_filter.constant, column_name,
		                                column_type, objectid_columns);
	}
	case TableFilterType::IN_FILTER: {
		const auto &in_filter = filter.Cast<InFilter>();
		if (in_filter.values.empty()) {
			break;
		}
		bsoncxx::builder::basic::array in_array;
		for (const auto &val : in_filter.values) {
			AppendValueToArray(in_array, val, column_type, column_name, objectid_columns);
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
		const auto &conj_filter = static_cast<const ConjunctionFilter &>(filter);
		bsoncxx::builder::basic::document merged_doc;
		for (const auto &child_filter : conj_filter.child_filters) {
			auto child_doc = ConvertSingleFilterToMongo(*child_filter, column_name, column_type, objectid_columns);
			for (auto it = child_doc.view().begin(); it != child_doc.view().end(); ++it) {
				if (it->key() == column_name && it->type() == bsoncxx::type::k_document) {
					for (auto nested_it = it->get_document().value.begin(); nested_it != it->get_document().value.end();
					     ++nested_it) {
						merged_doc.append(bsoncxx::builder::basic::kvp(
						    nested_it->key(), bsoncxx::types::bson_value::value(nested_it->get_value())));
					}
				} else {
					merged_doc.append(
					    bsoncxx::builder::basic::kvp(it->key(), bsoncxx::types::bson_value::value(it->get_value())));
				}
			}
		}
		doc.append(bsoncxx::builder::basic::kvp(column_name, merged_doc.extract()));
		break;
	}
	case TableFilterType::CONJUNCTION_OR: {
		const auto &conj_filter = static_cast<const ConjunctionFilter &>(filter);
		bsoncxx::builder::basic::array or_array;
		for (const auto &child_filter : conj_filter.child_filters) {
			if (child_filter->filter_type == TableFilterType::CONSTANT_COMPARISON) {
				const auto &cf = child_filter->Cast<ConstantFilter>();
				if (cf.comparison_type == ExpressionType::COMPARE_EQUAL) {
					bsoncxx::builder::basic::document or_doc;
					AppendValueToDocument(or_doc, column_name, cf.constant, column_type, column_name, objectid_columns);
					or_array.append(or_doc.extract());
					continue;
				}
			}
			auto child_doc = ConvertSingleFilterToMongo(*child_filter, column_name, column_type, objectid_columns);
			if (!child_doc.view().empty()) {
				or_array.append(child_doc.view());
			}
		}
		if (!or_array.view().empty()) {
			doc.append(bsoncxx::builder::basic::kvp("$or", or_array.extract()));
		}
		break;
	}
	case TableFilterType::STRUCT_EXTRACT: {
		const auto &struct_filter = filter.Cast<StructFilter>();
		if (struct_filter.child_filter) {
			string nested_path = column_name + "." + struct_filter.child_name;
			return ConvertSingleFilterToMongo(*struct_filter.child_filter, nested_path, column_type, objectid_columns);
		}
		break;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		const auto &opt_filter = filter.Cast<OptionalFilter>();
		if (opt_filter.child_filter) {
			return ConvertSingleFilterToMongo(*opt_filter.child_filter, column_name, column_type, objectid_columns);
		}
		break;
	}
	case TableFilterType::DYNAMIC_FILTER: {
		const auto &dyn_filter = filter.Cast<DynamicFilter>();
		if (!dyn_filter.filter_data || !dyn_filter.filter_data->initialized) {
			break;
		}
		if (!dyn_filter.filter_data->filter) {
			break;
		}
		return ConvertSingleFilterToMongo(*dyn_filter.filter_data->filter, column_name, column_type, objectid_columns);
	}
#endif // !DUCKDB_MAIN_VECTOR_API
#ifdef DUCKDB_MAIN_VECTOR_API
	case TableFilterType::EXPRESSION_FILTER: {
		// DuckDB main wraps all scan filters in ExpressionFilter.
		const auto &expr_filter = filter.Cast<ExpressionFilter>();
		if (!expr_filter.expr) {
			break;
		}
		// Skip optional/dynamic expression wrappers — we cannot guarantee their value at planning time.
		if (ExpressionFilter::IsOptionalFilter(filter)) {
			break;
		}
		auto &inner_expr = *expr_filter.expr;
		// Handle comparison expressions (e.g., col = 5, col > 10).
		// DuckDB main represents comparisons as BoundFunctionExpression; use BoundComparisonExpression helpers.
		if (BoundComparisonExpression::IsComparison(inner_expr)) {
			auto &cmp = inner_expr.Cast<BoundFunctionExpression>();
			const Expression *const_side = nullptr;
			ExpressionType cmp_type = inner_expr.GetExpressionType();
			const Expression &right = BoundComparisonExpression::Right(cmp);
			const Expression &left = BoundComparisonExpression::Left(cmp);
			if (right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				const_side = &right;
			} else if (left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				const_side = &left;
				switch (cmp_type) {
				case ExpressionType::COMPARE_LESSTHAN:
					cmp_type = ExpressionType::COMPARE_GREATERTHAN;
					break;
				case ExpressionType::COMPARE_LESSTHANOREQUALTO:
					cmp_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
					break;
				case ExpressionType::COMPARE_GREATERTHAN:
					cmp_type = ExpressionType::COMPARE_LESSTHAN;
					break;
				case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
					cmp_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
					break;
				default:
					break;
				}
			}
			if (const_side) {
				auto &const_expr = const_side->Cast<BoundConstantExpression>();
				return BuildComparisonFilterDoc(cmp_type, const_expr.value, column_name, column_type, objectid_columns);
			}
		}
		// Handle conjunction expressions (AND / OR).
		if (inner_expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
			auto &conj = inner_expr.Cast<BoundConjunctionExpression>();
			if (conj.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
				bsoncxx::builder::basic::document merged_doc;
				for (auto &child : conj.children) {
					ExpressionFilter child_ef(child->Copy());
					auto child_doc = ConvertSingleFilterToMongo(child_ef, column_name, column_type, objectid_columns);
					auto child_view = child_doc.view();
					for (auto it = child_view.begin(); it != child_view.end(); ++it) {
						if (it->key() == column_name && it->type() == bsoncxx::type::k_document) {
							for (auto nested = it->get_document().value.begin();
							     nested != it->get_document().value.end(); ++nested) {
								merged_doc.append(bsoncxx::builder::basic::kvp(
								    nested->key(), bsoncxx::types::bson_value::value(nested->get_value())));
							}
						} else {
							merged_doc.append(bsoncxx::builder::basic::kvp(
							    it->key(), bsoncxx::types::bson_value::value(it->get_value())));
						}
					}
				}
				doc.append(bsoncxx::builder::basic::kvp(column_name, merged_doc.extract()));
				return doc.extract();
			}
			if (conj.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
				bsoncxx::builder::basic::array or_array;
				for (auto &child : conj.children) {
					ExpressionFilter child_ef(child->Copy());
					auto child_doc = ConvertSingleFilterToMongo(child_ef, column_name, column_type, objectid_columns);
					if (!child_doc.view().empty()) {
						or_array.append(child_doc.view());
					}
				}
				if (!or_array.view().empty()) {
					doc.append(bsoncxx::builder::basic::kvp("$or", or_array.extract()));
				}
				return doc.extract();
			}
		}
		// Handle IS NULL / IS NOT NULL operator expressions.
		if (inner_expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
			auto &op_expr = inner_expr.Cast<BoundOperatorExpression>();
			if (op_expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL) {
				doc.append(bsoncxx::builder::basic::kvp(column_name, bsoncxx::types::b_null {}));
				return doc.extract();
			}
			if (op_expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) {
				bsoncxx::builder::basic::document ne_doc;
				ne_doc.append(bsoncxx::builder::basic::kvp("$ne", bsoncxx::types::b_null {}));
				doc.append(bsoncxx::builder::basic::kvp(column_name, ne_doc.extract()));
				return doc.extract();
			}
		}
		break;
	}
#endif // DUCKDB_MAIN_VECTOR_API
	default:
		break;
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

	// Keep track of column filters to merge if needed
	unordered_map<string, bsoncxx::builder::basic::document> column_filters;
	vector<bsoncxx::document::value> global_filters;

	// Process each filter
	MongoForEachFilter(*filters, [&](idx_t col_idx, TableFilter &filter_ref) {
		if (col_idx >= column_names.size()) {
			return;
		}

		const string &column_name = column_names[col_idx];
		const auto &column_type = column_types[col_idx];

		// Handle nested field paths for MongoDB
		string mongo_column_name = column_name;
		auto path_it = column_name_to_mongo_path.find(column_name);
		if (path_it != column_name_to_mongo_path.end()) {
			mongo_column_name = path_it->second;
		}

		auto filter_doc = ConvertSingleFilterToMongo(filter_ref, mongo_column_name, column_type, objectid_columns);

		// Skip empty filters
		if (filter_doc.view().empty()) {
			return;
		}

		auto filter_view = filter_doc.view();
		auto filter_it = filter_view.begin();
		if (filter_it != filter_view.end() && !filter_it->key().empty() && filter_it->key().front() == '$') {
			global_filters.push_back(std::move(filter_doc));
			return;
		}

		// Merge filters for the same column if needed
		auto it = column_filters.find(mongo_column_name);
		if (it == column_filters.end()) {
			column_filters[mongo_column_name] = bsoncxx::builder::basic::document {};
			auto &doc = column_filters[mongo_column_name];
			for (auto filter_it = filter_doc.view().begin(); filter_it != filter_doc.view().end(); ++filter_it) {
				doc.append(bsoncxx::builder::basic::kvp(filter_it->key(),
				                                        bsoncxx::types::bson_value::value(filter_it->get_value())));
			}
		} else {
			auto filter_view = filter_doc.view();
			for (auto elem_it = filter_view.begin(); elem_it != filter_view.end(); ++elem_it) {
				if (elem_it->key() == mongo_column_name && elem_it->type() == bsoncxx::type::k_document) {
					auto nested_doc = elem_it->get_document().value;
					for (auto nested_it = nested_doc.begin(); nested_it != nested_doc.end(); ++nested_it) {
						it->second.append(bsoncxx::builder::basic::kvp(
						    nested_it->key(), bsoncxx::types::bson_value::value(nested_it->get_value())));
					}
				} else {
					it->second.append(bsoncxx::builder::basic::kvp(
					    elem_it->key(), bsoncxx::types::bson_value::value(elem_it->get_value())));
				}
			}
		}
	});

	vector<bsoncxx::document::value> conjuncts;

	// Add column filters to query
	for (auto &col_filter_pair : column_filters) {
		auto col_doc = col_filter_pair.second.extract();
		auto col_doc_view = col_doc.view();
		auto it = col_doc_view.begin();
		if (it != col_doc_view.end() && it->key() == col_filter_pair.first) {
			auto next_it = it;
			++next_it;
			if (next_it == col_doc_view.end()) {
				bsoncxx::builder::basic::document single_filter;
				single_filter.append(
				    bsoncxx::builder::basic::kvp(it->key(), bsoncxx::types::bson_value::value(it->get_value())));
				conjuncts.push_back(single_filter.extract());
				continue;
			}
		}
		bsoncxx::builder::basic::document merged_filter;
		merged_filter.append(bsoncxx::builder::basic::kvp(col_filter_pair.first, col_doc));
		conjuncts.push_back(merged_filter.extract());
	}

	for (auto &global_filter : global_filters) {
		conjuncts.push_back(std::move(global_filter));
	}

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
