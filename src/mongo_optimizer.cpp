#include "mongo_optimizer.hpp"

#include "mongo_table_function.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>

#include <sstream>
#include <utility>

namespace duckdb {

struct BindingMapRule {
	idx_t from_table_index;
	idx_t to_table_index;
	idx_t column_offset;
};

static bool IsMongoScan(const LogicalGet &get) {
	return StringUtil::CIEquals(get.function.name, "mongo_scan") && get.bind_data &&
	       dynamic_cast<MongoScanData *>(get.bind_data.get());
}

static optional_ptr<MongoScanData> GetMongoBindData(LogicalGet &get) {
	if (!get.bind_data) {
		return nullptr;
	}
	return dynamic_cast<MongoScanData *>(get.bind_data.get());
}

static void ApplyBindingRulesToExpression(unique_ptr<Expression> &expr, const vector<BindingMapRule> &rules) {
	if (!expr) {
		return;
	}
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    expr, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &child) {
		    (void)child;
		    for (const auto &rule : rules) {
			    if (colref.binding.table_index == rule.from_table_index) {
				    colref.binding.table_index = rule.to_table_index;
				    colref.binding.column_index += rule.column_offset;
			    }
		    }
	    });
}

static void ApplyBindingRulesToOperator(LogicalOperator &op, const vector<BindingMapRule> &rules);

static void ApplyBindingRulesToOperatorChildren(LogicalOperator &op, const vector<BindingMapRule> &rules) {
	for (auto &child : op.children) {
		if (child) {
			ApplyBindingRulesToOperator(*child, rules);
		}
	}
}

static void ApplyBindingRulesToOrderNodes(vector<BoundOrderByNode> &orders, const vector<BindingMapRule> &rules) {
	for (auto &order : orders) {
		ApplyBindingRulesToExpression(order.expression, rules);
	}
}

static void ApplyBindingRulesToOperator(LogicalOperator &op, const vector<BindingMapRule> &rules) {
	for (auto &expr : op.expressions) {
		ApplyBindingRulesToExpression(expr, rules);
	}

	switch (op.type) {
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto &order = op.Cast<LogicalOrder>();
		ApplyBindingRulesToOrderNodes(order.orders, rules);
		break;
	}
	case LogicalOperatorType::LOGICAL_TOP_N: {
		auto &topn = op.Cast<LogicalTopN>();
		ApplyBindingRulesToOrderNodes(topn.orders, rules);
		break;
	}
	default:
		break;
	}

	ApplyBindingRulesToOperatorChildren(op, rules);
}

static string JoinJsonArray(const vector<bsoncxx::document::value> &stages) {
	std::stringstream ss;
	ss << "[";
	for (idx_t i = 0; i < stages.size(); i++) {
		if (i > 0) {
			ss << ",";
		}
		ss << bsoncxx::to_json(stages[i].view());
	}
	ss << "]";
	return ss.str();
}

static bool DocIsEmpty(const bsoncxx::document::view &v) {
	return v.begin() == v.end();
}

static bsoncxx::document::value BuildMatchFromExistingFilters(const LogicalGet &get, const MongoScanData &data) {
	vector<bsoncxx::document::value> conjuncts;

	// Manual filter := '{}' parameter (if any)
	if (!data.filter_query.empty()) {
		conjuncts.push_back(bsoncxx::from_json(data.filter_query));
	}

	// TableFilterSet pushdown (simple comparisons)
	if (!get.table_filters.filters.empty()) {
		// ConvertFiltersToMongoQuery expects a mutable TableFilterSet (optional_ptr<TableFilterSet>),
		// but LogicalGet::table_filters is const here. Copy the filter set for translation.
		auto filters_copy = get.table_filters.Copy();
		auto simple = ConvertFiltersToMongoQuery(optional_ptr<TableFilterSet>(filters_copy.get()), data.column_names,
		                                         data.column_types, data.column_name_to_mongo_path);
		if (!DocIsEmpty(simple.view())) {
			conjuncts.push_back(std::move(simple));
		}
	}

	// Complex filter pushdown stored in bind data
	if (!data.complex_filter_expr.view().empty()) {
		bsoncxx::builder::basic::document expr_doc;
		expr_doc.append(bsoncxx::builder::basic::kvp("$expr", data.complex_filter_expr.view()));
		conjuncts.push_back(expr_doc.extract());
	}

	if (conjuncts.empty()) {
		return bsoncxx::builder::basic::document {}.extract();
	}
	if (conjuncts.size() == 1) {
		return std::move(conjuncts[0]);
	}

	bsoncxx::builder::basic::array and_terms;
	for (auto &c : conjuncts) {
		and_terms.append(c.view());
	}
	bsoncxx::builder::basic::document match;
	match.append(bsoncxx::builder::basic::kvp("$and", and_terms));
	return match.extract();
}

static bool IsSimpleColumnRef(const Expression &expr, idx_t expected_table_index, idx_t &out_col_idx) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &colref = expr.Cast<BoundColumnRefExpression>();
	if (colref.binding.table_index != expected_table_index) {
		return false;
	}
	out_col_idx = colref.binding.column_index;
	return true;
}

static bool IsSupportedAggregate(const BoundAggregateExpression &aggr, const LogicalGet &get, idx_t &out_child_col,
                                 string &out_kind) {
	// No DISTINCT/FILTER/ORDER BY in MVP
	if (aggr.IsDistinct() || aggr.filter || aggr.order_bys) {
		return false;
	}

	auto fname = StringUtil::Lower(aggr.function.name);
	if (fname == "count_star") {
		out_kind = "count_star";
		out_child_col = DConstants::INVALID_INDEX;
		return aggr.children.empty();
	}

	// Allow COUNT(col) only if it is a direct column ref (still non-distinct, no filter)
	if (fname == "count") {
		if (aggr.children.size() != 1) {
			return false;
		}
		idx_t col_idx;
		if (!IsSimpleColumnRef(*aggr.children[0], get.table_index, col_idx)) {
			return false;
		}
		out_kind = "count";
		out_child_col = col_idx;
		return true;
	}
	if (fname == "sum" || fname == "min" || fname == "max" || fname == "avg") {
		if (aggr.children.size() != 1) {
			return false;
		}
		idx_t col_idx;
		if (!IsSimpleColumnRef(*aggr.children[0], get.table_index, col_idx)) {
			return false;
		}
		out_kind = fname;
		out_child_col = col_idx;
		return true;
	}
	return false;
}

static string BuildTopNPipelineJson(const LogicalGet &get, const MongoScanData &data, OrderType order, idx_t limit) {
	vector<bsoncxx::document::value> stages;

	auto match_doc = BuildMatchFromExistingFilters(get, data);
	if (!DocIsEmpty(match_doc.view())) {
		bsoncxx::builder::basic::document match_stage;
		match_stage.append(bsoncxx::builder::basic::kvp("$match", match_doc.view()));
		stages.push_back(match_stage.extract());
	}

	bsoncxx::builder::basic::document sort_spec;
	sort_spec.append(bsoncxx::builder::basic::kvp("_id", order == OrderType::ASCENDING ? 1 : -1));
	bsoncxx::builder::basic::document sort_stage;
	sort_stage.append(bsoncxx::builder::basic::kvp("$sort", sort_spec.extract()));
	stages.push_back(sort_stage.extract());

	bsoncxx::builder::basic::document limit_stage;
	limit_stage.append(bsoncxx::builder::basic::kvp("$limit", static_cast<int64_t>(limit)));
	stages.push_back(limit_stage.extract());

	return JoinJsonArray(stages);
}

static bool RewriteMongoTopN(unique_ptr<LogicalOperator> &node) {
	if (!node || node->type != LogicalOperatorType::LOGICAL_TOP_N) {
		return false;
	}
	auto &topn = node->Cast<LogicalTopN>();
	if (topn.children.size() != 1) {
		return false;
	}
	if (topn.offset != 0 || topn.limit == 0) {
		return false;
	}
	if (topn.orders.size() != 1) {
		return false;
	}

	auto &order = topn.orders[0];
	if (order.null_order != OrderByNullType::ORDER_DEFAULT) {
		// Keep conservative semantics in MVP
		return false;
	}

	// Must be ORDER BY _id
	if (!order.expression || order.expression->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &colref = order.expression->Cast<BoundColumnRefExpression>();

	auto &child = *topn.children[0];
	if (child.type != LogicalOperatorType::LOGICAL_GET) {
		return false;
	}
	auto &get = child.Cast<LogicalGet>();
	if (!IsMongoScan(get)) {
		return false;
	}
	auto bind = GetMongoBindData(get);
	if (!bind) {
		return false;
	}

	// Ensure the sort key corresponds to the _id column in the scan output
	if (colref.binding.table_index != get.table_index) {
		return false;
	}
	auto sort_col_idx = colref.binding.column_index;
	if (sort_col_idx >= bind->column_names.size() || !StringUtil::CIEquals(bind->column_names[sort_col_idx], "_id")) {
		return false;
	}

	auto pipeline_json = BuildTopNPipelineJson(get, *bind, order.type, topn.limit);

	// Replace bind data with a pipeline-enabled bind data (schema stays the same)
	auto new_bind = make_uniq<MongoScanData>();
	*new_bind = *bind; // copy POD-ish fields (includes shared_ptr connection)
	new_bind->pipeline_json = pipeline_json;

	get.bind_data = std::move(new_bind);
	get.named_parameters["pipeline"] = Value(pipeline_json);
	// Remove TopN operator (pipeline already sorts/limits)
	node = std::move(topn.children[0]);
	return true;
}

static string BuildAggregatePipelineJson(const LogicalGet &get, const MongoScanData &data,
                                        const vector<pair<string, string>> &group_fields,
                                        const vector<pair<string, bsoncxx::document::value>> &aggs,
                                        bool ungrouped_count_only) {
	vector<bsoncxx::document::value> stages;

	auto match_doc = BuildMatchFromExistingFilters(get, data);
	if (!DocIsEmpty(match_doc.view())) {
		bsoncxx::builder::basic::document match_stage;
		match_stage.append(bsoncxx::builder::basic::kvp("$match", match_doc.view()));
		stages.push_back(match_stage.extract());
	}

	if (ungrouped_count_only) {
		// Use $count for COUNT(*) queries
		bsoncxx::builder::basic::document count_stage;
		count_stage.append(bsoncxx::builder::basic::kvp("$count", "count"));
		stages.push_back(count_stage.extract());
		return JoinJsonArray(stages);
	}

	// $group stage
	bsoncxx::builder::basic::document group_spec;
	if (group_fields.empty()) {
		group_spec.append(bsoncxx::builder::basic::kvp("_id", bsoncxx::types::b_null {}));
	} else {
		bsoncxx::builder::basic::document id_doc;
		for (const auto &gf : group_fields) {
			// gf.first is output field name, gf.second is mongo path
			id_doc.append(bsoncxx::builder::basic::kvp(gf.first, StringUtil::Format("$%s", gf.second)));
		}
		group_spec.append(bsoncxx::builder::basic::kvp("_id", id_doc.extract()));
	}
	for (const auto &agg : aggs) {
		group_spec.append(bsoncxx::builder::basic::kvp(agg.first, agg.second.view()));
	}
	bsoncxx::builder::basic::document group_stage;
	group_stage.append(bsoncxx::builder::basic::kvp("$group", group_spec.extract()));
	stages.push_back(group_stage.extract());

	// $project stage to flatten _id
	bsoncxx::builder::basic::document project_spec;
	for (const auto &gf : group_fields) {
		project_spec.append(bsoncxx::builder::basic::kvp(gf.first, StringUtil::Format("$_id.%s", gf.first)));
	}
	for (const auto &agg : aggs) {
		project_spec.append(bsoncxx::builder::basic::kvp(agg.first, 1));
	}
	project_spec.append(bsoncxx::builder::basic::kvp("_id", 0));
	bsoncxx::builder::basic::document project_stage;
	project_stage.append(bsoncxx::builder::basic::kvp("$project", project_spec.extract()));
	stages.push_back(project_stage.extract());

	return JoinJsonArray(stages);
}

static bool RewriteMongoAggregate(unique_ptr<LogicalOperator> &node, vector<BindingMapRule> &binding_rules) {
	if (!node || node->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return false;
	}
	auto &aggr = node->Cast<LogicalAggregate>();
	if (aggr.children.size() != 1) {
		return false;
	}
	if (aggr.grouping_sets.size() > 1 || !aggr.grouping_functions.empty()) {
		return false;
	}

	auto &child = *aggr.children[0];
	if (child.type != LogicalOperatorType::LOGICAL_GET) {
		return false;
	}
	auto &get = child.Cast<LogicalGet>();
	if (!IsMongoScan(get)) {
		return false;
	}
	auto bind = GetMongoBindData(get);
	if (!bind) {
		return false;
	}

	// GROUP BY keys must be direct column refs
	vector<pair<string, string>> group_fields; // {output_field_name, mongo_path}
	for (auto &gexpr : aggr.groups) {
		idx_t col_idx;
		if (!IsSimpleColumnRef(*gexpr, get.table_index, col_idx)) {
			return false;
		}
		if (col_idx >= bind->column_names.size()) {
			return false;
		}
		auto &col_name = bind->column_names[col_idx];
		auto it = bind->column_name_to_mongo_path.find(col_name);
		if (it == bind->column_name_to_mongo_path.end()) {
			return false;
		}
		group_fields.emplace_back(col_name, it->second);
	}

	// Aggregate expressions must be supported and direct
	vector<pair<string, bsoncxx::document::value>> agg_specs;
	vector<string> out_names;
	vector<LogicalType> out_types;

	// Output schema: group keys first
	for (auto &gf : group_fields) {
		out_names.push_back(gf.first);
		// Use the existing DuckDB type for that column
		auto idx = bind->column_name_to_mongo_path.find(gf.first);
		(void)idx;
		// Find type by name index lookup
		auto name_it = std::find(bind->column_names.begin(), bind->column_names.end(), gf.first);
		if (name_it == bind->column_names.end()) {
			return false;
		}
		auto type_idx = static_cast<idx_t>(name_it - bind->column_names.begin());
		out_types.push_back(bind->column_types[type_idx]);
	}

	bool ungrouped = group_fields.empty();
	bool count_star_only = false;

	if (aggr.expressions.size() == 1 && ungrouped) {
		// Special-case COUNT(*) ungrouped
		if (aggr.expressions[0]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE) {
			auto &b = aggr.expressions[0]->Cast<BoundAggregateExpression>();
			idx_t child_col;
			string kind;
			if (IsSupportedAggregate(b, get, child_col, kind) && kind == "count_star") {
				count_star_only = true;
			}
		}
	}

	if (!count_star_only) {
		for (idx_t i = 0; i < aggr.expressions.size(); i++) {
			if (aggr.expressions[i]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
				return false;
			}
			auto &b = aggr.expressions[i]->Cast<BoundAggregateExpression>();
			idx_t child_col;
			string kind;
			if (!IsSupportedAggregate(b, get, child_col, kind)) {
				return false;
			}

			// Name aggregates as __aggN for stable pipeline output
			auto out_field = StringUtil::Format("__agg%llu", i);

			bsoncxx::builder::basic::document spec;
			if (kind == "count_star") {
				spec.append(bsoncxx::builder::basic::kvp("$sum", 1));
				out_types.push_back(LogicalType::BIGINT);
			} else if (kind == "count") {
				// COUNT(col): sum(cond(col != null))
				auto col_name = bind->column_names[child_col];
				auto path_it = bind->column_name_to_mongo_path.find(col_name);
				if (path_it == bind->column_name_to_mongo_path.end()) {
					return false;
				}
				// {$sum: {$cond: [{$ne: ["$col", null]}, 1, 0]}}
				bsoncxx::builder::basic::document ne;
				bsoncxx::builder::basic::array ne_args;
				ne_args.append(StringUtil::Format("$%s", path_it->second));
				ne_args.append(bsoncxx::types::b_null {});
				ne.append(bsoncxx::builder::basic::kvp("$ne", ne_args));
				bsoncxx::builder::basic::document cond;
				bsoncxx::builder::basic::array cond_args;
				cond_args.append(ne.extract());
				cond_args.append(1);
				cond_args.append(0);
				cond.append(bsoncxx::builder::basic::kvp("$cond", cond_args));
				spec.append(bsoncxx::builder::basic::kvp("$sum", cond.extract()));
				out_types.push_back(LogicalType::BIGINT);
			} else {
				auto col_name = bind->column_names[child_col];
				auto path_it = bind->column_name_to_mongo_path.find(col_name);
				if (path_it == bind->column_name_to_mongo_path.end()) {
					return false;
				}
				spec.append(bsoncxx::builder::basic::kvp(StringUtil::Format("$%s", kind),
				                                         StringUtil::Format("$%s", path_it->second)));
				// Preserve DuckDB aggregate return type
				out_types.push_back(b.return_type);
			}

			agg_specs.emplace_back(out_field, spec.extract());
			out_names.push_back(out_field);
		}
	} else {
		// COUNT(*) only => output schema is one BIGINT named "count"
		out_names = {"count"};
		out_types = {LogicalType::BIGINT};
	}

	auto pipeline_json = BuildAggregatePipelineJson(get, *bind, group_fields, agg_specs, count_star_only);

	// Build new bind data for the pipeline output schema
	auto new_bind = make_uniq<MongoScanData>();
	new_bind->connection_string = bind->connection_string;
	new_bind->connection = bind->connection;
	new_bind->database_name = bind->database_name;
	new_bind->collection_name = bind->collection_name;
	new_bind->filter_query = ""; // folded into pipeline
	new_bind->pipeline_json = pipeline_json;
	new_bind->sample_size = bind->sample_size;
	new_bind->column_names = out_names;
	new_bind->column_types = out_types;
	new_bind->column_name_to_mongo_path.clear();
	for (idx_t i = 0; i < out_names.size(); i++) {
		new_bind->column_name_to_mongo_path[out_names[i]] = out_names[i];
	}

	// Create a new LogicalGet with table_index = group_index to preserve group bindings.
	auto replacement = make_uniq<LogicalGet>(aggr.group_index, get.function, std::move(new_bind), out_types, out_names);
	replacement->named_parameters = get.named_parameters;
	replacement->named_parameters["pipeline"] = Value(pipeline_json);
	replacement->parameters = get.parameters;

	// Add binding rewrite rule: aggregate_index bindings now refer to group_index with an offset.
	BindingMapRule rule;
	rule.from_table_index = aggr.aggregate_index;
	rule.to_table_index = aggr.group_index;
	rule.column_offset = count_star_only ? 0 : group_fields.size();
	binding_rules.push_back(rule);

	node = std::move(replacement);
	return true;
}

static void RewriteMongoPlans(unique_ptr<LogicalOperator> &node, vector<BindingMapRule> &binding_rules) {
	if (!node) {
		return;
	}

	// Try rewriting this node first (may replace it entirely)
	if (RewriteMongoTopN(node)) {
		// node replaced, continue rewriting at this node
		RewriteMongoPlans(node, binding_rules);
		return;
	}
	if (RewriteMongoAggregate(node, binding_rules)) {
		return;
	}

	// Recurse
	for (auto &child : node->children) {
		RewriteMongoPlans(child, binding_rules);
	}
}

void MongoOptimizerOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	(void)input;
	vector<BindingMapRule> binding_rules;
	RewriteMongoPlans(plan, binding_rules);
	if (!binding_rules.empty() && plan) {
		ApplyBindingRulesToOperator(*plan, binding_rules);
	}
}

} // namespace duckdb

