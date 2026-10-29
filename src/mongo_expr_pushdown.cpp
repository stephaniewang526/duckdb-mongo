#include "mongo_expr_pushdown.hpp"

#include "mongo_duckdb_utils.hpp"
#include "mongo_filter_pushdown.hpp"
#include "mongo_table_function.hpp"
#include "mongo_variant_pushdown.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"
#include "duckdb/optimizer/column_lifetime_analyzer.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/types.hpp>
#include <cstring>

namespace duckdb {
namespace {

// Helper function to unwrap CAST expressions and get the underlying column reference
static const BoundColumnRefExpression *UnwrapCastToColumnRef(const Expression &expr) {
	const Expression *current = &expr;
	while (BoundCastExpression::IsCast(*current)) {
		current = &BoundCastExpression::Child(current->Cast<BoundFunctionExpression>());
	}
	if (current->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		return &current->Cast<BoundColumnRefExpression>();
	}
	return nullptr;
}

// Helper function to convert a column reference to MongoDB field path
static string GetMongoPathForColumn(const BoundColumnRefExpression &col_ref, const vector<string> &column_names,
                                    const unordered_map<string, string> &column_name_to_mongo_path) {
	idx_t col_idx = col_ref.Binding().column_index;
	D_ASSERT(col_idx < column_names.size());
	if (col_idx >= column_names.size()) {
		return "";
	}
	const string &column_name = column_names[col_idx];
	auto path_it = column_name_to_mongo_path.find(column_name);
	if (path_it != column_name_to_mongo_path.end()) {
		return "$" + path_it->second;
	}
	return "$" + column_name;
}

// Helper function to get MongoDB path from an expression (handles CAST-wrapped columns)
static string GetMongoPathFromExpression(const Expression &expr, const vector<string> &column_names,
                                         const unordered_map<string, string> &column_name_to_mongo_path) {
	const BoundColumnRefExpression *col_ref = UnwrapCastToColumnRef(expr);
	if (col_ref) {
		return GetMongoPathForColumn(*col_ref, column_names, column_name_to_mongo_path);
	}
	return "";
}

// Helper function to append a constant value to a BSON array
static void AppendConstantToBSONArray(const BoundConstantExpression &const_expr,
                                      bsoncxx::builder::basic::array &array_builder) {
	const Value &val = const_expr.GetValue();
	switch (val.type().id()) {
	case LogicalTypeId::VARCHAR: {
		string str_val = val.GetValue<string>();
		array_builder.append(str_val);
		break;
	}
	case LogicalTypeId::BIGINT: {
		int64_t int_val = val.GetValue<int64_t>();
		array_builder.append(int_val);
		break;
	}
	case LogicalTypeId::DOUBLE: {
		double double_val = val.GetValue<double>();
		array_builder.append(double_val);
		break;
	}
	case LogicalTypeId::BOOLEAN: {
		bool bool_val = val.GetValue<bool>();
		array_builder.append(bool_val);
		break;
	}
	default:
		// For unsupported types, convert to string
		array_builder.append(val.ToString());
		break;
	}
}

// Function mapping configuration for MongoDB pushdown
struct MongoFunctionMapping {
	vector<string> duckdb_names;              // All aliases for this function
	string mongo_operator;                    // MongoDB aggregation operator (e.g., "$strLenCP")
	idx_t arg_count;                          // Expected argument count
	vector<LogicalTypeId> required_arg_types; // Required argument types (empty = any type)
};

static const vector<MongoFunctionMapping> MONGO_FUNCTION_MAPPINGS = {
    // String functions
    {{"length", "len", "char_length", "character_length"}, "$strLenCP", 1, {LogicalTypeId::VARCHAR}},
    {{"substring", "substr"}, "$substrCP", 3, {}},
};

// Case-insensitive map from function name to function mapping
static unordered_map<string, const MongoFunctionMapping *> BuildFunctionMappingMap() {
	unordered_map<string, const MongoFunctionMapping *> mapping_map;
	for (const auto &mapping : MONGO_FUNCTION_MAPPINGS) {
		for (const auto &name : mapping.duckdb_names) {
			string lower_name = StringUtil::Lower(name);
			mapping_map[lower_name] = &mapping;
		}
	}
	return mapping_map;
}

static const unordered_map<string, const MongoFunctionMapping *> &GetFunctionMappingMap() {
	static const auto mapping_map = BuildFunctionMappingMap();
	return mapping_map;
}

// Helper function to find a function mapping by name
static const MongoFunctionMapping *FindFunctionMapping(const string &func_name) {
	const auto &mapping_map = GetFunctionMappingMap();
	string lower_name = StringUtil::Lower(func_name);
	auto it = mapping_map.find(lower_name);
	if (it != mapping_map.end()) {
		return it->second;
	}
	return nullptr;
}

// Helper function to validate function signature
static bool ValidateFunctionSignature(const BoundFunctionExpression &func_expr, const MongoFunctionMapping &mapping) {
	const auto &children = func_expr.GetChildren();
	if (children.size() != mapping.arg_count) {
		return false;
	}

	// Check required argument types if specified
	if (!mapping.required_arg_types.empty() && mapping.required_arg_types.size() == mapping.arg_count) {
		for (idx_t i = 0; i < mapping.arg_count; i++) {
			if (children[i]->GetReturnType().id() != mapping.required_arg_types[i]) {
				return false;
			}
		}
	}

	if (mapping.mongo_operator == "$substrCP") {
		// Expect substring(string, start, length) with numeric constants for start/length.
		if (children.size() != 3) {
			return false;
		}
		auto first_type = children[0]->GetReturnType().id();
		if (first_type != LogicalTypeId::VARCHAR) {
			return false;
		}
		auto start_class = children[1]->GetExpressionClass();
		auto len_class = children[2]->GetExpressionClass();
		return start_class == ExpressionClass::BOUND_CONSTANT && len_class == ExpressionClass::BOUND_CONSTANT;
	}

	return true;
}

static bool IsSafeMongoFunction(const BoundFunctionExpression &func_expr) {
	const MongoFunctionMapping *mapping = FindFunctionMapping(func_expr.Function().GetName().GetIdentifierName());
	if (!mapping) {
		return false;
	}
	if (mapping->mongo_operator != "$substrCP") {
		return false;
	}
	if (!ValidateFunctionSignature(func_expr, *mapping)) {
		return false;
	}
	const auto &children = func_expr.GetChildren();
	auto &start_expr = children[1]->Cast<BoundConstantExpression>();
	auto &len_expr = children[2]->Cast<BoundConstantExpression>();
	auto start_val = start_expr.GetValue().GetValue<int64_t>();
	auto len_val = len_expr.GetValue().GetValue<int64_t>();
	return start_val >= 1 && len_val >= 0;
}

static bool IsSafeMongoExpr(const Expression &expr) {
	if (!BoundComparisonExpression::IsComparison(expr)) {
		return false;
	}
	const Expression &left = BoundComparisonExpression::Left(expr.Cast<BoundFunctionExpression>());
	const Expression &right = BoundComparisonExpression::Right(expr.Cast<BoundFunctionExpression>());
	if (left.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		return IsSafeMongoFunction(left.Cast<BoundFunctionExpression>());
	}
	if (right.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		return IsSafeMongoFunction(right.Cast<BoundFunctionExpression>());
	}
	return false;
}

// Helper function to convert a DuckDB function to MongoDB aggregation operator
static bool ConvertFunctionToMongoExpr(const BoundFunctionExpression &func_expr, const vector<string> &column_names,
                                       const unordered_map<string, string> &column_name_to_mongo_path,
                                       bsoncxx::builder::basic::document &result_builder) {
	const string func_name = func_expr.Function().GetName().GetIdentifierName();

	// Find function mapping
	const MongoFunctionMapping *mapping = FindFunctionMapping(func_name);
	if (!mapping) {
		return false;
	}

	// Validate function signature
	if (!ValidateFunctionSignature(func_expr, *mapping)) {
		return false;
	}

	// Build arguments array
	bsoncxx::builder::basic::array args;
	const auto &children = func_expr.GetChildren();

	for (idx_t i = 0; i < children.size(); i++) {
		auto &arg = children[i];

		// Check if argument is a column reference
		const BoundColumnRefExpression *col_ref = UnwrapCastToColumnRef(*arg);
		if (col_ref) {
			string mongo_path = GetMongoPathForColumn(*col_ref, column_names, column_name_to_mongo_path);
			if (mongo_path.empty()) {
				return false;
			}
			args.append(mongo_path);
			continue;
		}

		// Check if argument is a constant
		if (arg->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			auto &const_expr = arg->Cast<BoundConstantExpression>();
			if (mapping->mongo_operator == "$substrCP" && i == 1) {
				auto start_val = const_expr.GetValue().GetValue<int64_t>();
				args.append(bsoncxx::types::b_int64 {start_val - 1});
			} else {
				AppendConstantToBSONArray(const_expr, args);
			}
			continue;
		}

		// Unsupported argument type
		return false;
	}

	// Create MongoDB expression: { $operator: [args...] }
	result_builder.append(bsoncxx::builder::basic::kvp(mapping->mongo_operator, args.extract()));
	return true;
}

static bool IsSimpleColumnToConstantComparison(const Expression &expr) {
	if (!BoundComparisonExpression::IsComparison(expr)) {
		return false;
	}

	const Expression *left_expr = &BoundComparisonExpression::Left(expr.Cast<BoundFunctionExpression>());
	const Expression *right_expr = &BoundComparisonExpression::Right(expr.Cast<BoundFunctionExpression>());

	// Unwrap CAST on left side
	while (BoundCastExpression::IsCast(*left_expr)) {
		left_expr = &BoundCastExpression::Child(left_expr->Cast<BoundFunctionExpression>());
	}

	// Unwrap CAST on right side
	while (BoundCastExpression::IsCast(*right_expr)) {
		right_expr = &BoundCastExpression::Child(right_expr->Cast<BoundFunctionExpression>());
	}

	bool left_is_column = left_expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF;
	bool right_is_constant = right_expr->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT;
	bool left_is_function = left_expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION;
	bool right_is_function = right_expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION;

	// Simple filter: column-to-constant comparison without functions
	return left_is_column && right_is_constant && !left_is_function && !right_is_function;
}

// Helper function to convert an expression to MongoDB $expr format
static bool ConvertExpressionToMongoExpr(const Expression &expr, const vector<string> &column_names,
                                         const unordered_map<string, string> &column_name_to_mongo_path,
                                         mongo_table_index_t table_index,
                                         bsoncxx::builder::basic::document &result_builder) {
	// Check if expression is volatile or can throw
	if ((expr.IsVolatile() || expr.CanThrow()) && !IsSafeMongoExpr(expr)) {
		return false;
	}

	// Early exit for simple column-to-constant comparisons before expensive operations
	// These should be handled by TableFilter conversion, which produces faster MongoDB native queries
	if (IsSimpleColumnToConstantComparison(expr)) {
		return false;
	}

	// Extract column bindings to verify all columns are from the same table
	vector<ColumnBinding> bindings;
	ColumnLifetimeAnalyzer::ExtractColumnBindings(expr, bindings);
	for (const auto &binding : bindings) {
		if (binding.table_index != table_index) {
			return false;
		}
	}

	// Comparisons are BoundFunctionExpression, so detect them by helper rather than expression class.
	if (BoundComparisonExpression::IsComparison(expr)) {
		const Expression *left_expr = &BoundComparisonExpression::Left(expr.Cast<BoundFunctionExpression>());
		const Expression *right_expr = &BoundComparisonExpression::Right(expr.Cast<BoundFunctionExpression>());

		// Unwrap CAST on left side
		while (BoundCastExpression::IsCast(*left_expr)) {
			left_expr = &BoundCastExpression::Child(left_expr->Cast<BoundFunctionExpression>());
		}

		// Unwrap CAST on right side
		while (BoundCastExpression::IsCast(*right_expr)) {
			right_expr = &BoundCastExpression::Child(right_expr->Cast<BoundFunctionExpression>());
		}

		bool left_is_column = left_expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF;
		bool right_is_constant = right_expr->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT;
		bool right_is_column = right_expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF;
		bool left_is_function = left_expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION;
		bool right_is_function = right_expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION;

		if (left_is_column && right_is_constant && !left_is_function && !right_is_function) {
			return false;
		}

		ExpressionType comp_type = expr.GetExpressionType();
		string mongo_op;
		switch (comp_type) {
		case ExpressionType::COMPARE_GREATERTHAN:
			mongo_op = "$gt";
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			mongo_op = "$gte";
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			mongo_op = "$lt";
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			mongo_op = "$lte";
			break;
		case ExpressionType::COMPARE_EQUAL:
			mongo_op = "$eq";
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			mongo_op = "$ne";
			break;
		default:
			return false;
		}

		bsoncxx::builder::basic::array args_array;
		bsoncxx::builder::basic::document left_expr_doc;

		string left_path = GetMongoPathFromExpression(*left_expr, column_names, column_name_to_mongo_path);
		if (!left_path.empty()) {
			args_array.append(left_path);
		} else if (left_expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &left_func = left_expr->Cast<BoundFunctionExpression>();
			if (!ConvertFunctionToMongoExpr(left_func, column_names, column_name_to_mongo_path, left_expr_doc)) {
				return false;
			}
			args_array.append(left_expr_doc.extract());
		} else {
			return false;
		}

		if (right_expr->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			auto &right_const = right_expr->Cast<BoundConstantExpression>();
			Value const_val = right_const.GetValue();
			if (left_expr->GetReturnType() != const_val.type()) {
				Value casted_val;
				string error_message;
				if (MongoDefaultTryCastAs(const_val, left_expr->GetReturnType(), casted_val, &error_message, true)) {
					BoundConstantExpression casted_const(casted_val);
					AppendConstantToBSONArray(casted_const, args_array);
				} else {
					AppendConstantToBSONArray(right_const, args_array);
				}
			} else {
				AppendConstantToBSONArray(right_const, args_array);
			}
		} else {
			string right_path = GetMongoPathFromExpression(*right_expr, column_names, column_name_to_mongo_path);
			if (!right_path.empty()) {
				args_array.append(right_path);
			} else if (right_expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
				bsoncxx::builder::basic::document right_expr_doc;
				auto &right_func = right_expr->Cast<BoundFunctionExpression>();
				if (!ConvertFunctionToMongoExpr(right_func, column_names, column_name_to_mongo_path, right_expr_doc)) {
					return false;
				}
				args_array.append(right_expr_doc.extract());
			} else {
				return false;
			}
		}

		result_builder.append(bsoncxx::builder::basic::kvp(mongo_op, args_array.extract()));
		return true;
	} else if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func_expr = expr.Cast<BoundFunctionExpression>();
		return ConvertFunctionToMongoExpr(func_expr, column_names, column_name_to_mongo_path, result_builder);
	}
	return false;
}

// Resolves `struct_extract(...(column, 'a'), 'b')` to the dotted path "column.a.b". Plain column references resolve
// too, so an OR can mix struct fields with top-level columns; `saw_struct_field` tells the caller whether any struct
// field was involved. Only leaves of a pushable type resolve, so the translation stays exact.
static bool ResolveStructFieldPath(const Expression &expr, const LogicalGet &get, const MongoScanData &data,
                                   const vector<string> &effective_column_names, string &path, bool &saw_struct_field) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		if (col_ref.Binding().table_index != get.table_index) {
			return false;
		}
		string mongo_path = GetMongoPathForColumn(col_ref, effective_column_names, data.column_name_to_mongo_path);
		if (mongo_path.size() < 2) {
			return false;
		}
		path = mongo_path.substr(1); // drop the "$" prefix used by $expr paths
		return true;
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &func = expr.Cast<BoundFunctionExpression>();
	auto &name = func.Function().GetName();
	if ((name != "struct_extract" && name != "struct_extract_at") || func.GetChildren().empty() || !func.BindInfo()) {
		return false;
	}
	auto &child = *func.GetChildren()[0];
	if (child.GetReturnType().id() != LogicalTypeId::STRUCT || StructType::IsUnnamed(child.GetReturnType())) {
		return false;
	}
	auto &field_name =
	    StructType::GetChildName(child.GetReturnType(), func.BindInfo()->Cast<StructExtractBindData>().index)
	        .GetIdentifierName();
	// Dots and a leading "$" have path semantics in MongoDB.
	if (field_name.empty() || field_name[0] == '$' || field_name.find('.') != string::npos) {
		return false;
	}
	string parent;
	if (!ResolveStructFieldPath(child, get, data, effective_column_names, parent, saw_struct_field)) {
		return false;
	}
	saw_struct_field = true;
	path = parent + "." + field_name;
	return true;
}

// Struct field predicates are pushed from here rather than as table filters: DuckDB keys them on the parent column,
// whose nested type MongoSupportsPushdownType rejects so that predicates we cannot translate (whole-struct equality,
// list-typed fields) stay in DuckDB. Here we see the whole expression and only consume exact translations.
static bool TryPushdownStructFieldPredicate(const Expression &expr, const LogicalGet &get, const MongoScanData &data,
                                            const vector<string> &effective_column_names,
                                            vector<bsoncxx::document::value> &match_terms) {
	bool saw_struct_field = false;
	MongoFieldPathResolver resolve = [&](const Expression &leaf, string &path) {
		return MongoCanPushdownFilterType(leaf.GetReturnType()) &&
		       ResolveStructFieldPath(leaf, get, data, effective_column_names, path, saw_struct_field);
	};
	auto translation = TranslatePredicateToMongo(expr, resolve, data.objectid_columns);
	if (!saw_struct_field || !translation.complete || translation.doc.view().empty()) {
		return false;
	}
	match_terms.push_back(std::move(translation.doc));
	return true;
}

static bool SameBson(const bsoncxx::document::view &a, const bsoncxx::document::view &b) {
	return a.length() == b.length() && memcmp(a.data(), b.data(), a.length()) == 0;
}

// Whether `term` is already enforced by the stored match filter: equal to it or to a conjunct of its (nested) $and.
// Filter pushdown runs again after projection pushdown rewrites the filters, so the same predicate can reappear.
static bool HasMatchTerm(const bsoncxx::document::view &match_filter, const bsoncxx::document::view &term) {
	if (match_filter.empty()) {
		return false;
	}
	if (SameBson(match_filter, term)) {
		return true;
	}
	auto and_elem = match_filter["$and"];
	if (!and_elem || and_elem.type() != bsoncxx::type::k_array) {
		return false;
	}
	for (auto &&conjunct : and_elem.get_array().value) {
		if (conjunct.type() == bsoncxx::type::k_document && HasMatchTerm(conjunct.get_document().value, term)) {
			return true;
		}
	}
	return false;
}

} // namespace

// Main complex filter pushdown function
// This is called before TableFilter conversion. We intentionally skip simple
// column-to-constant comparisons here so they can be handled by TableFilter conversion,
// which produces faster MongoDB native queries.
void MongoPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
                                vector<unique_ptr<Expression>> &filters) {
	auto &mongo_data = bind_data->Cast<MongoScanData>();

	// Column references are numbered by their position in get.GetColumnIds() (the projected columns), not by schema
	// position, for attached tables and mongo_scan() alike. Remap to schema-ordered names so a column reference
	// resolves to the right field.
	auto &col_ids = get.GetColumnIds();
	vector<string> effective_column_names(col_ids.size());
	for (idx_t i = 0; i < col_ids.size(); i++) {
		idx_t schema_idx = col_ids[i].GetPrimaryIndex();
		if (schema_idx < mongo_data.column_names.size()) {
			effective_column_names[i] = mongo_data.column_names[schema_idx];
		}
	}

	// Build MongoDB $expr document for complex filters
	bsoncxx::builder::basic::document expr_builder;
	bool has_complex_filter = false;
	vector<bsoncxx::document::value> match_terms;

	// Process each filter expression
	for (auto it = filters.begin(); it != filters.end();) {
		auto &filter_expr = *it;

		// Early exit for simple filters - skip expensive conversion attempt
		// This avoids overhead from ConvertExpressionToMongoExpr for filters that
		// will be handled by TableFilter conversion anyway
		if (IsSimpleColumnToConstantComparison(*filter_expr)) {
			++it;
			continue;
		}

		if (TryPushdownStructFieldPredicate(*filter_expr, get, mongo_data, effective_column_names, match_terms)) {
			it = filters.erase(it);
			continue;
		}

		// VARIANT path predicates become a superset pre-filter; the filter stays for DuckDB to evaluate exactly
		bsoncxx::document::value variant_doc = bsoncxx::builder::basic::document {}.extract();
		if (TryTranslateVariantPredicate(*filter_expr, get, mongo_data, variant_doc)) {
			if (!HasMatchTerm(mongo_data.match_filter.view(), variant_doc.view())) {
				match_terms.push_back(std::move(variant_doc));
			}
			++it;
			continue;
		}

		// Try to convert expression to MongoDB $expr
		bsoncxx::builder::basic::document expr_doc;
		if (ConvertExpressionToMongoExpr(*filter_expr, effective_column_names, mongo_data.column_name_to_mongo_path,
		                                 get.table_index, expr_doc)) {
			// Successfully converted - merge into $expr document
			// Merge expressions using $and if we have multiple
			if (has_complex_filter) {
				// We need to wrap existing and new expressions in $and
				bsoncxx::builder::basic::document new_expr_builder;
				bsoncxx::builder::basic::array and_array;

				// Add existing expression
				auto existing_expr = expr_builder.extract();
				and_array.append(existing_expr);

				// Add new expression
				and_array.append(expr_doc.extract());

				new_expr_builder.append(bsoncxx::builder::basic::kvp("$and", and_array.extract()));
				expr_builder = std::move(new_expr_builder);
			} else {
				// First expression - just use it directly
				expr_builder = std::move(expr_doc);
			}
			has_complex_filter = true;
			// Remove from filters vector (successfully pushed down)
			it = filters.erase(it);
			continue;
		}
		// Could not convert - leave in filters vector for DuckDB to handle
		++it;
	}

	// Store complex filter expression in bind data
	if (has_complex_filter) {
		mongo_data.complex_filter_expr = expr_builder.extract();
	}
	if (!match_terms.empty()) {
		match_terms.push_back(std::move(mongo_data.match_filter));
		mongo_data.match_filter = MongoAndDocuments(std::move(match_terms));
	}
}

} // namespace duckdb
