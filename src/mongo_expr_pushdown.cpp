#include "mongo_expr_pushdown.hpp"

#include "mongo_table_function.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/enums/expression_type.hpp"
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

namespace duckdb {
namespace {

// Helper function to unwrap CAST expressions and get the underlying column reference
static const BoundColumnRefExpression *UnwrapCastToColumnRef(const Expression &expr) {
	const Expression *current = &expr;
	// Unwrap CAST expressions recursively
	while (current->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		auto &cast_expr = current->Cast<BoundCastExpression>();
		current = cast_expr.child.get();
	}
	// Check if we have a column reference
	if (current->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		return &current->Cast<BoundColumnRefExpression>();
	}
	return nullptr;
}

// Helper function to convert a column reference to MongoDB field path
static string GetMongoPathForColumn(const BoundColumnRefExpression &col_ref, const vector<string> &column_names,
                                    const unordered_map<string, string> &column_name_to_mongo_path) {
	idx_t col_idx = col_ref.binding.column_index;
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
	const Value &val = const_expr.value;
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
	// Check argument count
	// Note: This logic will need to be extended to handle function overloads i.e. function with the same name that take
	// different argument types.
	if (func_expr.children.size() != mapping.arg_count) {
		return false;
	}

	// Check required argument types if specified
	if (!mapping.required_arg_types.empty() && mapping.required_arg_types.size() == mapping.arg_count) {
		for (idx_t i = 0; i < mapping.arg_count; i++) {
			if (func_expr.children[i]->return_type.id() != mapping.required_arg_types[i]) {
				return false;
			}
		}
	}

	if (mapping.mongo_operator == "$substrCP") {
		// Expect substring(string, start, length) with numeric constants for start/length.
		if (func_expr.children.size() != 3) {
			return false;
		}
		auto first_type = func_expr.children[0]->return_type.id();
		if (first_type != LogicalTypeId::VARCHAR) {
			return false;
		}
		auto start_class = func_expr.children[1]->GetExpressionClass();
		auto len_class = func_expr.children[2]->GetExpressionClass();
		return start_class == ExpressionClass::BOUND_CONSTANT && len_class == ExpressionClass::BOUND_CONSTANT;
	}

	return true;
}

static bool IsSafeMongoFunction(const BoundFunctionExpression &func_expr) {
	const MongoFunctionMapping *mapping = FindFunctionMapping(func_expr.function.name);
	if (!mapping) {
		return false;
	}
	if (mapping->mongo_operator != "$substrCP") {
		return false;
	}
	if (!ValidateFunctionSignature(func_expr, *mapping)) {
		return false;
	}
	auto &start_expr = func_expr.children[1]->Cast<BoundConstantExpression>();
	auto &len_expr = func_expr.children[2]->Cast<BoundConstantExpression>();
	auto start_val = start_expr.value.GetValue<int64_t>();
	auto len_val = len_expr.value.GetValue<int64_t>();
	return start_val >= 1 && len_val >= 0;
}

static bool IsSafeMongoExpr(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) {
		return false;
	}
	auto &comp_expr = expr.Cast<BoundComparisonExpression>();
	if (comp_expr.left && comp_expr.left->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		return IsSafeMongoFunction(comp_expr.left->Cast<BoundFunctionExpression>());
	}
	if (comp_expr.right && comp_expr.right->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		return IsSafeMongoFunction(comp_expr.right->Cast<BoundFunctionExpression>());
	}
	return false;
}

// Helper function to convert a DuckDB function to MongoDB aggregation operator
static bool ConvertFunctionToMongoExpr(const BoundFunctionExpression &func_expr, const vector<string> &column_names,
                                       const unordered_map<string, string> &column_name_to_mongo_path,
                                       bsoncxx::builder::basic::document &result_builder) {
	const string &func_name = func_expr.function.name;

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

	for (idx_t i = 0; i < func_expr.children.size(); i++) {
		auto &arg = func_expr.children[i];

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
				auto start_val = const_expr.value.GetValue<int64_t>();
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
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) {
		return false;
	}
	auto &comp_expr = expr.Cast<BoundComparisonExpression>();

	const Expression *left_expr = comp_expr.left.get();
	const Expression *right_expr = comp_expr.right.get();

	// Unwrap CAST on left side
	while (left_expr->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		auto &cast_expr = left_expr->Cast<BoundCastExpression>();
		left_expr = cast_expr.child.get();
	}

	// Unwrap CAST on right side
	while (right_expr->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		auto &cast_expr = right_expr->Cast<BoundCastExpression>();
		right_expr = cast_expr.child.get();
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
                                         idx_t table_index, bsoncxx::builder::basic::document &result_builder) {
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

	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comp_expr = expr.Cast<BoundComparisonExpression>();

		// Unwrap CAST expressions on both sides to get to underlying expressions
		const Expression *left_expr = comp_expr.left.get();
		const Expression *right_expr = comp_expr.right.get();

		// Unwrap CAST on left side
		while (left_expr->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
			auto &cast_expr = left_expr->Cast<BoundCastExpression>();
			left_expr = cast_expr.child.get();
		}

		// Unwrap CAST on right side
		while (right_expr->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
			auto &cast_expr = right_expr->Cast<BoundCastExpression>();
			right_expr = cast_expr.child.get();
		}

		bool left_is_column = left_expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF;
		bool right_is_constant = right_expr->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT;
		bool right_is_column = right_expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF;
		bool left_is_function = left_expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION;
		bool right_is_function = right_expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION;

		// Skip simple column-to-constant comparisons (e.g., age > 25)
		// These should be handled by TableFilter conversion, which produces faster MongoDB native queries
		// (e.g., {age: {$gt: 25}}) that can use indexes efficiently, rather than slower $expr queries
		// (e.g., {$expr: {$gt: ["$age", 25]}}) which cannot use indexes as effectively.
		if (left_is_column && right_is_constant && !left_is_function && !right_is_function) {
			return false;
		}

		// Handle complex comparisons: column-to-column, function calls, etc.
		ExpressionType comp_type = comp_expr.type;
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

		// Convert left and right sides to MongoDB expressions (using unwrapped expressions)
		bsoncxx::builder::basic::array args_array;
		bsoncxx::builder::basic::document left_expr_doc;

		// Convert left side (using unwrapped expression)
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

		// Convert right side (using unwrapped expression)
		if (right_expr->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			auto &right_const = right_expr->Cast<BoundConstantExpression>();
			// Cast constant to match left expression type if needed
			// This handles cases like YEAR() returning BIGINT but constant being INTEGER
			Value const_val = right_const.value;
			if (left_expr->return_type != const_val.type()) {
				Value casted_val;
				string error_message;
				if (const_val.DefaultTryCastAs(left_expr->return_type, casted_val, &error_message, true)) {
					BoundConstantExpression casted_const(casted_val);
					AppendConstantToBSONArray(casted_const, args_array);
				} else {
					// Cast failed - use original value
					AppendConstantToBSONArray(right_const, args_array);
				}
			} else {
				AppendConstantToBSONArray(right_const, args_array);
			}
		} else {
			// Try to get MongoDB path (handles CAST-wrapped column references)
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

		// Build comparison expression: { $mongo_op: [left_expr, right_expr] }
		result_builder.append(bsoncxx::builder::basic::kvp(mongo_op, args_array.extract()));
		return true;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func_expr = expr.Cast<BoundFunctionExpression>();
		return ConvertFunctionToMongoExpr(func_expr, column_names, column_name_to_mongo_path, result_builder);
	}
	default:
		return false;
	}
}

} // namespace

// Main complex filter pushdown function
// This is called before TableFilter conversion. We intentionally skip simple
// column-to-constant comparisons here so they can be handled by TableFilter conversion,
// which produces faster MongoDB native queries.
void MongoPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
                                vector<unique_ptr<Expression>> &filters) {
	auto &mongo_data = bind_data->Cast<MongoScanData>();

	// Build MongoDB $expr document for complex filters
	bsoncxx::builder::basic::document expr_builder;
	bool has_complex_filter = false;

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

		// Try to convert expression to MongoDB $expr
		bsoncxx::builder::basic::document expr_doc;
		if (ConvertExpressionToMongoExpr(*filter_expr, mongo_data.column_names, mongo_data.column_name_to_mongo_path,
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
}

} // namespace duckdb
