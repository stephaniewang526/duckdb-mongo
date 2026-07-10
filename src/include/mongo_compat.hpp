#pragma once

// Compatibility layer for DuckDB v1.5.x vs main branch API differences.
// Uses __has_include to detect the target DuckDB version at compile time.

#include "duckdb/common/types/vector.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog_entry.hpp"

// FlatVector was moved to a separate header on DuckDB main.
#if __has_include("duckdb/common/vector/flat_vector.hpp")
#include "duckdb/common/vector/flat_vector.hpp"
#endif

// --- Vector API compatibility ---
// DuckDB main removed GetAuxiliary(), renamed Initialize(bool) to Initialize(enum),
// made FlatVector::GetData return const, and added count_t param to Reference(Value).
#if __has_include("duckdb/common/types/size.hpp")
#include "duckdb/common/types/size.hpp"
#define DUCKDB_MAIN_VECTOR_API 1
#endif

#ifdef DUCKDB_MAIN_VECTOR_API
#include "duckdb/planner/filter/expression_filter.hpp"
// DuckDB main: bound_comparison_expression.hpp is transitively included above.
#else
// DuckDB v1.5.x: include explicitly so MongoIsComparisonExpr / MongoComparisonLeft/Right are available.
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#endif

// DuckDB main made BoundSimpleFunction::name protected and returns Identifier.
#ifdef DUCKDB_MAIN_VECTOR_API
#define MONGO_FUNCTION_NAME(func) ((func).GetName().GetIdentifierName())
#else
#define MONGO_FUNCTION_NAME(func) ((func).name)
#endif

namespace duckdb {

// Check if a vector has an auxiliary buffer.
inline bool MongoVectorHasAuxiliary(Vector &vec) {
#ifdef DUCKDB_MAIN_VECTOR_API
	return vec.GetBufferRef().get() != nullptr;
#else
	return vec.GetAuxiliary().get() != nullptr;
#endif
}

// Initialize a vector without zeroing memory.
inline void MongoVectorInitializeUninitialized(Vector &vec, idx_t capacity = STANDARD_VECTOR_SIZE) {
#ifdef DUCKDB_MAIN_VECTOR_API
	vec.Initialize(VectorDataInitialization::UNINITIALIZED, capacity);
#else
	vec.Initialize(false, capacity);
#endif
}

// Get a mutable data pointer from a flat vector.
template <typename T>
inline T *MongoFlatVectorGetDataMutable(Vector &vec) {
#ifdef DUCKDB_MAIN_VECTOR_API
	return FlatVector::GetDataMutable<T>(vec);
#else
	return FlatVector::GetData<T>(vec);
#endif
}

// Reference a single Value in a vector.
inline void MongoVectorReferenceSingleValue(Vector &vec, const Value &val) {
#ifdef DUCKDB_MAIN_VECTOR_API
	vec.Reference(val, count_t(1));
#else
	vec.Reference(val);
#endif
}

// Set a flat vector's tracked size (DuckDB main tracks per-vector sizes for DataChunk::Verify).
inline void MongoSetVectorSize(Vector &vec, idx_t size) {
#ifdef DUCKDB_MAIN_VECTOR_API
	FlatVector::SetSize(vec, size);
#endif
}

// Copy a single TableFilter (DuckDB main removed TableFilter::Copy(); only ExpressionFilter::Copy() remains).
inline unique_ptr<TableFilter> MongoCopyFilter(const TableFilter &filter) {
#ifdef DUCKDB_MAIN_VECTOR_API
	auto &ef = static_cast<const ExpressionFilter &>(filter);
	return ef.Copy();
#else
	return filter.Copy();
#endif
}

// Cross-version comparison expression helpers.
// DuckDB main: comparisons are BoundFunctionExpression; v1.5.x: BoundComparisonExpression.
inline bool MongoIsComparisonExpr(const Expression &expr) {
#ifdef DUCKDB_MAIN_VECTOR_API
	return BoundComparisonExpression::IsComparison(expr);
#else
	return expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON;
#endif
}

inline const Expression &MongoComparisonLeft(const Expression &expr) {
#ifdef DUCKDB_MAIN_VECTOR_API
	return BoundComparisonExpression::Left(expr.Cast<BoundFunctionExpression>());
#else
	return *expr.Cast<BoundComparisonExpression>().left;
#endif
}

inline const Expression &MongoComparisonRight(const Expression &expr) {
#ifdef DUCKDB_MAIN_VECTOR_API
	return BoundComparisonExpression::Right(expr.Cast<BoundFunctionExpression>());
#else
	return *expr.Cast<BoundComparisonExpression>().right;
#endif
}

// DuckDB main made Expression::return_type and BaseExpression::type protected.
// Provide accessor helpers that compile on both v1.5.x (public fields) and main (getters).
#ifdef DUCKDB_MAIN_VECTOR_API
#define MONGO_EXPR_RETURN_TYPE(expr) ((expr).GetReturnType())
#define MONGO_EXPR_TYPE(expr)        ((expr).GetExpressionType())
#else
#define MONGO_EXPR_RETURN_TYPE(expr) ((expr).return_type)
#define MONGO_EXPR_TYPE(expr)        ((expr).GetExpressionType())
#endif

// DuckDB main changed Expression::GetName() to return Identifier instead of string.
#ifdef DUCKDB_MAIN_VECTOR_API
inline string MongoExprName(const Expression &expr) {
	return expr.GetName().GetIdentifierName();
}
inline const string &MongoStructChildName(const LogicalType &type, idx_t index) {
	return StructType::GetChildName(type, index).GetIdentifierName();
}
inline string MongoCatalogEntryName(const CatalogEntry &entry) {
	return entry.name.GetIdentifierName();
}
#else
inline string MongoExprName(const Expression &expr) {
	return expr.GetName();
}
inline const string &MongoStructChildName(const LogicalType &type, idx_t index) {
	return StructType::GetChildName(type, index);
}
inline string MongoCatalogEntryName(const CatalogEntry &entry) {
	return entry.name;
}
#endif

// DuckDB main made several bound-expression member fields private and exposed accessors.
// These helpers compile on both v1.5.x (public fields) and main (accessors).

inline const Value &MongoConstantValue(const BoundConstantExpression &expr) {
#ifdef DUCKDB_MAIN_VECTOR_API
	return expr.GetValue();
#else
	return expr.value;
#endif
}

inline const vector<unique_ptr<Expression>> &MongoConjunctionChildren(const BoundConjunctionExpression &expr) {
#ifdef DUCKDB_MAIN_VECTOR_API
	return expr.GetChildren();
#else
	return expr.children;
#endif
}

inline const ColumnBinding &MongoColumnBinding(const BoundColumnRefExpression &expr) {
#ifdef DUCKDB_MAIN_VECTOR_API
	return expr.Binding();
#else
	return expr.binding;
#endif
}

inline ColumnBinding &MongoColumnBindingMutable(BoundColumnRefExpression &expr) {
#ifdef DUCKDB_MAIN_VECTOR_API
	return expr.BindingMutable();
#else
	return expr.binding;
#endif
}

// Cast always has a non-null child; return a pointer for uniform call sites.
inline const Expression *MongoCastChild(const BoundCastExpression &expr) {
#ifdef DUCKDB_MAIN_VECTOR_API
	return &expr.Child();
#else
	return expr.child.get();
#endif
}

inline const unique_ptr<Expression> &MongoAggregateFilter(const BoundAggregateExpression &expr) {
#ifdef DUCKDB_MAIN_VECTOR_API
	return expr.GetFilter();
#else
	return expr.filter;
#endif
}

inline const unique_ptr<BoundOrderModifier> &MongoAggregateOrderBys(const BoundAggregateExpression &expr) {
#ifdef DUCKDB_MAIN_VECTOR_API
	return expr.GetOrderBys();
#else
	return expr.order_bys;
#endif
}

inline const vector<unique_ptr<Expression>> &MongoAggregateChildren(const BoundAggregateExpression &expr) {
#ifdef DUCKDB_MAIN_VECTOR_API
	return expr.GetChildren();
#else
	return expr.children;
#endif
}

// Return type differs across versions (BoundAggregateFunction vs AggregateFunction); deduce it.
inline decltype(auto) MongoAggregateFunction(const BoundAggregateExpression &expr) {
#ifdef DUCKDB_MAIN_VECTOR_API
	return (expr.Function());
#else
	return (expr.function);
#endif
}

// BoundFunctionExpression children and function name.
inline const vector<unique_ptr<Expression>> &MongoFuncChildren(const BoundFunctionExpression &expr) {
#ifdef DUCKDB_MAIN_VECTOR_API
	return expr.GetChildren();
#else
	return expr.children;
#endif
}

inline decltype(auto) MongoFuncFunction(const BoundFunctionExpression &expr) {
#ifdef DUCKDB_MAIN_VECTOR_API
	return (expr.Function());
#else
	return (expr.function);
#endif
}

// DuckDB main changed LogicalGet column names from vector<string> to vector<Identifier>.
#ifdef DUCKDB_MAIN_VECTOR_API
#include "duckdb/common/identifier.hpp"
using MongoColumnNameVector = vector<Identifier>;
inline MongoColumnNameVector MongoMakeColumnNames(const vector<string> &names) {
	MongoColumnNameVector result;
	result.reserve(names.size());
	for (auto &n : names) {
		result.emplace_back(Identifier(n));
	}
	return result;
}
#else
using MongoColumnNameVector = vector<string>;
inline MongoColumnNameVector MongoMakeColumnNames(const vector<string> &names) {
	return names;
}
#endif

// DuckDB main changed child_list_t from vector<pair<string,T>> to vector<pair<Identifier,T>>.
#ifdef DUCKDB_MAIN_VECTOR_API
template <typename T>
inline void MongoChildListAppend(child_list_t<T> &list, const string &name, T type) {
	list.push_back({Identifier(name), std::move(type)});
}
inline const string &MongoChildPairName(const pair<Identifier, LogicalType> &p) {
	return p.first.GetIdentifierName();
}
#else
template <typename T>
inline void MongoChildListAppend(child_list_t<T> &list, const string &name, T type) {
	list.push_back({name, std::move(type)});
}
inline const string &MongoChildPairName(const pair<string, LogicalType> &p) {
	return p.first;
}
#endif

// DuckDB main refactored CreateViewInfo, CreateSchemaInfo, DropInfo to use QualifiedName.
// These helpers abstract field access for cross-version compatibility.
#ifdef DUCKDB_MAIN_VECTOR_API

inline void MongoSetViewSchema(CreateViewInfo &info, const string &schema) {
	info.SetSchema(Identifier(schema));
}
inline void MongoSetViewName(CreateViewInfo &info, const string &name) {
	info.SetName(Identifier(name));
}
inline string MongoGetSchemaName(const CreateSchemaInfo &info) {
	return info.SchemaName().GetIdentifierName();
}
inline void MongoSetSchemaName(CreateSchemaInfo &info, const string &schema) {
	info.SetName(Identifier(schema));
}
inline string MongoGetDropName(const DropInfo &info) {
	return info.GetQualifiedName().Name().GetIdentifierName();
}
inline string MongoGetViewSchema(const CreateViewInfo &info) {
	return info.GetQualifiedName().Schema().GetIdentifierName();
}
inline string MongoGetViewName(const CreateViewInfo &info) {
	return info.GetViewName().GetIdentifierName();
}
using MongoDefaultEntryList = vector<Identifier>;
inline MongoDefaultEntryList MongoMakeDefaultEntries(const vector<string> &names) {
	MongoDefaultEntryList result;
	result.reserve(names.size());
	for (auto &n : names) {
		result.emplace_back(Identifier(n));
	}
	return result;
}
#else

inline void MongoSetViewSchema(CreateViewInfo &info, const string &schema) {
	info.schema = schema;
}
inline void MongoSetViewName(CreateViewInfo &info, const string &name) {
	info.view_name = name;
}
inline string MongoGetSchemaName(const CreateSchemaInfo &info) {
	return info.schema;
}
inline void MongoSetSchemaName(CreateSchemaInfo &info, const string &schema) {
	info.schema = schema;
}
inline string MongoGetDropName(const DropInfo &info) {
	return info.name;
}
inline string MongoGetViewSchema(const CreateViewInfo &info) {
	return info.schema;
}
inline string MongoGetViewName(const CreateViewInfo &info) {
	return info.view_name;
}
using MongoDefaultEntryList = vector<string>;
inline MongoDefaultEntryList MongoMakeDefaultEntries(const vector<string> &names) {
	return names;
}
#endif

} // namespace duckdb
