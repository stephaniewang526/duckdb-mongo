#pragma once

// Compatibility layer for DuckDB v1.5.x vs main branch API differences.
// Uses __has_include to detect the target DuckDB version at compile time.

#include "duckdb/common/types/vector.hpp"
#include "duckdb/planner/table_filter.hpp"

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

// DuckDB main made BoundSimpleFunction::name protected; use GetName() accessor.
#ifdef DUCKDB_MAIN_VECTOR_API
#define MONGO_FUNCTION_NAME(func) ((func).GetName())
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

} // namespace duckdb
