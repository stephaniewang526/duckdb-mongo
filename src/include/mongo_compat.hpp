#pragma once

// Compatibility layer for DuckDB v1.5.x vs main branch API differences.
// Uses __has_include to detect the target DuckDB version at compile time.

#include "duckdb/common/types/vector.hpp"

// FlatVector was moved to a separate header on DuckDB main.
#if __has_include("duckdb/common/vector/flat_vector.hpp")
#include "duckdb/common/vector/flat_vector.hpp"
#endif

// --- ExpressionFilter detection ---
// DuckDB main wraps all scan filters in ExpressionFilter; v1.5.x uses ConstantFilter directly.
#if __has_include("duckdb/planner/filter/expression_filter.hpp")
#define DUCKDB_HAS_EXPRESSION_FILTER 1
#endif

// --- Vector API compatibility ---
// DuckDB main removed GetAuxiliary(), renamed Initialize(bool) to Initialize(enum),
// made FlatVector::GetData return const, and added count_t param to Reference(Value).
#if __has_include("duckdb/common/types/size.hpp")
#include "duckdb/common/types/size.hpp"
#define DUCKDB_MAIN_VECTOR_API 1
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
