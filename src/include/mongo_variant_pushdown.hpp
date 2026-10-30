#pragma once

#include "duckdb.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <bsoncxx/document/value.hpp>
#include <string>
#include <vector>

namespace duckdb {

struct MongoScanData;

// Field keys of a column DuckDB narrowed to a VARIANT path (ColumnIndexType::PUSHDOWN_EXTRACT), outermost first.
// Returns false for a plain column or a path with a positional (array index) step.
bool MongoVariantExtractKeys(const ColumnIndex &index, vector<string> &keys);

// A key that can be written in MongoDB dot notation without changing meaning.
bool MongoIsPlainFieldKey(const std::string &key);

// Translates a predicate over `variant_extract` paths of VARIANT columns of `get` into a MongoDB filter that matches
// every document the predicate holds for. DuckDB casts VARIANT liberally ('5'::INT, 5.4::INT) while MongoDB compares
// by BSON type, so the result is a superset pre-filter and the caller must keep evaluating the predicate in DuckDB.
// Returns false when nothing could be translated.
bool TryTranslateVariantPredicate(const Expression &expr, const LogicalGet &get, const MongoScanData &data,
                                  bsoncxx::document::value &result);

} // namespace duckdb
