#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/common/table_index.hpp"
#include "duckdb/planner/column_binding.hpp"

#include <bsoncxx/document/value.hpp>
#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace duckdb {

using mongo_table_index_t = decltype(std::declval<ColumnBinding>().table_index);

// --- TableFilterSet helpers ---

inline bool MongoHasFilters(const TableFilterSet &tfs) {
	return tfs.HasFilters();
}

template <typename Fn>
inline void MongoForEachFilter(TableFilterSet &tfs, Fn fn) {
	for (auto &entry : tfs) {
		fn(static_cast<idx_t>(entry.GetIndex()), entry.Filter());
	}
}

inline void MongoSetFilter(TableFilterSet &tfs, idx_t col_idx, unique_ptr<TableFilter> filter) {
	tfs.SetFilterByColumnIndex(ProjectionIndex(col_idx), std::move(filter));
}

// --- Predicate translation ---

// Types whose constants the translator can encode as BSON with the same comparison semantics MongoDB applies.
// Filters on any other type must stay in DuckDB (see MongoSupportsPushdownType).
bool MongoCanPushdownFilterType(const LogicalType &type);

// Maps the field side of a predicate (a column or struct field reference) to a MongoDB dotted path. Returns false
// when the expression is not a field reference the caller wants translated.
using MongoFieldPathResolver = std::function<bool(const Expression &expr, std::string &mongo_path)>;

struct MongoPredicateTranslation {
	// Empty when nothing could be translated.
	bsoncxx::document::value doc;
	// True when `doc` enforces the whole predicate, so the caller may drop the DuckDB-side evaluation. Optional
	// filter children DuckDB re-checks itself never make a translation incomplete.
	bool complete;
};

// Translates comparisons, IN lists, AND/OR conjunctions and null checks over the paths `resolve` accepts.
MongoPredicateTranslation TranslatePredicateToMongo(const Expression &expr, const MongoFieldPathResolver &resolve,
                                                    const std::unordered_set<std::string> &objectid_columns);

// ANDs non-empty documents together; a single document is returned as-is.
bsoncxx::document::value MongoAndDocuments(std::vector<bsoncxx::document::value> conjuncts);

// Translates the scan's table filters (keyed by schema column index). Throws when a filter DuckDB considers fully
// pushed down cannot be translated, since nothing else would enforce it.
bsoncxx::document::value
ConvertFiltersToMongoQuery(optional_ptr<TableFilterSet> filters, const std::vector<std::string> &column_names,
                           const std::vector<LogicalType> &column_types,
                           const std::unordered_map<std::string, std::string> &column_name_to_mongo_path,
                           const std::unordered_set<std::string> &objectid_columns);

} // namespace duckdb
