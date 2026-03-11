#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/planner/table_filter.hpp"

// DuckDB main moved TableFilterSet to a separate header and made `filters` private.
// It also changed table_index from idx_t to TableIndex.
// Detect the new layout and provide helpers that compile against both v1.5.0 and main.
#if __has_include("duckdb/planner/table_filter_set.hpp")
#include "duckdb/planner/table_filter_set.hpp"
#define DUCKDB_PRIVATE_TABLE_FILTERS 1
#endif
#if __has_include("duckdb/common/table_index.hpp")
#include "duckdb/common/table_index.hpp"
#endif
#include "duckdb/planner/column_binding.hpp"

#include <bsoncxx/document/value.hpp>
#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {

// Resolves to idx_t (v1.5.0) or TableIndex (main)
using mongo_table_index_t = decltype(std::declval<ColumnBinding>().table_index);

// --- TableFilterSet compatibility helpers ---

inline bool MongoHasFilters(const TableFilterSet &tfs) {
#ifdef DUCKDB_PRIVATE_TABLE_FILTERS
	return tfs.HasFilters();
#else
	return !tfs.filters.empty();
#endif
}

template <typename Fn>
inline void MongoForEachFilter(TableFilterSet &tfs, Fn fn) {
#ifdef DUCKDB_PRIVATE_TABLE_FILTERS
	for (auto &entry : tfs) {
		fn(entry.ColumnIndex(), entry.Filter());
	}
#else
	for (auto &entry : tfs.filters) {
		fn(entry.first, *entry.second);
	}
#endif
}

inline void MongoSetFilter(TableFilterSet &tfs, idx_t col_idx, unique_ptr<TableFilter> filter) {
#ifdef DUCKDB_PRIVATE_TABLE_FILTERS
	tfs.SetFilterByColumnIndex(col_idx, std::move(filter));
#else
	tfs.filters[col_idx] = std::move(filter);
#endif
}

// --- End compatibility helpers ---

bsoncxx::document::value
ConvertFiltersToMongoQuery(optional_ptr<TableFilterSet> filters, const std::vector<std::string> &column_names,
                           const std::vector<LogicalType> &column_types,
                           const std::unordered_map<std::string, std::string> &column_name_to_mongo_path);

} // namespace duckdb
