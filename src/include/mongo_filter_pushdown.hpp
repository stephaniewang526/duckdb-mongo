#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/common/table_index.hpp"
#include "duckdb/planner/column_binding.hpp"

#include <bsoncxx/document/value.hpp>
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

bsoncxx::document::value
ConvertFiltersToMongoQuery(optional_ptr<TableFilterSet> filters, const std::vector<std::string> &column_names,
                           const std::vector<LogicalType> &column_types,
                           const std::unordered_map<std::string, std::string> &column_name_to_mongo_path,
                           const std::unordered_set<std::string> &objectid_columns);

} // namespace duckdb
