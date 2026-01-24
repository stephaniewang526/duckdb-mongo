#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/planner/table_filter.hpp"
#include <bsoncxx/document/value.hpp>
#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {

bsoncxx::document::value ConvertFiltersToMongoQuery(optional_ptr<TableFilterSet> filters,
                                                    const std::vector<std::string> &column_names,
                                                    const std::vector<LogicalType> &column_types,
                                                    const std::unordered_map<std::string, std::string> &column_name_to_mongo_path);

} // namespace duckdb
