#pragma once

// Small utilities over DuckDB APIs that would otherwise be repeated at several call sites.

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

namespace duckdb {

// Scan filters are always ExpressionFilter; TableFilter itself has no Copy().
inline unique_ptr<TableFilter> MongoCopyFilter(const TableFilter &filter) {
	return ExpressionFilter::GetExpressionFilter(filter, "MongoCopyFilter").Copy();
}

// Declare a table function's optional named parameters as typed kwargs.
inline void MongoAddNamedParameters(TableFunction &function,
                                    const vector<pair<const char *, LogicalType>> &parameters) {
	function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		for (auto &parameter : parameters) {
			options.Add(parameter.first, parameter.second);
		}
	});
}

inline vector<Identifier> MongoToIdentifiers(const vector<string> &names) {
	vector<Identifier> result;
	result.reserve(names.size());
	for (auto &n : names) {
		result.emplace_back(Identifier(n));
	}
	return result;
}

template <typename T>
inline void MongoChildListAppend(child_list_t<T> &list, const string &name, T type) {
	list.push_back({Identifier(name), std::move(type)});
}

// Value::DefaultTryCastAs returns optional<Value>; keep the bool + out-param shape for call sites.
inline bool MongoDefaultTryCastAs(const Value &value, const LogicalType &target_type, Value &result,
                                  string *error_message = nullptr, bool strict = false) {
	auto casted = value.DefaultTryCastAs(target_type, error_message, strict);
	if (casted) {
		result = std::move(*casted);
		return true;
	}
	return false;
}

} // namespace duckdb
