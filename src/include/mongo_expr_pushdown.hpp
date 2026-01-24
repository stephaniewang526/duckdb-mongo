#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class ClientContext;
class Expression;
class FunctionData;
class LogicalGet;

void MongoPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
                                vector<unique_ptr<Expression>> &filters);

} // namespace duckdb
