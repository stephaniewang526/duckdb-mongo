#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

// Optimizer extension entry point (runs after built-in optimizers).
// Rewrites eligible Mongo plans (COUNT/GROUPBY/TopN) into `mongo_scan(pipeline := ...)`.
void MongoOptimizerOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

} // namespace duckdb

