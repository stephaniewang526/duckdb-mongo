#include "mongo_table_function.hpp"
#include "mongo_instance.hpp"
#include "mongo_filter_pushdown.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/execution/operator/helper/physical_limit.hpp"
#include "duckdb/execution/operator/helper/physical_streaming_limit.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/optimizer/column_lifetime_analyzer.hpp"
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/pipeline.hpp>
#include <mongocxx/options/aggregate.hpp>
#include <algorithm>
#include <sstream>
#include <cctype>
#include <iostream>
#include <map>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {

InsertionOrderPreservingMap<string> MongoScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	if (!input.bind_data) {
		return result;
	}
	auto &data = input.bind_data->Cast<MongoScanData>();

	result["database"] = data.database_name;
	result["collection"] = data.collection_name;
	if (!data.pipeline_json.empty()) {
		result["scan_method"] = "aggregate";
		// Keep EXPLAIN readable by truncating the pipeline
		const idx_t max_len = 400;
		if (data.pipeline_json.size() > max_len) {
			result["pipeline"] = data.pipeline_json.substr(0, max_len) + "...";
		} else {
			result["pipeline"] = data.pipeline_json;
		}
	} else {
		result["scan_method"] = "find";
		if (!data.filter_query.empty()) {
			result["filter"] = data.filter_query;
		}
		if (!data.complex_filter_expr.view().empty()) {
			result["expr"] = bsoncxx::to_json(data.complex_filter_expr.view());
		}
	}
	return result;
}

unique_ptr<FunctionData> MongoScanBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<MongoScanData>();

	// Parse function arguments
	if (input.inputs.size() < 3) {
		throw InvalidInputException(
		    "mongo_scan requires at least 3 arguments: connection_string, database, collection");
	}

	result->connection_string = input.inputs[0].GetValue<string>();
	result->database_name = input.inputs[1].GetValue<string>();
	result->collection_name = input.inputs[2].GetValue<string>();

	// Parse named parameters
	if (input.named_parameters.find("filter") != input.named_parameters.end()) {
		result->filter_query = input.named_parameters["filter"].GetValue<string>();
	}

	if (input.named_parameters.find("pipeline") != input.named_parameters.end()) {
		result->pipeline_json = input.named_parameters["pipeline"].GetValue<string>();
	}

	if (input.named_parameters.find("sample_size") != input.named_parameters.end()) {
		result->sample_size = input.named_parameters["sample_size"].GetValue<int64_t>();
	}

	// Parse schema_mode parameter
	if (input.named_parameters.find("schema_mode") != input.named_parameters.end()) {
		result->schema_mode = ParseSchemaMode(input.named_parameters["schema_mode"].GetValue<string>());
	}

	// Ensure MongoDB instance is initialized
	GetMongoInstance();

	// Create connection
	result->connection = make_shared_ptr<MongoConnection>(result->connection_string);

	// Get collection
	auto db = result->connection->client[result->database_name];
	auto collection = db[result->collection_name];

	// Schema resolution priority:
	// 1. User-provided columns parameter (highest priority)
	// 2. __schema document in collection (for Atlas SQL customers)
	// 3. Infer from documents (fallback)
	bool schema_set = false;

	// Check for user-provided columns parameter
	if (input.named_parameters.find("columns") != input.named_parameters.end()) {
		ParseSchemaFromColumnsParameter(context, input.named_parameters["columns"], result->column_names,
		                                result->column_types, result->column_name_to_mongo_path);
		schema_set = true;
		result->has_explicit_schema = true; // Explicit schema via columns parameter
	}

	// If no user-provided schema, check for __schema document (Atlas SQL)
	if (!schema_set) {
		schema_set = ParseSchemaFromAtlasDocument(collection, result->column_names, result->column_types,
		                                          result->column_name_to_mongo_path);
		if (schema_set) {
			result->has_explicit_schema = true; // Explicit schema via __schema document
		}
	}

	// If still no schema, infer from documents
	if (!schema_set) {
		InferSchemaFromDocuments(collection, result->sample_size, result->column_names, result->column_types,
		                         result->column_name_to_mongo_path);
	}

	// Set return types and names
	return_types = result->column_types;
	names = result->column_names;

	return std::move(result);
}

bsoncxx::document::value BuildMongoProjection(const vector<column_t> &column_ids,
                                              const vector<string> &all_column_names,
                                              const unordered_map<string, string> &column_name_to_mongo_path) {
	// Collect all MongoDB paths for requested columns
	vector<string> mongo_paths;
	bool has_id = false;

	for (column_t col_id : column_ids) {
		// Skip virtual columns (like ROWID) - virtual columns start at VIRTUAL_COLUMN_START
		if (col_id >= VIRTUAL_COLUMN_START) {
			continue;
		}

		idx_t col_idx = col_id;
		if (col_idx >= all_column_names.size()) {
			continue;
		}

		const string &column_name = all_column_names[col_idx];

		// Get MongoDB path for this column
		string mongo_path;
		auto path_it = column_name_to_mongo_path.find(column_name);
		if (path_it != column_name_to_mongo_path.end()) {
			mongo_path = path_it->second;
		} else {
			mongo_path = column_name;
		}

		// Track if _id is included
		if (mongo_path == "_id") {
			has_id = true;
		}

		mongo_paths.push_back(mongo_path);
	}

	// Sort paths to enable efficient prefix collapsing
	// Sort lexicographically so parent paths come before nested paths
	// (e.g., "address" comes before "address.zip")
	// This makes prefix detection straightforward: if path A is a prefix of path B,
	// then A will appear before B in sorted order
	sort(mongo_paths.begin(), mongo_paths.end());

	// Collapse paths with common prefixes
	// If we have both a parent path and nested children, keep only the parent
	// (MongoDB will include parent documents when projecting nested fields)
	// Example: ["address", "address.zip", "address.city"] -> ["address"]
	// Example: ["address.zip", "address.city"] -> ["address.zip", "address.city"]
	vector<string> collapsed_paths;
	for (const string &path : mongo_paths) {
		// Check if this path is nested under any existing path
		// Since paths are sorted, we only need to check the last few paths
		bool is_nested = false;
		for (auto it = collapsed_paths.rbegin(); it != collapsed_paths.rend(); ++it) {
			if (path.find(*it + ".") == 0) {
				// Current path is nested under existing path, skip it
				is_nested = true;
				break;
			}
			// Since paths are sorted, if current path doesn't start with this existing path,
			// it won't start with any earlier paths either
			if (*it < path) {
				break;
			}
		}

		if (!is_nested) {
			// Remove any existing paths that are nested under current path
			// Since paths are sorted, these would be at the end
			while (!collapsed_paths.empty()) {
				const string &last = collapsed_paths.back();
				if (last.find(path + ".") == 0) {
					collapsed_paths.pop_back();
				} else {
					break;
				}
			}
			collapsed_paths.push_back(path);
		}
	}

	// Build projection document from collapsed paths
	bsoncxx::builder::basic::document projection_builder;

	if (collapsed_paths.empty()) {
		// If we have no real fields, return empty document (no projection = return all fields)
		return bsoncxx::builder::basic::document {}.extract();
	}

	// Add all collapsed paths to projection (sorted for consistent output)
	vector<string> sorted_paths(collapsed_paths.begin(), collapsed_paths.end());
	sort(sorted_paths.begin(), sorted_paths.end());
	for (const string &path : sorted_paths) {
		projection_builder.append(bsoncxx::builder::basic::kvp(path, 1));
	}

	// Include _id if it wasn't already included (MongoDB typically includes _id by default)
	// Only add _id if we have other fields to project
	if (!has_id) {
		projection_builder.append(bsoncxx::builder::basic::kvp("_id", 1));
	}

	return projection_builder.extract();
}

unique_ptr<LocalTableFunctionState> MongoScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                       GlobalTableFunctionState *global_state) {
	const auto &data = dynamic_cast<const MongoScanData &>(*input.bind_data);
	auto result = make_uniq<MongoScanState>();

	// Store connection info in state
	result->connection = data.connection;
	result->database_name = data.database_name;
	result->collection_name = data.collection_name;
	result->filter_query = data.filter_query;
	result->pipeline_json = data.pipeline_json;

	// Projection pushdown: collect columns needed (selected + filter columns that couldn't be pushed down)
	unordered_set<idx_t> needed_column_indices;

	// Determine which columns to fetch based on filter_prune optimization
	// When filter_prune is enabled and projection_ids is populated, DuckDB has already
	// excluded filter columns that aren't used elsewhere in the query plan
	vector<column_t> columns_to_fetch;
	if (input.CanRemoveFilterColumns() && !input.projection_ids.empty()) {
		// Filter pruning is active: use projection_ids to get only columns actually needed
		// projection_ids contains indices into input.column_ids
		for (idx_t proj_idx : input.projection_ids) {
			if (proj_idx < input.column_ids.size()) {
				columns_to_fetch.push_back(input.column_ids[proj_idx]);
			}
		}
	} else {
		// No filter pruning: use all column_ids
		columns_to_fetch = input.column_ids;
	}

	// Add selected columns to needed set
	for (column_t col_id : columns_to_fetch) {
		if (col_id < VIRTUAL_COLUMN_START && col_id < data.column_names.size()) {
			needed_column_indices.insert(col_id);
		}
	}

	// Get collection
	auto db = result->connection->client[result->database_name];
	auto collection = db[result->collection_name];

	// If a pipeline is provided, execute an aggregation pipeline instead of find().
	// Note: for pipeline results that do not match the underlying collection schema,
	// callers must provide an explicit schema via the `columns` parameter.
	if (!result->pipeline_json.empty()) {
		// Populate requested columns so the scan only materializes columns DuckDB asked for.
		// (We do not automatically inject a $project stage into the pipeline here, because the pipeline may already
		// change the shape of the documents. Optimizer-generated pipelines should include $project explicitly when
		// beneficial.)
		for (column_t col_id : input.column_ids) {
			if (col_id < VIRTUAL_COLUMN_START && col_id < data.column_names.size()) {
				result->requested_column_indices.push_back(col_id);
				result->requested_column_names.push_back(data.column_names[col_id]);
				result->requested_column_types.push_back(data.column_types[col_id]);
			}
		}

		// Parse JSON array pipeline by wrapping it in a document.
		// Example input: '[{"$match":{"x":1}},{"$count":"count"}]'
		auto wrapped = StringUtil::Format("{\"pipeline\": %s}", result->pipeline_json);
		try {
			result->pipeline_document = bsoncxx::from_json(wrapped);
		} catch (const std::exception &e) {
			throw InvalidInputException("mongo_scan \"pipeline\" contains invalid JSON: %s", e.what());
		}
		auto pipeline_elem = result->pipeline_document.view()["pipeline"];
		if (!pipeline_elem || pipeline_elem.type() != bsoncxx::type::k_array) {
			throw InvalidInputException("mongo_scan \"pipeline\" must be a JSON array of stage documents");
		}

		mongocxx::pipeline pipeline;
		auto stages = pipeline_elem.get_array().value;
		for (auto it = stages.begin(); it != stages.end(); ++it) {
			if (it->type() != bsoncxx::type::k_document) {
				throw InvalidInputException("mongo_scan \"pipeline\" stages must be JSON objects");
			}
			pipeline.append_stage(it->get_document().value);
		}

		mongocxx::options::aggregate agg_opts;
		result->cursor = make_uniq<mongocxx::cursor>(collection.aggregate(pipeline, agg_opts));
		result->current = make_uniq<mongocxx::cursor::iterator>(result->cursor->begin());
		result->end = make_uniq<mongocxx::cursor::iterator>(result->cursor->end());
		return std::move(result);
	}

	// Build query from pushed-down filters first to determine which filters were successfully pushed down
	bsoncxx::document::view_or_value query_filter;
	bool filters_pushed_down = false;
	if (input.filters) {
		// Map filter column indices from column_ids space to schema space
		unordered_map<idx_t, idx_t> filter_index_map;
		for (size_t i = 0; i < input.column_ids.size(); i++) {
			column_t col_id = input.column_ids[i];
			if (col_id < VIRTUAL_COLUMN_START && col_id < data.column_names.size()) {
				filter_index_map[i] = col_id;
			}
		}

		// Create a new filter set with remapped indices
		auto remapped_filters = make_uniq<TableFilterSet>();
		for (const auto &filter_pair : input.filters->filters) {
			idx_t filter_col_idx = filter_pair.first;
			auto it = filter_index_map.find(filter_col_idx);
			if (it != filter_index_map.end()) {
				idx_t schema_col_idx = it->second;
				remapped_filters->filters[schema_col_idx] = filter_pair.second->Copy();
			}
		}

		// Only attempt conversion if we successfully remapped at least one filter
		if (!remapped_filters->filters.empty()) {
			// Convert DuckDB filters to MongoDB query using remapped indices
			auto mongo_filter = ConvertFiltersToMongoQuery(remapped_filters.get(), data.column_names, data.column_types,
			                                               data.column_name_to_mongo_path);

			// Check if filters were successfully pushed down (non-empty MongoDB query)
			// If filters are pushed down to MongoDB, MongoDB filters server-side and we don't need filter columns
			auto filter_view = mongo_filter.view();
			// Count non-empty fields in the filter document
			idx_t filter_field_count = 0;
			for (auto it = filter_view.begin(); it != filter_view.end(); ++it) {
				filter_field_count++;
			}
			// Filters were pushed down if we have a non-empty filter document
			// (empty document means conversion failed or filters couldn't be converted)
			filters_pushed_down = (filter_field_count > 0);

			query_filter = std::move(mongo_filter);
		} else {
			// No filters could be remapped, so they can't be pushed down
			filters_pushed_down = false;
			query_filter = bsoncxx::builder::stream::document {} << bsoncxx::builder::stream::finalize;
		}

		// Merge complex filter expressions ($expr) with simple filters
		if (!data.complex_filter_expr.view().empty()) {
			bsoncxx::builder::basic::document merged_query;
			auto filter_view = query_filter.view();

			// Copy all simple filters to merged query
			for (auto it = filter_view.begin(); it != filter_view.end(); ++it) {
				merged_query.append(bsoncxx::builder::basic::kvp(it->key(), it->get_value()));
			}

			// Add complex filter ($expr)
			auto complex_view = data.complex_filter_expr.view();
			if (!complex_view.empty()) {
				merged_query.append(bsoncxx::builder::basic::kvp("$expr", complex_view));
			}

			query_filter = merged_query.extract();
			filters_pushed_down = true;
		}
	} else if (!result->filter_query.empty()) {
		query_filter = bsoncxx::from_json(result->filter_query);
		filters_pushed_down = true; // Manual filter query means filters are pushed down
	} else if (!data.complex_filter_expr.view().empty()) {
		// Only complex filters, no simple filters
		bsoncxx::builder::basic::document query_with_expr;
		auto complex_view = data.complex_filter_expr.view();
		query_with_expr.append(bsoncxx::builder::basic::kvp("$expr", complex_view));
		query_filter = query_with_expr.extract();
		filters_pushed_down = true;
	} else {
		query_filter = bsoncxx::builder::stream::document {} << bsoncxx::builder::stream::finalize;
	}

	// Add filter columns to projection only if filters weren't pushed down to MongoDB.
	// Pushed-down filters are handled server-side, so we don't need those columns.
	// Unpushed filters require columns for post-scan filtering in DuckDB.
	if (input.filters && !input.filters->filters.empty() && !input.column_ids.empty() && !filters_pushed_down) {
		// Map filter indices from column_ids space to schema space
		unordered_map<idx_t, idx_t> filter_index_map;
		for (size_t i = 0; i < input.column_ids.size(); i++) {
			column_t col_id = input.column_ids[i];
			if (col_id < VIRTUAL_COLUMN_START && col_id < data.column_names.size()) {
				filter_index_map[i] = col_id;
			}
		}
		// Add filter columns to needed set (only if filters weren't pushed down)
		for (const auto &filter_pair : input.filters->filters) {
			idx_t filter_col_idx = filter_pair.first;
			auto it = filter_index_map.find(filter_col_idx);
			if (it != filter_index_map.end()) {
				needed_column_indices.insert(it->second);
			}
		}
	}

	// Store requested columns in the order DuckDB requested them (from input.column_ids)
	// This is critical: output.data columns must match the order DuckDB expects
	// input.column_ids contains the column indices in the order DuckDB wants them
	unordered_set<idx_t> needed_set(needed_column_indices.begin(), needed_column_indices.end());

	// First, add columns in the order DuckDB requested them (from input.column_ids)
	// This preserves the SELECT clause order
	for (column_t col_id : input.column_ids) {
		if (col_id < VIRTUAL_COLUMN_START && col_id < data.column_names.size()) {
			idx_t col_idx = col_id;
			if (needed_set.find(col_idx) != needed_set.end()) {
				// Check if already added (to avoid duplicates)
				bool already_added = false;
				for (idx_t req_idx : result->requested_column_indices) {
					if (req_idx == col_idx) {
						already_added = true;
						break;
					}
				}
				if (!already_added) {
					result->requested_column_indices.push_back(col_idx);
					result->requested_column_names.push_back(data.column_names[col_idx]);
					result->requested_column_types.push_back(data.column_types[col_idx]);
				}
			}
		}
	}

	// Also add any filter columns that weren't in input.column_ids (if filters weren't pushed down)
	if (input.filters && !input.filters->filters.empty() && !input.column_ids.empty() && !filters_pushed_down) {
		unordered_map<idx_t, idx_t> filter_index_map;
		for (size_t i = 0; i < input.column_ids.size(); i++) {
			column_t col_id = input.column_ids[i];
			if (col_id < VIRTUAL_COLUMN_START && col_id < data.column_names.size()) {
				filter_index_map[i] = col_id;
			}
		}
		for (const auto &filter_pair : input.filters->filters) {
			idx_t filter_col_idx = filter_pair.first;
			auto it = filter_index_map.find(filter_col_idx);
			if (it != filter_index_map.end()) {
				idx_t schema_col_idx = it->second;
				// Only add if not already added
				if (needed_set.find(schema_col_idx) != needed_set.end()) {
					// Check if already in requested columns
					bool already_added = false;
					for (idx_t req_idx : result->requested_column_indices) {
						if (req_idx == schema_col_idx) {
							already_added = true;
							break;
						}
					}
					if (!already_added) {
						result->requested_column_indices.push_back(schema_col_idx);
						result->requested_column_names.push_back(data.column_names[schema_col_idx]);
						result->requested_column_types.push_back(data.column_types[schema_col_idx]);
					}
				}
			}
		}
	}

	// Build MongoDB find options
	mongocxx::options::find opts;

	// When schema enforcement is needed, ensure ALL schema columns are fetched from MongoDB
	// so validation can check all columns, not just the ones DuckDB requested
	bool needs_schema_enforcement = data.has_explicit_schema && data.schema_mode != SchemaMode::PERMISSIVE;
	if (needs_schema_enforcement) {
		// Add all schema columns to requested_column_indices if not already present
		for (idx_t col_idx = 0; col_idx < data.column_names.size(); col_idx++) {
			bool already_added = false;
			for (idx_t req_idx : result->requested_column_indices) {
				if (req_idx == col_idx) {
					already_added = true;
					break;
				}
			}
			if (!already_added) {
				result->requested_column_indices.push_back(col_idx);
				result->requested_column_names.push_back(data.column_names[col_idx]);
				result->requested_column_types.push_back(data.column_types[col_idx]);
			}
		}
	}

	// Build MongoDB projection from requested columns
	if (!result->requested_column_indices.empty()) {
		vector<column_t> projection_column_ids(result->requested_column_indices.begin(),
		                                       result->requested_column_indices.end());
		auto projection_doc =
		    BuildMongoProjection(projection_column_ids, data.column_names, data.column_name_to_mongo_path);

		// Check if projection document has fields (empty means return all fields)
		auto proj_view = projection_doc.view();
		int field_count = 0;
		for (auto it = proj_view.begin(); it != proj_view.end(); ++it) {
			field_count++;
		}

		if (field_count > 0) {
			// Store projection document to keep it alive for cursor lifetime
			result->projection_document = std::move(projection_doc);
			opts.projection(result->projection_document.view());
		}
	}

	// LIMIT pushdown: Push constant LIMIT values to MongoDB
	// Only works when LIMIT is directly above table scan (simple queries, not Q3/Q10 with joins)
	if (input.op) {
		if (input.op->type == PhysicalOperatorType::LIMIT) {
			const auto &limit_op = input.op->Cast<PhysicalLimit>();
			if (limit_op.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
				idx_t limit_value = limit_op.limit_val.GetConstantValue();
				if (limit_value > 0 && limit_value < PhysicalLimit::MAX_LIMIT_VALUE) {
					opts.limit(limit_value);
					result->limit = limit_value;
				}
			}
		} else if (input.op->type == PhysicalOperatorType::STREAMING_LIMIT) {
			const auto &streaming_limit_op = input.op->Cast<PhysicalStreamingLimit>();
			if (streaming_limit_op.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
				idx_t limit_value = streaming_limit_op.limit_val.GetConstantValue();
				if (limit_value > 0 && limit_value < PhysicalLimit::MAX_LIMIT_VALUE) {
					opts.limit(limit_value);
					result->limit = limit_value;
				}
			}
		}
	}

	// Create cursor with query filter and options (including projection if set)
	result->cursor = make_uniq<mongocxx::cursor>(collection.find(query_filter, opts));
	result->current = make_uniq<mongocxx::cursor::iterator>(result->cursor->begin());
	result->end = make_uniq<mongocxx::cursor::iterator>(result->cursor->end());

	return std::move(result);
}

void MongoScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	const auto &bind_data = dynamic_cast<const MongoScanData &>(*data_p.bind_data);
	auto &state = dynamic_cast<MongoScanState &>(*data_p.local_state);

	if (state.finished) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = 0;
	const idx_t max_count = STANDARD_VECTOR_SIZE;

	// Handle COUNT(*) queries: output has 1 column but may have filter columns in requested_column_names
	// Skip this optimization if schema enforcement is needed (non-PERMISSIVE mode with explicit schema)
	bool needs_schema_enforcement = bind_data.has_explicit_schema && bind_data.schema_mode != SchemaMode::PERMISSIVE;
	if (output.ColumnCount() == 1 && state.requested_column_names.size() > 1 && !needs_schema_enforcement) {
		state.requested_column_names.clear();
		state.requested_column_types.clear();
		state.requested_column_indices.clear();

		while (count < max_count && *state.current != *state.end) {
			++(*state.current);
			count++;
		}
		output.SetCardinality(count);
		if (*state.current == *state.end) {
			state.finished = true;
		}
		return;
	}

	// Use requested columns if projection pushdown is active, otherwise use all columns
	const vector<string> *column_names;
	const vector<LogicalType> *column_types;

	if (!state.requested_column_names.empty()) {
		column_names = &state.requested_column_names;
		column_types = &state.requested_column_types;
	} else {
		column_names = &bind_data.column_names;
		column_types = &bind_data.column_types;
	}

	if (output.data.empty()) {
		output.Initialize(context, *column_types);
	}

	idx_t num_cols_to_use = MinValue<idx_t>(column_names->size(), output.ColumnCount());

	// Initialize vectors for scanning
	for (idx_t col_idx = 0; col_idx < num_cols_to_use; col_idx++) {
		auto &vec = output.data[col_idx];
		vec.SetVectorType(VectorType::FLAT_VECTOR);
		if (((*column_types)[col_idx].id() == LogicalTypeId::LIST ||
		     (*column_types)[col_idx].id() == LogicalTypeId::STRUCT) &&
		    !vec.GetAuxiliary()) {
			vec.Initialize(false, STANDARD_VECTOR_SIZE);
		}
	}

	// If the pipeline returned no rows for a COUNT(*) pushdown, emit a single 0 row.
	if (state.current && state.end && *state.current == *state.end && !bind_data.pipeline_json.empty() &&
	    output.ColumnCount() == 1 && state.requested_column_names.size() == 1 &&
	    StringUtil::CIEquals(state.requested_column_names[0], "count")) {
		if (output.data.empty()) {
			output.Initialize(context, *column_types);
		}
		auto &vec = output.data[0];
		vec.SetVectorType(VectorType::FLAT_VECTOR);
		FlatVector::GetData<int64_t>(vec)[0] = 0;
		FlatVector::SetNull(vec, 0, false);
		output.SetCardinality(1);
		state.finished = true;
		return;
	}

	// Scan documents and flatten into output
	while (count < max_count && *state.current != *state.end) {
		auto doc = **state.current;
		vector<string> trunc_names(column_names->begin(), column_names->begin() + num_cols_to_use);
		vector<LogicalType> trunc_types(column_types->begin(), column_types->begin() + num_cols_to_use);

		// For schema enforcement, always validate ALL schema columns
		// (DuckDB might not request all columns, e.g., for COUNT(*))
		bool row_valid = true;
		if (needs_schema_enforcement) {
			// Validate full schema first (checks all columns, doesn't write to output)
			row_valid = ValidateDocumentSchema(doc, bind_data.column_names, bind_data.column_types,
			                                   bind_data.column_name_to_mongo_path, bind_data.schema_mode);
		}

		// If row is valid (or no enforcement needed), flatten requested columns to output
		if (row_valid && num_cols_to_use > 0) {
			// Flatten only the requested columns to output
			// Note: FlattenDocument also does schema checks, but we've already validated above
			row_valid =
			    FlattenDocument(doc, trunc_names, trunc_types, output, count, bind_data.column_name_to_mongo_path,
			                    bind_data.schema_mode, bind_data.has_explicit_schema);
		}
		++(*state.current);
		if (row_valid) {
			count++;
		}
		// If row_valid is false (DROPMALFORMED), we skip incrementing count,
		// effectively dropping this row from the output
	}

	output.SetCardinality(count);

	if (state.current && state.end && *state.current == *state.end) {
		state.finished = true;
	}
}

void RegisterMongoTableFunction(DatabaseInstance &db) {
	TableFunction mongo_scan("mongo_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                         MongoScanFunction, MongoScanBind, nullptr, MongoScanInitLocal);

	// Add optional parameters
	mongo_scan.named_parameters["filter"] = LogicalType::VARCHAR;
	mongo_scan.named_parameters["sample_size"] = LogicalType::BIGINT;
	mongo_scan.named_parameters["columns"] = LogicalType::ANY;
	mongo_scan.named_parameters["schema_mode"] = LogicalType::VARCHAR;

	// Register the table function using ExtensionLoader
	// Note: This should be called from ExtensionLoader::Load, not directly
	// The ExtensionLoader will handle registration
}

} // namespace duckdb
