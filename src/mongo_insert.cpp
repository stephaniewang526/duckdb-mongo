#include "mongo_insert.hpp"
#include "mongo_batch_sink.hpp"
#include "mongo_instance.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/basic/sub_array.hpp>
#include <bsoncxx/builder/basic/sub_document.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/oid.hpp>
#include <bsoncxx/types.hpp>
#include <bsoncxx/types/bson_value/value.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/exception/exception.hpp>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <map>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Per-cell serialization (a copy of the read/filter path's serializer)
//===--------------------------------------------------------------------===//
static bool IsValidObjectIdHex(const string &str) {
	if (str.size() != 24) {
		return false;
	}
	for (char c : str) {
		if (!std::isxdigit(static_cast<unsigned char>(c))) {
			return false;
		}
	}
	return true;
}

static bool IsActualObjectIdColumn(const string &column_name, const unordered_set<string> &objectid_columns) {
	return objectid_columns.count(column_name) > 0;
}

// The BSON value one cell serializes to. LIST becomes an array and STRUCT a
// subdocument, each built by recursing through the same rule, so the sink writes
// the RFC's Array / Subdocument / Binary forms rather than a stringified value.
// column_name and objectid_columns drive ObjectId detection at the top level; a
// nested value passes an empty name, so a hex string inside a list or struct
// stays a string.
static bsoncxx::types::bson_value::value CellToBson(const Value &value, const LogicalType &type,
                                                    const string &column_name,
                                                    const unordered_set<string> &objectid_columns) {
	namespace types = bsoncxx::types;
	if (value.IsNull()) {
		return types::bson_value::value(types::b_null {});
	}
	switch (type.id()) {
	case LogicalTypeId::VARCHAR: {
		auto str_val = value.GetValue<string>();
		if (!column_name.empty() && IsActualObjectIdColumn(column_name, objectid_columns) &&
		    IsValidObjectIdHex(str_val)) {
			return types::bson_value::value(bsoncxx::oid(str_val));
		}
		return types::bson_value::value(str_val);
	}
	case LogicalTypeId::BIGINT:
		return types::bson_value::value(value.GetValue<int64_t>());
	case LogicalTypeId::INTEGER:
		return types::bson_value::value(value.GetValue<int32_t>());
	case LogicalTypeId::DOUBLE:
		return types::bson_value::value(value.GetValue<double>());
	case LogicalTypeId::BOOLEAN:
		return types::bson_value::value(value.GetValue<bool>());
	case LogicalTypeId::DATE: {
		auto date_val = value.GetValue<date_t>();
		auto date_obj = Date::FromDate(Date::ExtractYear(date_val), Date::ExtractMonth(date_val),
		                               Date::ExtractDay(date_val));
		auto ts = Timestamp::FromDatetime(date_obj, Time::FromTime(0, 0, 0));
		return types::bson_value::value(types::b_date {std::chrono::milliseconds(Timestamp::GetEpochMs(ts))});
	}
	case LogicalTypeId::TIMESTAMP: {
		auto ts = value.GetValue<timestamp_t>();
		return types::bson_value::value(types::b_date {std::chrono::milliseconds(Timestamp::GetEpochMs(ts))});
	}
	case LogicalTypeId::BLOB: {
		auto bytes = value.GetValueUnsafe<string_t>();
		return types::bson_value::value(reinterpret_cast<const uint8_t *>(bytes.GetData()),
		                                NumericCast<uint32_t>(bytes.GetSize()), bsoncxx::binary_sub_type::k_binary);
	}
	case LogicalTypeId::LIST: {
		bsoncxx::builder::basic::array arr;
		auto child_type = ListType::GetChildType(type);
		for (auto &child : ListValue::GetChildren(value)) {
			arr.append(CellToBson(child, child_type, "", objectid_columns));
		}
		return types::bson_value::value(arr.view());
	}
	case LogicalTypeId::STRUCT: {
		bsoncxx::builder::basic::document sub;
		auto &child_types = StructType::GetChildTypes(type);
		auto &children = StructValue::GetChildren(value);
		for (idx_t i = 0; i < children.size(); i++) {
			sub.append(bsoncxx::builder::basic::kvp(child_types[i].first,
			                                        CellToBson(children[i], child_types[i].second, "", objectid_columns)));
		}
		return types::bson_value::value(sub.view());
	}
	default:
		return types::bson_value::value(value.ToString());
	}
}

static void AppendValueToDocument(bsoncxx::builder::basic::document &doc_builder, const string &key, const Value &value,
                                  const LogicalType &type, const string &column_name,
                                  const unordered_set<string> &objectid_columns) {
	doc_builder.append(bsoncxx::builder::basic::kvp(key, CellToBson(value, type, column_name, objectid_columns)));
}

//===--------------------------------------------------------------------===//
// MongoTableEntry
//===--------------------------------------------------------------------===//
MongoTableEntry::MongoTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                 shared_ptr<MongoScanData> scan_data_p)
    : TableCatalogEntry(catalog, schema, info), scan_data(std::move(scan_data_p)) {
}

unique_ptr<BaseStatistics> MongoTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

TableFunction MongoTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	// Copy the inferred schema and open a fresh connection for this scan (mongocxx::client is not shared across scans).
	auto data = make_uniq<MongoScanData>();
	data->connection_string = scan_data->connection_string;
	data->database_name = scan_data->database_name;
	data->collection_name = scan_data->collection_name;
	data->column_names = scan_data->column_names;
	data->column_types = scan_data->column_types;
	data->column_name_to_mongo_path = scan_data->column_name_to_mongo_path;
	data->objectid_columns = scan_data->objectid_columns;
	data->has_explicit_schema = scan_data->has_explicit_schema;
	data->schema_mode = scan_data->schema_mode;
	data->sample_size = scan_data->sample_size;
	// is_base_table tells the complex-filter pushdown that column references are numbered by projected position (a
	// base-table LogicalGet), so it remaps them to schema order. The aggregate / top-N / count rewrites in the MongoDB
	// optimizer already resolve columns by name, so they need no change.
	data->is_base_table = true;

	GetMongoInstance();
	data->connection = make_shared_ptr<MongoConnection>(data->connection_string);

	bind_data = std::move(data);
	return GetMongoScanTableFunction();
}

TableStorageInfo MongoTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	return info;
}

DataTable &MongoTableEntry::GetStorage() {
	throw InternalException("MongoTableEntry has no local storage");
}

unique_ptr<CatalogEntry> MongoCreateTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, ClientContext &context,
                                               const string &connection_string, const string &database_name,
                                               const string &collection_name) {
	if (connection_string.empty() || database_name.empty()) {
		return nullptr;
	}

	auto scan_data = make_shared_ptr<MongoScanData>();
	scan_data->connection_string = connection_string;
	scan_data->database_name = database_name;
	scan_data->collection_name = collection_name;

	try {
		// Only surface a table for a collection that actually exists. Schema inference on a missing collection would
		// otherwise fabricate an _id-only schema, shadowing catalog lookups (e.g. duckdb_tables in SHOW TABLES).
		GetMongoInstance();
		auto client = mongocxx::client(mongocxx::uri(connection_string));
		if (!client[database_name].has_collection(collection_name)) {
			return nullptr;
		}
		MongoBindSchema(context, *scan_data, false);
	} catch (...) {
		return nullptr;
	}

	if (scan_data->column_names.empty()) {
		// No inferable schema (empty or non-existent collection): let the caller fall back to the view path.
		return nullptr;
	}

	CreateTableInfo info(schema, collection_name);
	for (idx_t i = 0; i < scan_data->column_names.size(); i++) {
		info.columns.AddColumn(ColumnDefinition(scan_data->column_names[i], scan_data->column_types[i]));
	}

	return make_uniq_base<CatalogEntry, MongoTableEntry>(catalog, schema, info, std::move(scan_data));
}

//===--------------------------------------------------------------------===//
// Sink / source state
//===--------------------------------------------------------------------===//
class MongoInsertGlobalState : public GlobalSinkState {
public:
	mutex lock;
	atomic<idx_t> next_worker_id {0};
	idx_t insert_count = 0;
};

class MongoInsertLocalState : public LocalSinkState {
public:
	MongoInsertLocalState(const string &connection_string, idx_t batch_size)
	    : client(mongocxx::uri(connection_string)), batch(batch_size) {
	}

	//! Stable per-worker index, so worker threads are distinguishable.
	idx_t worker_id = 0;
	//! Rows this worker has staged in total.
	idx_t row_count = 0;
	//! One client per worker (mongocxx::client is not thread-safe).
	mongocxx::client client;
	//! Thread-local batched writer: staging buffer + insert_many flush, shared with the COPY ... TO path.
	MongoBatchSink batch;
};

class MongoInsertSourceState : public GlobalSourceState {
public:
	idx_t MaxThreads() override {
		return 1;
	}
};

//===--------------------------------------------------------------------===//
// MongoInsertOperator
//===--------------------------------------------------------------------===//
MongoInsertOperator::MongoInsertOperator(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                         string connection_string_p, string database_name_p, string collection_name_p,
                                         vector<string> column_names_p, vector<LogicalType> column_types_p,
                                         unordered_map<string, string> column_name_to_mongo_path_p,
                                         unordered_set<string> objectid_columns_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      connection_string(std::move(connection_string_p)), database_name(std::move(database_name_p)),
      collection_name(std::move(collection_name_p)), column_names(std::move(column_names_p)),
      column_types(std::move(column_types_p)), column_name_to_mongo_path(std::move(column_name_to_mongo_path_p)),
      objectid_columns(std::move(objectid_columns_p)) {
	batch_size = MongoResolveBatchSize();
}

string MongoInsertOperator::GetName() const {
	return "MONGO_INSERT";
}

namespace {

// A node in the document tree assembled from the columns of one row: an internal node groups sibling paths, a leaf
// holds the cell value and its full MongoDB path (used for ObjectId detection).
struct DocNode {
	std::map<string, DocNode> children;
	bool is_leaf = false;
	Value value;
	string full_path;
};

void EmitNode(bsoncxx::builder::basic::document &builder, const string &key, const DocNode &node,
              const unordered_set<string> &objectid_columns) {
	if (node.is_leaf) {
		AppendValueToDocument(builder, key, node.value, node.value.type(), node.full_path, objectid_columns);
		return;
	}
	bsoncxx::builder::basic::document sub;
	for (auto &entry : node.children) {
		EmitNode(sub, entry.first, entry.second, objectid_columns);
	}
	builder.append(bsoncxx::builder::basic::kvp(key, sub.extract()));
}

} // namespace

bsoncxx::document::value MongoSerializeRow(DataChunk &chunk, idx_t row_idx, idx_t col_count,
                                           const vector<string> &column_names,
                                           const unordered_map<string, string> &column_name_to_mongo_path,
                                           const unordered_set<string> &objectid_columns) {
	DocNode root;
	for (idx_t c = 0; c < col_count; c++) {
		// A NULL cell is omitted rather than written as BSON null, so a row that
		// does not supply _id leaves MongoDB to generate one.
		if (chunk.GetValue(c, row_idx).IsNull()) {
			continue;
		}
		const string &col_name = column_names[c];
		auto path_it = column_name_to_mongo_path.find(col_name);
		string mongo_path = path_it != column_name_to_mongo_path.end() ? path_it->second : col_name;

		auto segments = StringUtil::Split(mongo_path, '.');
		if (segments.empty()) {
			segments.push_back(col_name);
		}

		DocNode *node = &root;
		for (idx_t s = 0; s + 1 < segments.size(); s++) {
			node = &node->children[segments[s]];
		}
		auto &leaf = node->children[segments.back()];
		leaf.is_leaf = true;
		leaf.value = chunk.GetValue(c, row_idx);
		leaf.full_path = mongo_path;
	}

	bsoncxx::builder::basic::document builder;
	for (auto &entry : root.children) {
		EmitNode(builder, entry.first, entry.second, objectid_columns);
	}
	return builder.extract();
}

bsoncxx::document::value MongoInsertOperator::SerializeRow(DataChunk &chunk, idx_t row_idx, idx_t col_count) const {
	return MongoSerializeRow(chunk, row_idx, col_count, column_names, column_name_to_mongo_path, objectid_columns);
}

void MongoInsertOperator::FlushBatch(MongoInsertLocalState &lstate, mongocxx::collection &collection) const {
	if (lstate.batch.Empty()) {
		return;
	}
	lstate.batch.Flush(collection);
}

unique_ptr<GlobalSinkState> MongoInsertOperator::GetGlobalSinkState(ClientContext &context) const {
	GetMongoInstance();
	// spectrum: Init
	return make_uniq<MongoInsertGlobalState>();
}

unique_ptr<LocalSinkState> MongoInsertOperator::GetLocalSinkState(ExecutionContext &context) const {
	GetMongoInstance();
	auto &gstate = sink_state->Cast<MongoInsertGlobalState>();
	auto lstate = make_uniq<MongoInsertLocalState>(connection_string, batch_size);
	lstate->worker_id = gstate.next_worker_id.fetch_add(1);
	return std::move(lstate);
}

SinkResultType MongoInsertOperator::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<MongoInsertLocalState>();
	chunk.Flatten();

	idx_t col_count = MinValue<idx_t>(chunk.ColumnCount(), column_names.size());
	auto collection = lstate.client[database_name][collection_name];

	for (idx_t row = 0; row < chunk.size(); row++) {
		auto doc = SerializeRow(chunk, row, col_count);
		lstate.row_count++;
		if (lstate.batch.Stage(std::move(doc))) {
			FlushBatch(lstate, collection);
		}
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType MongoInsertOperator::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<MongoInsertGlobalState>();
	auto &lstate = input.local_state.Cast<MongoInsertLocalState>();

	auto collection = lstate.client[database_name][collection_name];
	FlushBatch(lstate, collection);

	lock_guard<mutex> guard(gstate.lock);
	gstate.insert_count += lstate.row_count;
	// spectrum: Combine w=std::to_string(lstate.worker_id)
	// spectrum-state: total%=gstate.insert_count
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType MongoInsertOperator::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> MongoInsertOperator::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<MongoInsertSourceState>();
}

SourceResultType MongoInsertOperator::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<MongoInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.insert_count)));
	// spectrum: Report total%=gstate.insert_count
	return SourceResultType::FINISHED;
}

} // namespace duckdb
