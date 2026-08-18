#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "mongo_table_function.hpp"
#include <bsoncxx/builder/basic/document.hpp>
#include <mongocxx/collection.hpp>

namespace duckdb {

class MongoInsertLocalState;

// Serialize one row of a DataChunk to a BSON document, re-nesting dotted MongoDB paths into subdocuments (the inverse of
// the read path's flattening). NULL cells are omitted rather than written as BSON null, so a row that supplies no _id
// lets MongoDB generate one. Shared by the INSERT sink and the COPY ... TO writer.
bsoncxx::document::value MongoSerializeRow(DataChunk &chunk, idx_t row_idx, idx_t col_count,
                                           const vector<string> &column_names,
                                           const unordered_map<string, string> &column_name_to_mongo_path,
                                           const unordered_set<string> &objectid_columns);

// A MongoDB collection surfaced as a base table so that both SELECT and INSERT resolve through the TABLE_ENTRY lookup
// path. Reads go through GetScanFunction (returns mongo_scan with pre-built bind data); the collection has no local
// storage.
class MongoTableEntry : public TableCatalogEntry {
public:
	MongoTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                shared_ptr<MongoScanData> scan_data);

	//! Schema and connection info for the collection, shared with reads and the insert operator.
	shared_ptr<MongoScanData> scan_data;

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	DataTable &GetStorage() override;
};

// Build a MongoTableEntry for a collection, running schema inference. Returns nullptr when the collection has no
// inferable schema (e.g. an empty or non-existent collection), so the caller can fall back to the view path.
unique_ptr<CatalogEntry> MongoCreateTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, ClientContext &context,
                                               const string &connection_string, const string &database_name,
                                               const string &collection_name);

// Sink operator that inserts the child plan's rows into a MongoDB collection with insert_many, and reports the inserted
// row count as its single-row source output (DuckDB reports it as the INSERT count).
class MongoInsertOperator : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

public:
	MongoInsertOperator(PhysicalPlan &physical_plan, vector<LogicalType> types, string connection_string,
	                    string database_name, string collection_name, vector<string> column_names,
	                    vector<LogicalType> column_types, unordered_map<string, string> column_name_to_mongo_path,
	                    unordered_set<string> objectid_columns, idx_t estimated_cardinality);

	string connection_string;
	string database_name;
	string collection_name;
	//! Table columns in logical order; a child chunk's column i is column_names[i].
	vector<string> column_names;
	vector<LogicalType> column_types;
	//! Inverse of the read path's flattening: flattened column name -> dotted MongoDB path.
	unordered_map<string, string> column_name_to_mongo_path;
	//! Columns whose BSON type is ObjectId, keyed by MongoDB path.
	unordered_set<string> objectid_columns;
	//! Number of staged documents that triggers an insert_many. Low values are useful for tests.
	idx_t batch_size = 1000;

public:
	string GetName() const override;

	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

	// Source interface (reports the insert count)
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                  OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

public:
	//! Serialize one row of the chunk to a BSON document, re-nesting dotted MongoDB paths into subdocuments.
	bsoncxx::document::value SerializeRow(DataChunk &chunk, idx_t row_idx, idx_t col_count) const;
	//! insert_many the worker's staged documents and clear its buffer.
	void FlushBatch(MongoInsertLocalState &lstate, mongocxx::collection &collection) const;
};

} // namespace duckdb
