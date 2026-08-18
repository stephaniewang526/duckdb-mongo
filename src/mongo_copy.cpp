#include "mongo_copy.hpp"
#include "mongo_catalog.hpp"
#include "mongo_instance.hpp"
#include "mongo_insert.hpp"
#include "mongo_batch_sink.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include <bsoncxx/builder/basic/document.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/exception/exception.hpp>
#include <cstdlib>

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Target resolution
//===--------------------------------------------------------------------===//
// Parse a COPY ... TO target of the form 'alias.collection' or 'alias.database.collection', resolving the leading alias
// to an attached MongoDB catalog and reusing its connection string. Mirrors how INSERT / CREATE TABLE AS address a
// collection through the attached catalog rather than a standalone connection.
void ResolveCopyTarget(ClientContext &context, const string &target, string &connection_string, string &database_name,
                       string &collection_name) {
	auto parts = StringUtil::Split(target, '.');
	if (parts.size() != 2 && parts.size() != 3) {
		throw BinderException("COPY ... TO (FORMAT mongo) expects a target of the form 'attached_db.collection' or "
		                      "'attached_db.database.collection', got \"%s\"",
		                      target);
	}

	const string &alias = parts[0];
	auto catalog_entry = Catalog::GetCatalogEntry(context, alias);
	if (!catalog_entry) {
		throw BinderException("COPY ... TO (FORMAT mongo): no attached database named \"%s\"", alias);
	}
	if (catalog_entry->GetCatalogType() != "mongo") {
		throw BinderException("COPY ... TO (FORMAT mongo): attached database \"%s\" is not a MongoDB catalog", alias);
	}

	auto &mongo_catalog = catalog_entry->Cast<MongoCatalog>();
	connection_string = mongo_catalog.connection_string;
	if (parts.size() == 3) {
		database_name = parts[1];
		collection_name = parts[2];
	} else {
		database_name = mongo_catalog.database_name;
		collection_name = parts[1];
		if (database_name.empty()) {
			throw BinderException("COPY ... TO (FORMAT mongo): attached database \"%s\" has no default database; use "
			                      "'attached_db.database.collection'",
			                      alias);
		}
	}
}

//===--------------------------------------------------------------------===//
// Bind / execution state
//===--------------------------------------------------------------------===//
struct MongoCopyBindData : public FunctionData {
	string connection_string;
	string database_name;
	string collection_name;
	//! Output columns of the SELECT, in order; column i of each chunk is column_names[i].
	vector<string> column_names;
	vector<LogicalType> column_types;
	//! Number of staged documents that triggers an insert_many (env-overridable, as for the INSERT sink).
	idx_t batch_size = 1000;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<MongoCopyBindData>();
		result->connection_string = connection_string;
		result->database_name = database_name;
		result->collection_name = collection_name;
		result->column_names = column_names;
		result->column_types = column_types;
		result->batch_size = batch_size;
		return std::move(result);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<MongoCopyBindData>();
		return connection_string == other.connection_string && database_name == other.database_name &&
		       collection_name == other.collection_name && column_names == other.column_names &&
		       batch_size == other.batch_size;
	}
};

// COPY tracks the written row count itself (PhysicalCopyToFile::rows_copied), so the global state carries no MongoDB
// handle; each worker opens its own client in the local state (mongocxx::client is not thread-safe).
struct MongoCopyGlobalState : public GlobalFunctionData {};

struct MongoCopyLocalState : public LocalFunctionData {
	MongoCopyLocalState(const string &connection_string, idx_t batch_size)
	    : client(mongocxx::uri(connection_string)), batch(batch_size) {
	}

	mongocxx::client client;
	MongoBatchSink batch;
};

//===--------------------------------------------------------------------===//
// Copy callbacks
//===--------------------------------------------------------------------===//
unique_ptr<FunctionData> MongoCopyBind(ClientContext &context, CopyFunctionBindInput &input,
                                       const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto result = make_uniq<MongoCopyBindData>();
	// Resolve the target at bind time, before physical planning expands the target string as a filesystem path. Because
	// the target is not a real file, DuckDB leaves use_tmp_file off, so no filesystem operation touches it.
	ResolveCopyTarget(context, input.info.file_path, result->connection_string, result->database_name,
	                  result->collection_name);
	result->column_names = names;
	result->column_types = sql_types;
	result->batch_size = MongoResolveBatchSize();

	// A new collection would otherwise appear only on the first insert, so an empty result set would leave nothing
	// behind. Create it up front so COPY ... TO always produces the collection, matching CREATE TABLE AS.
	MongoBatchSink::EnsureCollection(result->connection_string, result->database_name, result->collection_name);

	return std::move(result);
}

unique_ptr<GlobalFunctionData> MongoCopyInitializeGlobal(ClientContext &context, FunctionData &bind_data,
                                                         const string &file_path) {
	GetMongoInstance();
	// spectrum: Init
	return make_uniq<MongoCopyGlobalState>();
}

unique_ptr<LocalFunctionData> MongoCopyInitializeLocal(ExecutionContext &context, FunctionData &bind_data) {
	GetMongoInstance();
	auto &bdata = bind_data.Cast<MongoCopyBindData>();
	return make_uniq<MongoCopyLocalState>(bdata.connection_string, bdata.batch_size);
}

void MongoCopySink(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                   LocalFunctionData &lstate_p, DataChunk &input) {
	auto &bdata = bind_data.Cast<MongoCopyBindData>();
	auto &lstate = lstate_p.Cast<MongoCopyLocalState>();
	input.Flatten();

	idx_t col_count = MinValue<idx_t>(input.ColumnCount(), bdata.column_names.size());
	auto collection = lstate.client[bdata.database_name][bdata.collection_name];

	// A fresh collection has no inferred schema, so columns map straight to top-level fields (no flatten reversal) and
	// no column is known to be an ObjectId -- the same choice CREATE TABLE AS makes.
	static const unordered_map<string, string> no_paths;
	static const unordered_set<string> no_objectid_columns;

	for (idx_t row = 0; row < input.size(); row++) {
		if (lstate.batch.Stage(MongoSerializeRow(input, row, col_count, bdata.column_names, no_paths,
		                                         no_objectid_columns))) {
			lstate.batch.Flush(collection);
		}
	}
}

void MongoCopyCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                      LocalFunctionData &lstate_p) {
	auto &bdata = bind_data.Cast<MongoCopyBindData>();
	auto &lstate = lstate_p.Cast<MongoCopyLocalState>();
	auto collection = lstate.client[bdata.database_name][bdata.collection_name];
	lstate.batch.Flush(collection);
}

void MongoCopyFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	// All documents are flushed per worker in Combine; nothing global to finalize.
}

} // namespace

CopyFunction GetMongoCopyFunction() {
	CopyFunction info("mongo");
	info.copy_to_bind = MongoCopyBind;
	info.copy_to_initialize_global = MongoCopyInitializeGlobal;
	info.copy_to_initialize_local = MongoCopyInitializeLocal;
	info.copy_to_sink = MongoCopySink;
	info.copy_to_combine = MongoCopyCombine;
	info.copy_to_finalize = MongoCopyFinalize;
	info.extension = "mongo";
	return info;
}

} // namespace duckdb
