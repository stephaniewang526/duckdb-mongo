#include "mongo_catalog.hpp"
#include "mongo_duckdb_utils.hpp"
#include "mongo_instance.hpp"
#include "mongo_insert.hpp"
#include "mongo_batch_sink.hpp"
#include "mongo_schema_entry.hpp"
#include "mongo_secrets.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include <bsoncxx/builder/basic/document.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/client.hpp>

namespace duckdb {

// Forward declaration
class MongoCatalog;

// Default generator for MongoDB collections (creates views dynamically).
class MongoCollectionGenerator : public DefaultGenerator {
public:
	MongoCollectionGenerator(Catalog &catalog, SchemaCatalogEntry &schema, const string &connection_string_p,
	                         const string &database_name_p, MongoCatalog *mongo_catalog_p = nullptr)
	    : DefaultGenerator(catalog), schema(schema), connection_string(connection_string_p),
	      database_name(database_name_p), collections_loaded(false), mongo_catalog(mongo_catalog_p) {
		GetMongoInstance();
		// Pre-warm connection by creating the client
		// This ensures the connection string is validated early, but doesn't force
		// database access which might fail if database doesn't exist yet
		// The actual connection will be established on first use
		try {
			GetOrCreateClient();
		} catch (...) {
			// Connection will be established on first actual query if pre-warm fails
		}
	}

public:
	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const Identifier &entry_name) override {
		string name_str = entry_name.GetIdentifierName();
		EnsureCollectionsLoaded();
		for (const auto &collection_name : collection_names) {
			if (StringUtil::CIEquals(name_str, collection_name)) {
				return CreateEntryForCollection(context, collection_name);
			}
		}
		return nullptr;
	}

	vector<Identifier> GetDefaultEntries() override {
		EnsureCollectionsLoaded();
		return MongoToIdentifiers(collection_names);
	}

	unique_ptr<CatalogEntry> CreateEntryForCollection(ClientContext &context, const string &collection_name) {
		// Check cache to avoid expensive SQL parsing
		if (mongo_catalog) {
			auto cached_info = mongo_catalog->GetCachedViewInfo(database_name, collection_name);
			if (cached_info) {
				auto info_copy = make_uniq<CreateViewInfo>();
				info_copy->SetSchema(schema.name);
				info_copy->SetName(Identifier(collection_name));
				info_copy->sql = cached_info->sql;
				info_copy->query = cached_info->query
				                       ? unique_ptr_cast<SQLStatement, SelectStatement>(cached_info->query->Copy())
				                       : nullptr;
				info_copy->types = cached_info->types;
				info_copy->names = cached_info->names;
				info_copy->aliases = cached_info->aliases;
				info_copy->temporary = cached_info->temporary;
				info_copy->internal = cached_info->internal;
				info_copy->dependencies = cached_info->dependencies;

				auto entry = make_uniq_base<CatalogEntry, ViewCatalogEntry>(catalog, schema, *info_copy);
				return entry;
			}
		}

		auto result = make_uniq<CreateViewInfo>();
		result->SetSchema(schema.name);
		result->SetName(Identifier(collection_name));

		auto escape_sql_string = [](const string &str) -> string {
			string result;
			result.reserve(str.size() + str.size() / 10);
			for (char c : str) {
				if (c == '\'') {
					result += "''";
				} else {
					result += c;
				}
			}
			return result;
		};

		if (cached_escaped_connection_string.empty()) {
			cached_escaped_connection_string = escape_sql_string(connection_string);
			cached_escaped_database_name = escape_sql_string(database_name);
		}
		string escaped_collection_name = escape_sql_string(collection_name);

		result->sql = StringUtil::Format("SELECT * FROM mongo_scan('%s', '%s', '%s')", cached_escaped_connection_string,
		                                 cached_escaped_database_name, escaped_collection_name);

		Parser parser(context);
		auto view_info = CreateViewInfo::FromSelect(parser, std::move(result));

		if (mongo_catalog) {
			mongo_catalog->CacheViewInfo(database_name, collection_name, *view_info);
		}

		auto entry = make_uniq_base<CatalogEntry, ViewCatalogEntry>(catalog, schema, *view_info);
		return entry;
	}

private:
	mongocxx::client &GetOrCreateClient() {
		if (!cached_client || cached_connection_string != connection_string) {
			string conn_str = connection_string;
			bool has_query_params = conn_str.find('?') != string::npos;

			if (conn_str.find("connectTimeoutMS") == string::npos) {
				if (!has_query_params) {
					conn_str += "?connectTimeoutMS=5000";
					has_query_params = true;
				} else {
					conn_str += "&connectTimeoutMS=5000";
				}
			}
			if (conn_str.find("serverSelectionTimeoutMS") == string::npos) {
				if (!has_query_params) {
					conn_str += "?serverSelectionTimeoutMS=5000";
				} else {
					conn_str += "&serverSelectionTimeoutMS=5000";
				}
			}
			if (conn_str.find("socketTimeoutMS") == string::npos) {
				if (!has_query_params) {
					conn_str += "?socketTimeoutMS=5000";
				} else {
					conn_str += "&socketTimeoutMS=5000";
				}
			}

			mongocxx::uri uri(conn_str);
			cached_client = make_uniq<mongocxx::client>(uri);
			cached_connection_string = connection_string;
		}
		return *cached_client;
	}

	void EnsureCollectionsLoaded() {
		if (collections_loaded) {
			return;
		}

		// Skip DuckDB internal schemas
		// The "" database backs the placeholder "main" schema of an ATTACH without a database.
		if (database_name.empty() || database_name == "main" || database_name == "information_schema" ||
		    database_name == "pg_catalog") {
			collections_loaded = true;
			return;
		}

		// A failed login or an unreachable server must not look like an empty
		// database, so errors propagate and the next call retries.
		vector<string> listed;
		try {
			auto &client = GetOrCreateClient();
			auto mongo_db = client[database_name];
			for (const auto &collection : mongo_db.list_collection_names()) {
				if (!StringUtil::StartsWith(collection, "system.")) {
					listed.push_back(collection);
				}
			}
		} catch (const std::exception &e) {
			throw IOException("Failed to list MongoDB collections in database \"%s\": %s", database_name, e.what());
		}

		collection_names = std::move(listed);
		collections_loaded = true;
		if (mongo_catalog && !collection_names.empty()) {
			mongo_catalog->CacheCollectionNames(database_name, collection_names);
		}
	}

	SchemaCatalogEntry &schema;
	string connection_string;
	string database_name;
	vector<string> collection_names;
	bool collections_loaded;
	MongoCatalog *mongo_catalog;
	unique_ptr<mongocxx::client> cached_client;
	string cached_connection_string;
	string cached_escaped_connection_string;
	string cached_escaped_database_name;
};

MongoCatalog::MongoCatalog(AttachedDatabase &db, const string &connection_string, const string &database_name)
    : Catalog(db), connection_string(connection_string), database_name(database_name), schemas_scanned(false) {
	GetMongoInstance();
	// Set default schema early
	if (database_name.empty()) {
		default_schema = "main";
	} else {
		default_schema = database_name;
	}
}

void MongoCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> MongoCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	lock_guard<mutex> lock(schemas_lock);

	auto schema_name = info.SchemaName().GetIdentifierName();
	auto it = schemas.find(schema_name);
	if (it != schemas.end()) {
		switch (info.on_conflict) {
		case OnCreateConflict::ERROR_ON_CONFLICT:
			throw CatalogException("Schema with name \"%s\" already exists", schema_name);
		case OnCreateConflict::REPLACE_ON_CONFLICT:
			schemas.erase(it);
			break;
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return it->second.get();
		default:
			throw InternalException("Unsupported OnCreateConflict for CreateSchema");
		}
	}

	auto schema_entry = make_uniq<MongoSchemaEntry>(*this, info);
	auto result = schema_entry.get();
	schemas[schema_name] = shared_ptr<MongoSchemaEntry>(schema_entry.release());

	return result;
}

void MongoCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	{
		lock_guard<mutex> lock(schemas_lock);
		if (schemas_scanned) {
			for (auto &[name, schema] : schemas) {
				callback(*schema);
			}
			return;
		}
		schemas_scanned = true;
	}

	auto client = GetClient();
	auto system_transaction = CatalogTransaction::GetSystemTransaction(GetDatabase());

	if (!database_name.empty()) {
		// Specific database attached - create only that schema
		CreateSchemaInfo schema_info;
		schema_info.SetSchema(Identifier(database_name));
		schema_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		auto schema_entry = CreateSchema(system_transaction, schema_info);
		if (schema_entry) {
			auto &schema = schema_entry->Cast<MongoSchemaEntry>();
			auto default_generator =
			    make_uniq<MongoCollectionGenerator>(*this, schema, connection_string, database_name, this);
			schema.SetDefaultGenerator(std::move(default_generator));
			schema.SetConnectionInfo(connection_string, database_name);
			callback(schema);
		}
		if (default_schema.empty()) {
			default_schema = database_name;
		}
		return;
	}

	// No specific database - create schema for each MongoDB database
	// First create "main" schema with empty generator for DuckDB consistency
	CreateSchemaInfo main_schema_info;
	main_schema_info.SetSchema(Identifier("main"));
	main_schema_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	auto main_schema_entry = CreateSchema(system_transaction, main_schema_info);
	if (main_schema_entry) {
		auto &main_schema = main_schema_entry->Cast<MongoSchemaEntry>();
		auto default_generator = make_uniq<MongoCollectionGenerator>(*this, main_schema, connection_string, "", this);
		main_schema.SetDefaultGenerator(std::move(default_generator));
		main_schema.SetConnectionInfo(connection_string, "");
		callback(main_schema);
	}

	for (const auto &schema_name : client.list_database_names()) {
		if (schema_name == "admin" || schema_name == "local" || schema_name == "config") {
			continue;
		}

		CreateSchemaInfo schema_info;
		schema_info.SetSchema(Identifier(schema_name));
		schema_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		auto schema_entry = CreateSchema(system_transaction, schema_info);
		if (schema_entry) {
			auto &schema = schema_entry->Cast<MongoSchemaEntry>();
			auto default_generator =
			    make_uniq<MongoCollectionGenerator>(*this, schema, connection_string, schema_name, this);
			schema.SetDefaultGenerator(std::move(default_generator));
			schema.SetConnectionInfo(connection_string, schema_name);
			callback(schema);
		}
	}

	if (default_schema.empty()) {
		default_schema = "main";
	}
}

optional_ptr<SchemaCatalogEntry> MongoCatalog::LookupSchema(CatalogTransaction transaction,
                                                            const EntryLookupInfo &schema_lookup,
                                                            OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();

	// Ensure schemas are scanned before lookup
	if (!schemas_scanned) {
		auto &context = transaction.GetContext();
		ScanSchemas(context, [](SchemaCatalogEntry &) {});
	}

	if (schema_name.empty()) {
		auto default_ident = GetDefaultSchema();
		schema_name = default_ident ? default_ident->GetIdentifierName() : string();
	}

	if (!schema_name.empty() && !database_name.empty()) {
		lock_guard<mutex> lock(schemas_lock);
		auto it = schemas.find(database_name);
		if (it != schemas.end()) {
			return it->second.get();
		}
	}

	optional_ptr<SchemaCatalogEntry> schema;
	{
		lock_guard<mutex> lock(schemas_lock);
		auto it = schemas.find(schema_name);
		schema = it != schemas.end() ? it->second.get() : nullptr;
	}

	if (!schema && !schema_name.empty()) {
		// On-demand schema creation: only when no database_name is specified,
		// allow creating schemas for MongoDB databases that exist.
		// When database_name is specified, we only have the database_name schema.
		if (database_name.empty()) {
			try {
				CreateSchemaInfo schema_info;
				schema_info.SetSchema(Identifier(schema_name));
				schema_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
				auto system_transaction = CatalogTransaction::GetSystemTransaction(GetDatabase());
				auto schema_entry = CreateSchema(system_transaction, schema_info);
				if (schema_entry) {
					auto &schema_ref = schema_entry->Cast<MongoSchemaEntry>();
					auto default_generator =
					    make_uniq<MongoCollectionGenerator>(*this, schema_ref, connection_string, schema_name, this);
					schema_ref.SetDefaultGenerator(std::move(default_generator));
					schema_ref.SetConnectionInfo(connection_string, schema_name);
					schema = &schema_ref;
				}
			} catch (const std::exception &e) {
				// Fall through
			}
		}
	}

	if (!schema && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	}

	return schema;
}

PhysicalOperator &MongoCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                  LogicalCreateTable &op, PhysicalOperator &plan) {
	auto &create_info = op.info->Base();
	string schema_name = create_info.GetQualifiedName().Schema().GetIdentifierName();
	string table_name = create_info.GetTableName().GetIdentifierName();
	const string &db_name = schema_name.empty() ? database_name : schema_name;
	const string &collection_name = table_name;

	// A new collection has no inferred schema, so columns map straight to top-level fields (no flatten reversal) and no
	// column is known to be an ObjectId. The SELECT's output columns give the field names and types in order.
	auto column_names = create_info.columns.GetColumnNames();
	auto column_types = create_info.columns.GetColumnTypes();

	// Clear the dropped_tables set so subsequent lookups can find the newly created collection.
	auto &schema = op.info->schema.Cast<MongoSchemaEntry>();
	schema.ClearDroppedEntry(collection_name);

	MongoBatchSink::EnsureCollection(connection_string, db_name, collection_name);

	auto &insert = planner.Make<MongoInsertOperator>(
	    op.types, connection_string, db_name, collection_name, std::move(column_names), std::move(column_types),
	    unordered_map<string, string> {}, unordered_set<string> {}, op.estimated_cardinality);
	insert.children.push_back(plan);
	return insert;
}

PhysicalOperator &MongoCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                           optional_ptr<PhysicalOperator> plan) {
	if (!plan) {
		throw NotImplementedException("INSERT INTO ... DEFAULT VALUES is not supported for MongoDB catalogs");
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw NotImplementedException("INSERT ... ON CONFLICT is not supported for MongoDB catalogs");
	}
	if (op.return_chunk) {
		throw NotImplementedException("INSERT ... RETURNING is not supported for MongoDB catalogs");
	}

	// Project defaults so the child chunk carries all table columns in logical order.
	if (!op.column_index_map.empty()) {
		plan = planner.ResolveDefaultsProjection(op, *plan);
	}

	auto &mongo_table = op.table.Cast<MongoTableEntry>();
	auto &sd = *mongo_table.scan_data;
	auto &insert = planner.Make<MongoInsertOperator>(op.types, connection_string, sd.database_name, sd.collection_name,
	                                                 sd.column_names, sd.column_types, sd.column_name_to_mongo_path,
	                                                 sd.objectid_columns, op.estimated_cardinality);
	insert.children.push_back(*plan);
	return insert;
}

PhysicalOperator &MongoCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                           PhysicalOperator &plan) {
	throw NotImplementedException("DELETE is not supported for MongoDB catalogs");
}

PhysicalOperator &MongoCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                           PhysicalOperator &plan) {
	throw NotImplementedException("UPDATE is not supported for MongoDB catalogs");
}

void MongoCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	lock_guard<mutex> lock(schemas_lock);
	auto drop_name = info.GetQualifiedName().Name().GetIdentifierName();
	auto it = schemas.find(drop_name);
	if (it != schemas.end()) {
		schemas.erase(it);
	} else if (info.if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		throw CatalogException("Schema with name \"%s\" not found", drop_name);
	}
}

vector<string> MongoCatalog::GetCachedCollectionNames(const string &db_name) const {
	lock_guard<mutex> lock(collection_cache_lock);
	auto it = collection_cache.find(db_name);
	if (it != collection_cache.end()) {
		return it->second;
	}
	return vector<string>();
}

void MongoCatalog::CacheCollectionNames(const string &db_name, const vector<string> &collections) {
	lock_guard<mutex> lock(collection_cache_lock);
	collection_cache[db_name] = collections;
}

shared_ptr<CreateViewInfo> MongoCatalog::GetCachedViewInfo(const string &db_name, const string &collection_name) const {
	lock_guard<mutex> lock(view_info_cache_lock);
	string cache_key = db_name + ":" + collection_name;
	auto it = view_info_cache.find(cache_key);
	if (it != view_info_cache.end()) {
		return it->second;
	}
	return nullptr;
}

void MongoCatalog::CacheViewInfo(const string &db_name, const string &collection_name, const CreateViewInfo &info) {
	lock_guard<mutex> lock(view_info_cache_lock);
	string cache_key = db_name + ":" + collection_name;
	auto cached = make_shared_ptr<CreateViewInfo>();
	cached->SetSchema(info.GetQualifiedName().Schema());
	cached->SetName(info.GetViewName());
	cached->sql = info.sql;
	cached->query = info.query ? unique_ptr_cast<SQLStatement, SelectStatement>(info.query->Copy()) : nullptr;
	cached->types = info.types;
	cached->names = info.names;
	cached->aliases = info.aliases;
	cached->temporary = info.temporary;
	cached->internal = info.internal;
	cached->dependencies = info.dependencies;
	view_info_cache[cache_key] = cached;
}

void MongoCatalog::InvalidateCollectionNamesCache(const string &db_name) {
	lock_guard<mutex> lock(collection_cache_lock);
	collection_cache.erase(db_name);
}

void MongoCatalog::InvalidateViewInfoCache(const string &db_name, const string &collection_name) {
	lock_guard<mutex> lock(view_info_cache_lock);
	string cache_key = db_name + ":" + collection_name;
	view_info_cache.erase(cache_key);
}

void MongoCatalog::ClearCache() {
	{
		lock_guard<mutex> lock(collection_cache_lock);
		collection_cache.clear();
	}
	{
		lock_guard<mutex> lock(view_info_cache_lock);
		view_info_cache.clear();
	}
	{
		lock_guard<mutex> lock(schemas_lock);
		for (auto &[name, schema] : schemas) {
			schema->InvalidateCache();
		}
		schemas_scanned = false;
	}
}

void MongoCatalog::VerifyScanConnectionAllowed(ClientContext &context, const string &connection_string) {
	Value setting_val;
	auto lookup = context.TryGetCurrentSetting("mongo_enable_direct_scan", setting_val);
	if (!lookup || setting_val.IsNull() || setting_val.GetValue<bool>()) {
		return;
	}

	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &catalog = db_ref->GetCatalog();
		if (catalog.GetCatalogType() != "mongo") {
			continue;
		}
		if (catalog.Cast<MongoCatalog>().connection_string == connection_string) {
			return;
		}
	}

	throw BinderException(
	    "Direct mongo_scan is disabled because mongo_enable_direct_scan is false. Query an attached MongoDB "
	    "database, or pass a connection string that exactly matches an attached catalog.");
}

string MongoCatalog::GetConnectionString(ClientContext &context, const string &attach_path, string secret_name) {
	// if no secret is specified we default to the unnamed mongo secret, if it exists
	string connection_string = attach_path;
	bool explicit_secret = !secret_name.empty();
	if (!explicit_secret) {
		// look up settings from the default unnamed mongo secret if none is provided
		secret_name = "__default_mongo";
	}

	auto secret_entry = GetMongoSecret(context, secret_name);
	if (secret_entry) {
		// secret found - build connection string from secret
		const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
		connection_string = BuildMongoConnectionString(kv_secret, attach_path);
	} else if (explicit_secret) {
		// secret not found and one was explicitly provided - throw an error
		throw BinderException("Secret with name \"%s\" not found", secret_name);
	}
	return connection_string;
}

} // namespace duckdb
