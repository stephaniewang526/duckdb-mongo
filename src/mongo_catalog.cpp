#include "mongo_catalog.hpp"
#include "mongo_instance.hpp"
#include "mongo_schema_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/main/attached_database.hpp"
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
		// Pre-warm connection
		try {
			GetOrCreateClient();
		} catch (...) {
			// Retry when needed
		}
	}

public:
	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override {
		EnsureCollectionsLoaded();
		for (const auto &collection_name : collection_names) {
			if (StringUtil::CIEquals(entry_name, collection_name)) {
				return CreateEntryForCollection(context, collection_name);
			}
		}
		return nullptr;
	}

	vector<string> GetDefaultEntries() override {
		EnsureCollectionsLoaded();
		return collection_names;
	}

	unique_ptr<CatalogEntry> CreateEntryForCollection(ClientContext &context, const string &collection_name) {
		// Check cache to avoid expensive SQL parsing
		if (mongo_catalog) {
			auto cached_info = mongo_catalog->GetCachedViewInfo(database_name, collection_name);
			if (cached_info) {
				auto info_copy = make_uniq<CreateViewInfo>();
				info_copy->schema = schema.name;
				info_copy->view_name = collection_name;
				info_copy->sql = cached_info->sql;
				info_copy->query = cached_info->query ? unique_ptr_cast<SQLStatement, SelectStatement>(cached_info->query->Copy()) : nullptr;
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
		result->schema = schema.name;
		result->view_name = collection_name;

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

		auto view_info = CreateViewInfo::FromSelect(context, std::move(result));
		
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
		if (database_name == "main" || database_name == "information_schema" || database_name == "pg_catalog") {
			collections_loaded = true;
			return;
		}

		// Always fetch fresh collection names from MongoDB
		collections_loaded = true;

		try {
			auto &client = GetOrCreateClient();
			auto mongo_db = client[database_name];
			auto collections = mongo_db.list_collection_names();

			vector<string> filtered_collections;
			filtered_collections.reserve(collections.size());
			collection_names.reserve(collections.size());

			for (const auto &collection : collections) {
				if (StringUtil::StartsWith(collection, "system.")) {
					continue;
				}
				filtered_collections.push_back(collection);
				collection_names.push_back(collection);
			}

			if (mongo_catalog && !filtered_collections.empty()) {
				mongo_catalog->CacheCollectionNames(database_name, filtered_collections);
			}
		} catch (...) {
			// Leave collection_names empty on error
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

	// Check if schema already exists
	auto it = schemas.find(info.schema);
	if (it != schemas.end()) {
		switch (info.on_conflict) {
		case OnCreateConflict::ERROR_ON_CONFLICT:
			throw CatalogException::EntryAlreadyExists(CatalogType::SCHEMA_ENTRY, info.schema);
		case OnCreateConflict::REPLACE_ON_CONFLICT:
			// Remove existing schema
			schemas.erase(it);
			break;
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return it->second.get();
		default:
			throw InternalException("Unsupported OnCreateConflict for CreateSchema");
		}
	}

	// Create new schema entry
	auto schema_entry = make_uniq<MongoSchemaEntry>(*this, info);
	auto result = schema_entry.get();
	schemas[info.schema] = shared_ptr<MongoSchemaEntry>(schema_entry.release());

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
	vector<string> databases;

	if (!database_name.empty()) {
		auto mongo_db = client[database_name];
		auto collections = mongo_db.list_collection_names();
		databases.push_back("");
	} else {
		auto db_list = client.list_database_names();
		for (const auto &db_name : db_list) {
			databases.push_back(db_name);
		}
	}

	auto system_transaction = CatalogTransaction::GetSystemTransaction(GetDatabase());
	string first_non_system_schema;

	// Create "main" schema when scanning all databases
	if (database_name.empty()) {
		CreateSchemaInfo main_schema_info;
		main_schema_info.schema = "main";
		main_schema_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		auto main_schema_entry = CreateSchema(system_transaction, main_schema_info);
		if (main_schema_entry) {
			auto &main_schema = main_schema_entry->Cast<MongoSchemaEntry>();
			// "main" schema doesn't correspond to a real MongoDB database, so use empty string
			auto default_generator =
			    make_uniq<MongoCollectionGenerator>(*this, main_schema, connection_string, "", this);
			main_schema.SetDefaultGenerator(std::move(default_generator));
			callback(main_schema);
		}
	}

	for (const auto &schema_name : databases) {
		if (database_name.empty() && (schema_name == "admin" || schema_name == "local" || schema_name == "config")) {
			continue;
		}

		string actual_schema_name;
		if (schema_name.empty() && !database_name.empty()) {
			actual_schema_name = database_name;
		} else if (schema_name.empty()) {
			actual_schema_name = "main";
		} else {
			actual_schema_name = schema_name;
		}

		if (first_non_system_schema.empty() && actual_schema_name != "admin" && actual_schema_name != "local" &&
		    actual_schema_name != "config") {
			first_non_system_schema = actual_schema_name;
		}

		CreateSchemaInfo schema_info;
		schema_info.schema = actual_schema_name;
		schema_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		auto schema_entry = CreateSchema(system_transaction, schema_info);
		if (schema_entry) {
			auto &schema = schema_entry->Cast<MongoSchemaEntry>();
			string generator_db_name = database_name.empty() ? schema_name : database_name;
			auto default_generator =
			    make_uniq<MongoCollectionGenerator>(*this, schema, connection_string, generator_db_name, this);
			schema.SetDefaultGenerator(std::move(default_generator));
			callback(schema);
		}
	}

	if (default_schema.empty()) {
		if (!database_name.empty()) {
			default_schema = database_name;
		} else {
			default_schema = "main";
		}
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
		schema_name = GetDefaultSchema();
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
		try {
			string mongo_db_name;
			if (!database_name.empty()) {
				if (schema_name == database_name) {
					mongo_db_name = database_name;
				}
			} else {
				mongo_db_name = schema_name;
			}

			if (!mongo_db_name.empty()) {
				CreateSchemaInfo schema_info;
				schema_info.schema = schema_name;
				schema_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
				auto system_transaction = CatalogTransaction::GetSystemTransaction(GetDatabase());
				auto schema_entry = CreateSchema(system_transaction, schema_info);
				if (schema_entry) {
					auto &schema_ref = schema_entry->Cast<MongoSchemaEntry>();
					auto default_generator =
					    make_uniq<MongoCollectionGenerator>(*this, schema_ref, connection_string, mongo_db_name, this);
					schema_ref.SetDefaultGenerator(std::move(default_generator));
					schema = &schema_ref;
				}
			}
		} catch (const std::exception &e) {
			// Fall through
		}
	}

	if (!schema && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	}

	return schema;
}

PhysicalOperator &MongoCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                  LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("CREATE TABLE AS is not supported for MongoDB catalogs");
}

PhysicalOperator &MongoCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                           optional_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("INSERT is not supported for MongoDB catalogs");
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
	auto it = schemas.find(info.name);
	if (it != schemas.end()) {
		schemas.erase(it);
	} else if (info.if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		throw CatalogException("Schema with name \"%s\" not found", info.name);
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
	cached->schema = info.schema;
	cached->view_name = info.view_name;
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

} // namespace duckdb
