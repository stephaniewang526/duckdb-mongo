#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "mongo_instance.hpp"
#include "mongo_schema_entry.hpp"
#include <mongocxx/client.hpp>

namespace duckdb {

class MongoCatalog : public Catalog {
public:
	explicit MongoCatalog(AttachedDatabase &db, const string &connection_string, const string &database_name = "");

	string connection_string;
	string database_name;  // Specific database to use (empty means all databases)
	string default_schema; // Default schema name (set during ScanSchemas)

	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "mongo";
	}

	// Override to list MongoDB databases as schemas
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	// Override LookupSchema to ensure schemas are found correctly
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	// Physical plan methods - MongoDB is read-only for now
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	void DropSchema(ClientContext &context, DropInfo &info) override;

	// Clear cache to force refresh
	void ClearCache();

	// Override to prevent accessing non-existent storage manager.
	bool InMemory() override {
		return false;
	}

	// Override to prevent accessing non-existent storage manager.
	string GetDBPath() override {
		return connection_string;
	}

	// Override to prevent accessing non-existent storage manager.
	DatabaseSize GetDatabaseSize(ClientContext &context) override {
		DatabaseSize size;
		size.free_blocks = 0;
		size.total_blocks = 0;
		size.used_blocks = 0;
		size.wal_size = 0;
		size.block_size = 0;
		size.bytes = 0;
		return size;
	}

	// Override to prevent accessing non-existent storage manager.
	bool IsEncrypted() const override {
		return false;
	}

	// Override to prevent accessing non-existent storage manager.
	string GetEncryptionCipher() const override {
		return string();
	}

	string GetDefaultSchema() const override {
		if (!default_schema.empty()) {
			return default_schema;
		}
		return database_name;
	}

	// Get MongoDB client
	mongocxx::client GetClient() const {
		GetMongoInstance(); // Ensure instance is initialized
		return mongocxx::client(mongocxx::uri(connection_string));
	}

	// Get cached collection names for a database (shared across schemas)
	vector<string> GetCachedCollectionNames(const string &db_name) const;
	// Cache collection names for a database
	void CacheCollectionNames(const string &db_name, const vector<string> &collections);
	// Invalidate collection names cache for a database (forces refresh on next access)
	void InvalidateCollectionNamesCache(const string &db_name);

	// Get cached CreateViewInfo for a collection (to avoid re-parsing SQL)
	shared_ptr<CreateViewInfo> GetCachedViewInfo(const string &db_name, const string &collection_name) const;
	// Cache CreateViewInfo for a collection
	void CacheViewInfo(const string &db_name, const string &collection_name, const CreateViewInfo &info);
	// Invalidate CreateViewInfo cache for a collection
	void InvalidateViewInfoCache(const string &db_name, const string &collection_name);

private:
	mutable mutex schemas_lock;
	unordered_map<string, shared_ptr<MongoSchemaEntry>> schemas;
	bool schemas_scanned;
	// Cache collection names per database (shared across schemas)
	mutable mutex collection_cache_lock;
	unordered_map<string, vector<string>> collection_cache; // Key: database_name, Value: collection names
	
	// Cache parsed CreateViewInfo per collection to avoid re-parsing SQL
	mutable mutex view_info_cache_lock;
	unordered_map<string, shared_ptr<CreateViewInfo>> view_info_cache; // Key: "db_name:collection_name", Value: cached CreateViewInfo
};

} // namespace duckdb
