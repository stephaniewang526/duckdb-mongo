#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/common/mutex.hpp"
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

	// Get default schema name (similar to Postgres extension)
	string GetDefaultSchema() const override {
		if (!default_schema.empty()) {
			return default_schema;
		}
		// If no default schema set yet, return database_name if specified, or empty
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

private:
	mutable mutex schemas_lock;
	unordered_map<string, shared_ptr<MongoSchemaEntry>> schemas;
	bool schemas_scanned;
	// Cache collection names per database (shared across schemas)
	mutable mutex collection_cache_lock;
	unordered_map<string, vector<string>> collection_cache; // Key: database_name, Value: collection names
};

} // namespace duckdb
