#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/default/default_generator.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include <memory>

namespace duckdb {

class MongoCatalog;
class MongoCollectionGenerator;

// Minimal schema entry for MongoDB that supports default generators for views
class MongoSchemaEntry : public SchemaCatalogEntry {
public:
	MongoSchemaEntry(Catalog &catalog, CreateSchemaInfo &info);

	// Override LookupEntry to support default generators
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

	// Set default generator for views (collections)
	void SetDefaultGenerator(unique_ptr<DefaultGenerator> generator);

	// Required SchemaCatalogEntry methods
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;

private:
	void TryLoadEntries(ClientContext &context);
	// Helper to create view entry lazily
	shared_ptr<CatalogEntry> GetOrCreateViewEntry(ClientContext &context, const string &collection_name);

	mutex entry_lock;
	mutex load_lock; // Separate lock for loading to prevent deadlocks
	case_insensitive_map_t<shared_ptr<CatalogEntry>> views;
	unique_ptr<DefaultGenerator> default_generator;
	atomic<bool> is_loaded = false;         // Track if collections have been loaded
	vector<string> loaded_collection_names; // Collection names loaded from MongoDB (for lazy view creation)
};

} // namespace duckdb
