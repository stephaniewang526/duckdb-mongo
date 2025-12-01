#include "mongo_catalog.hpp"
#include "mongo_instance.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/catalog/default/default_views.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include <mongocxx/exception/exception.hpp>

namespace duckdb {

// Default generator for MongoDB collections (creates views dynamically)
class MongoCollectionGenerator : public DefaultGenerator {
public:
	MongoCollectionGenerator(Catalog &catalog, SchemaCatalogEntry &schema, const string &connection_string_p,
	                         const string &database_name_p)
	    : DefaultGenerator(catalog), schema(schema), connection_string(connection_string_p),
	      database_name(database_name_p) {
		// Ensure MongoDB instance is initialized before creating any clients
		GetMongoInstance();
		// List collections for this database
		try {
			mongocxx::uri uri(connection_string);
			mongocxx::client client(uri);
			auto mongo_db = client[database_name];
			auto collections = mongo_db.list_collection_names();
			for (const auto &collection_name : collections) {
				collection_names.push_back(collection_name);
			}
		} catch (const std::exception &e) {
			// If we can't connect, leave collection_names empty
			// Error will occur when trying to access
		}
	}

public:
	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override {
		// Check if entry_name is a collection
		for (const auto &collection_name : collection_names) {
			if (StringUtil::CIEquals(entry_name, collection_name)) {
				// Create a view that uses mongo_scan
				auto result = make_uniq<CreateViewInfo>();
				result->schema = schema.name;
				result->view_name = collection_name;
				// Escape single quotes in strings
				auto escape_sql_string = [](const string &str) -> string {
					string result;
					for (char c : str) {
						if (c == '\'') {
							result += "''";
						} else {
							result += c;
						}
					}
					return result;
				};
				result->sql = StringUtil::Format("SELECT * FROM mongo_scan('%s', '%s', '%s')",
				                                 escape_sql_string(connection_string), escape_sql_string(database_name),
				                                 escape_sql_string(collection_name));
				auto view_info = CreateViewInfo::FromSelect(context, std::move(result));
				return make_uniq_base<CatalogEntry, ViewCatalogEntry>(catalog, schema, *view_info);
			}
		}
		return nullptr;
	}

	vector<string> GetDefaultEntries() override {
		return collection_names;
	}

private:
	SchemaCatalogEntry &schema;
	string connection_string;
	string database_name;
	vector<string> collection_names;
};

MongoCatalog::MongoCatalog(AttachedDatabase &db, const string &connection_string, const string &database_name)
    : DuckCatalog(db), connection_string(connection_string), database_name(database_name) {
	// Ensure MongoDB instance is initialized
	GetMongoInstance();
}

void MongoCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	// List MongoDB databases as schemas
	try {
		auto client = GetClient();
		vector<string> databases;

		// If a specific database is specified, use the default schema (empty string = main/default)
		// Otherwise, list all databases as separate schemas
		if (!database_name.empty()) {
			// Verify the database exists by trying to list its collections
			try {
				auto mongo_db = client[database_name];
				auto collections = mongo_db.list_collection_names();
				// Use empty string to indicate default schema
				databases.push_back("");
			} catch (const std::exception &e) {
				// Database doesn't exist or can't access it
				// Return empty list - this will allow ATTACH to succeed but queries will fail
				return;
			}
		} else {
			// List all databases
			auto db_list = client.list_database_names();
			for (const auto &db_name : db_list) {
				databases.push_back(db_name);
			}
		}

		auto system_transaction = CatalogTransaction::GetSystemTransaction(db.GetDatabase());

		for (const auto &schema_name : databases) {
			// Skip system databases (unless specifically requested via dbname)
			if (database_name.empty() &&
			    (schema_name == "admin" || schema_name == "local" || schema_name == "config")) {
				continue;
			}

			// Create or get schema
			// If schema_name is empty and dbname is specified, use the database name as schema name
			// This ensures the schema is created in our catalog and is unique
			string actual_schema_name;
			if (schema_name.empty() && !database_name.empty()) {
				// When dbname is specified, use the database name as the schema name
				// This ensures the schema is in our MongoCatalog and collections are accessible
				actual_schema_name = database_name;
			} else if (schema_name.empty()) {
				// No dbname specified and empty schema - shouldn't happen, but use "main" as fallback
				actual_schema_name = "main";
			} else {
				actual_schema_name = schema_name;
			}

			CreateSchemaInfo schema_info;
			schema_info.schema = actual_schema_name;
			schema_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
			auto schema_entry = CreateSchema(system_transaction, schema_info);
			if (schema_entry) {
				auto &schema = schema_entry->Cast<SchemaCatalogEntry>();
				// Set up default generator for collections in this schema
				// When dbname is specified, use that database name for the generator
				string generator_db_name = database_name.empty() ? schema_name : database_name;
				auto &duck_schema = schema.Cast<DuckSchemaEntry>();
				auto &catalog_set = duck_schema.GetCatalogSet(CatalogType::VIEW_ENTRY);

				// Always set the default generator, even if schema already existed
				// This ensures collections are accessible
				auto default_generator =
				    make_uniq<MongoCollectionGenerator>(*this, schema, connection_string, generator_db_name);
				catalog_set.SetDefaultGenerator(std::move(default_generator));

				callback(schema);
			}
		}
	} catch (const std::exception &e) {
		// If we can't connect, return empty list
		// This allows ATTACH to succeed even if MongoDB is not available
		// The error will occur when trying to query
	}
}

optional_ptr<SchemaCatalogEntry> MongoCatalog::LookupSchema(CatalogTransaction transaction,
                                                            const EntryLookupInfo &schema_lookup,
                                                            OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();

	// If looking for default schema and dbname is specified, use the database name
	if (schema_name.empty() && !database_name.empty()) {
		schema_name = database_name;
	}

	// Try to find the schema first
	auto schema = DuckCatalog::LookupSchema(transaction, EntryLookupInfo(CatalogType::SCHEMA_ENTRY, schema_name),
	                                        OnEntryNotFound::RETURN_NULL);

	// If schema not found and dbname is specified, create it on-demand
	if (!schema && !database_name.empty() && schema_name == database_name) {
		try {
			CreateSchemaInfo schema_info;
			schema_info.schema = schema_name;
			schema_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
			auto system_transaction = CatalogTransaction::GetSystemTransaction(db.GetDatabase());
			auto schema_entry = CreateSchema(system_transaction, schema_info);
			if (schema_entry) {
				auto &schema_ref = schema_entry->Cast<SchemaCatalogEntry>();
				// Set up default generator for collections in this schema
				string generator_db_name = database_name;
				auto &duck_schema = schema_ref.Cast<DuckSchemaEntry>();
				auto &catalog_set = duck_schema.GetCatalogSet(CatalogType::VIEW_ENTRY);
				auto default_generator =
				    make_uniq<MongoCollectionGenerator>(*this, schema_ref, connection_string, generator_db_name);
				catalog_set.SetDefaultGenerator(std::move(default_generator));
				schema = &schema_ref;
			}
		} catch (const std::exception &e) {
			// If we can't create the schema, fall through to return null or throw
		}
	}

	if (!schema && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	}

	return schema;
}

} // namespace duckdb
