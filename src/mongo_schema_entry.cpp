#include "mongo_schema_entry.hpp"
#include "mongo_catalog.hpp"
#include "mongo_duckdb_utils.hpp"
#include "mongo_insert.hpp"
#include "mongo_instance.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/exception.hpp"
#include <mongocxx/client.hpp>
#include <mongocxx/exception/exception.hpp>
#include <algorithm>
namespace duckdb {

MongoSchemaEntry::MongoSchemaEntry(Catalog &catalog, CreateSchemaInfo &info) : SchemaCatalogEntry(catalog, info) {
}

optional_ptr<CatalogEntry> MongoSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                         const EntryLookupInfo &lookup_info) {
	if (lookup_info.GetCatalogType() != CatalogType::VIEW_ENTRY &&
	    lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}

	const auto &entry_name = lookup_info.GetEntryName();

	if (!is_loaded && transaction.context) {
		TryLoadEntries(*transaction.context);
	}

	lock_guard<mutex> lock(entry_lock);

	// A TABLE_ENTRY lookup (both SELECT and INSERT resolve this way) prefers a base table entry, so that INSERT binds.
	// Fall back to the view path when the collection has no inferable schema (empty / non-existent collection).
	if (lookup_info.GetCatalogType() == CatalogType::TABLE_ENTRY && transaction.context) {
		auto table_it = tables.find(entry_name);
		if (table_it != tables.end()) {
			return table_it->second.get();
		}
		// Skip on-demand creation for recently dropped collections.
		if (!dropped_tables.count(entry_name) && !connection_string.empty() && !database_name.empty()) {
			auto table_entry = MongoCreateTableEntry(catalog, *this, *transaction.context, connection_string,
			                                         database_name, entry_name);
			if (table_entry) {
				auto shared_entry = shared_ptr<CatalogEntry>(table_entry.release());
				tables[entry_name] = shared_entry;
				return shared_entry.get();
			}
		}
	}

	auto it = views.find(entry_name);
	if (it != views.end()) {
		return it->second.get();
	}

	// A recently dropped collection should not be re-created by the default generator.
	if (dropped_tables.count(entry_name)) {
		return nullptr;
	}

	if (default_generator && transaction.context) {
		auto default_entry = default_generator->CreateDefaultEntry(*transaction.context, Identifier(entry_name));
		if (default_entry) {
			auto view_entry = dynamic_cast<ViewCatalogEntry *>(default_entry.get());
			if (view_entry) {
				auto shared_entry = shared_ptr<CatalogEntry>(default_entry.release());
				views[entry_name] = shared_entry;
				return shared_entry.get();
			}
		}
	}

	return nullptr;
}

void MongoSchemaEntry::SetDefaultGenerator(unique_ptr<DefaultGenerator> generator) {
	lock_guard<mutex> lock(entry_lock);
	default_generator = std::move(generator);
}

void MongoSchemaEntry::SetConnectionInfo(const string &connection_string_p, const string &database_name_p) {
	lock_guard<mutex> lock(entry_lock);
	connection_string = connection_string_p;
	database_name = database_name_p;
}

optional_ptr<CatalogEntry> MongoSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	lock_guard<mutex> lock(entry_lock);

	auto view_name = info.GetViewName().GetIdentifierName();
	auto it = views.find(view_name);
	if (it != views.end()) {
		if (info.on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw CatalogException("View with name \"%s\" already exists", view_name);
		}
		return it->second.get();
	}

	auto view_entry = make_uniq<ViewCatalogEntry>(catalog, *this, info);
	auto result = view_entry.get();
	auto shared_entry = shared_ptr<CatalogEntry>(view_entry.release());
	views[view_name] = shared_entry;

	return result;
}

optional_ptr<CatalogEntry> MongoSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	throw NotImplementedException("CREATE TABLE is not supported for MongoDB catalogs");
}

optional_ptr<CatalogEntry> MongoSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw NotImplementedException("CREATE FUNCTION is not supported for MongoDB catalogs");
}

optional_ptr<CatalogEntry> MongoSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                         TableCatalogEntry &table) {
	throw NotImplementedException("CREATE INDEX is not supported for MongoDB catalogs");
}

optional_ptr<CatalogEntry> MongoSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw NotImplementedException("CREATE SEQUENCE is not supported for MongoDB catalogs");
}

optional_ptr<CatalogEntry> MongoSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                 CreateTableFunctionInfo &info) {
	throw NotImplementedException("CREATE TABLE FUNCTION is not supported for MongoDB catalogs");
}

optional_ptr<CatalogEntry> MongoSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                CreateCopyFunctionInfo &info) {
	throw NotImplementedException("CREATE COPY FUNCTION is not supported for MongoDB catalogs");
}

optional_ptr<CatalogEntry> MongoSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                  CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("CREATE PRAGMA FUNCTION is not supported for MongoDB catalogs");
}

optional_ptr<CatalogEntry> MongoSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                             CreateCollationInfo &info) {
	throw NotImplementedException("CREATE COLLATION is not supported for MongoDB catalogs");
}

optional_ptr<CatalogEntry> MongoSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("CREATE TYPE is not supported for MongoDB catalogs");
}

void MongoSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("ALTER is not supported for MongoDB catalogs");
}

void MongoSchemaEntry::TryLoadEntries(ClientContext &context) {
	if (is_loaded) {
		return;
	}

	if (!default_generator) {
		is_loaded = true;
		return;
	}

	lock_guard<mutex> load_guard(load_lock);
	if (is_loaded) {
		return;
	}

	// Listing errors (auth, network) propagate; is_loaded stays false so the
	// next access retries.
	vector<string> collection_names;
	auto entries = default_generator->GetDefaultEntries();
	collection_names.reserve(entries.size());
	for (auto &e : entries) {
		collection_names.push_back(e.GetIdentifierName());
	}

	lock_guard<mutex> entry_guard(entry_lock);
	loaded_collection_names = collection_names;
	is_loaded = true;
}

void MongoSchemaEntry::ClearDroppedEntry(const string &name) {
	lock_guard<mutex> lock(entry_lock);
	dropped_tables.erase(name);
}

void MongoSchemaEntry::InvalidateCache() {
	lock_guard<mutex> lock(entry_lock);
	lock_guard<mutex> load_guard(load_lock);
	is_loaded = false;
	loaded_collection_names.clear();
	views.clear();
	tables.clear();
}

shared_ptr<CatalogEntry> MongoSchemaEntry::GetOrCreateViewEntry(ClientContext &context, const string &collection_name) {
	lock_guard<mutex> lock(entry_lock);

	auto it = views.find(collection_name);
	if (it != views.end()) {
		return it->second;
	}

	if (default_generator) {
		try {
			auto default_entry = default_generator->CreateDefaultEntry(context, Identifier(collection_name));
			if (default_entry) {
				auto view_entry = dynamic_cast<ViewCatalogEntry *>(default_entry.get());
				if (view_entry) {
					auto shared_entry = shared_ptr<CatalogEntry>(default_entry.release());
					views[collection_name] = shared_entry;
					return shared_entry;
				}
			}
		} catch (...) {
			return nullptr;
		}
	}

	return nullptr;
}

void MongoSchemaEntry::Scan(ClientContext &context, CatalogType type,
                            const std::function<void(CatalogEntry &)> &callback) {
	if (type == CatalogType::VIEW_ENTRY || type == CatalogType::TABLE_ENTRY) {
		// Always reload entries to ensure fresh data
		is_loaded = false;
		if (default_generator) {
			TryLoadEntries(context);
		}

		vector<string> collections_to_remove;
		vector<string> collections_to_create;

		{
			lock_guard<mutex> lock(entry_lock);

			// Find entries that no longer exist in MongoDB
			for (const auto &[name, entry] : views) {
				bool found = false;
				for (const auto &collection_name : loaded_collection_names) {
					if (StringUtil::CIEquals(name, collection_name)) {
						found = true;
						break;
					}
				}
				if (!found) {
					collections_to_remove.push_back(name);
				}
			}

			// Remove stale entries
			for (const auto &name : collections_to_remove) {
				views.erase(name);
			}

			// Find new collections that need entries
			for (const auto &collection_name : loaded_collection_names) {
				if (views.find(collection_name) == views.end()) {
					collections_to_create.push_back(collection_name);
				}
			}

			// Callback existing entries
			for (auto &[name, entry] : views) {
				callback(*entry);
			}
		}

		// Create entries for new collections (outside lock since GetOrCreateViewEntry acquires it)
		for (const auto &collection_name : collections_to_create) {
			auto entry = GetOrCreateViewEntry(context, collection_name);
			if (entry) {
				callback(*entry);
			}
		}
	}
}

void MongoSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	if (type == CatalogType::VIEW_ENTRY) {
		lock_guard<mutex> lock(entry_lock);
		for (auto &[name, entry] : views) {
			callback(*entry);
		}
	}
}

void MongoSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	auto drop_name = info.GetQualifiedName().Name().GetIdentifierName();
	if (info.type == CatalogType::VIEW_ENTRY) {
		lock_guard<mutex> lock(entry_lock);
		auto it = views.find(drop_name);
		if (it != views.end()) {
			views.erase(it);
		} else if (info.if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
			throw CatalogException("View with name \"%s\" not found", drop_name);
		}
	} else if (info.type == CatalogType::TABLE_ENTRY) {
		// Drop the underlying MongoDB collection and evict from all caches.
		if (!connection_string.empty() && !database_name.empty()) {
			GetMongoInstance();
			auto client = mongocxx::client(mongocxx::uri(connection_string));
			client[database_name][drop_name].drop();

			auto &mongo_catalog = catalog.Cast<MongoCatalog>();
			mongo_catalog.InvalidateCollectionNamesCache(database_name);
			mongo_catalog.InvalidateViewInfoCache(database_name, drop_name);
		}
		lock_guard<mutex> lock(entry_lock);
		tables.erase(drop_name);
		views.erase(drop_name);
		dropped_tables.insert(drop_name);
		loaded_collection_names.erase(
		    std::remove(loaded_collection_names.begin(), loaded_collection_names.end(), drop_name),
		    loaded_collection_names.end());
	} else {
		throw NotImplementedException("DROP is not supported for this entry type in MongoDB catalogs");
	}
}

} // namespace duckdb
