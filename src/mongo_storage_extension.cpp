#include "mongo_storage_extension.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "mongo_catalog.hpp"
#include "mongo_transaction_manager.hpp"

namespace duckdb {

unique_ptr<Catalog> MongoStorageAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                       AttachedDatabase &db, const string &name, AttachInfo &info,
                                       AttachOptions &attach_options) {
	// Parse connection string from info.path
	string connection_string = info.path;
	string database_name = ""; // Specific database to use (empty means all databases)
	
	// If the path doesn't start with "mongodb://", it's likely a key=value format
	// Parse it and convert to MongoDB connection string format
	if (!StringUtil::StartsWith(connection_string, "mongodb://") && 
	    !StringUtil::StartsWith(connection_string, "mongodb+srv://")) {
		// Parse key=value pairs (similar to Postgres libpq format)
		unordered_map<string, string> params;
		vector<string> pairs = StringUtil::Split(connection_string, " ");
		
		for (const auto &pair : pairs) {
			auto pos = pair.find('=');
			if (pos != string::npos && pos > 0) {
				string key_str = pair.substr(0, pos);
				string value_str = pair.substr(pos + 1);
				StringUtil::Trim(key_str);
				StringUtil::Trim(value_str);
				string key = StringUtil::Lower(key_str);
				params[key] = value_str;
			}
		}
		
		// Build MongoDB connection string
		string host = params.count("host") ? params["host"] : "localhost";
		string port = params.count("port") ? params["port"] : "27017";
		database_name = params.count("dbname") ? params["dbname"] : "";
		string username = params.count("user") ? params["user"] : "";
		string password = params.count("password") ? params["password"] : "";
		string auth_source = params.count("authsource") ? params["authsource"] : "";
		
		// Build connection string
		connection_string = "mongodb://";
		if (!username.empty() || !password.empty()) {
			connection_string += username;
			if (!password.empty()) {
				connection_string += ":" + password;
			}
			connection_string += "@";
		}
		connection_string += host + ":" + port;
		if (!database_name.empty()) {
			connection_string += "/" + database_name;
		}
		if (!auth_source.empty()) {
			connection_string += "?authSource=" + auth_source;
		}
	} else {
		// Extract database name from MongoDB URI if present
		// Format: mongodb://host:port/database
		size_t db_start = connection_string.find_last_of('/');
		if (db_start != string::npos && db_start < connection_string.length() - 1) {
			size_t db_end = connection_string.find_first_of('?', db_start + 1);
			if (db_end == string::npos) {
				db_end = connection_string.length();
			}
			if (db_end > db_start + 1) {
				database_name = connection_string.substr(db_start + 1, db_end - db_start - 1);
			}
		}
	}
	
	// Create MongoDB catalog
	auto catalog = make_uniq<MongoCatalog>(db, connection_string, database_name);
	catalog->Initialize(false);
	
	return std::move(catalog);
}

unique_ptr<TransactionManager> MongoStorageTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                              AttachedDatabase &db, Catalog &catalog) {
	auto &mongo_catalog = catalog.Cast<MongoCatalog>();
	return make_uniq<MongoTransactionManager>(db, mongo_catalog);
}

unique_ptr<StorageExtension> MongoStorageExtension::Create() {
	auto result = make_uniq<StorageExtension>();
	result->attach = MongoStorageAttach;
	result->create_transaction_manager = MongoStorageTransactionManager;
	return result;
}

} // namespace duckdb

