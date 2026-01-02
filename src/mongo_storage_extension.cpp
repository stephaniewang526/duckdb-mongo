#include "mongo_storage_extension.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "mongo_catalog.hpp"
#include "mongo_transaction_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

unique_ptr<Catalog> MongoStorageAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                       AttachedDatabase &db, const string &name, AttachInfo &info,
                                       AttachOptions &attach_options) {
	// Extract secret option from attach_options
	string secret_name;
	for (auto &entry : attach_options.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "secret") {
			secret_name = entry.second.ToString();
		} else {
			throw BinderException("Unrecognized option for Mongo attach: %s", entry.first);
		}
	}

	// Get connection string (with secret support)
	string attach_path = info.path;
	string connection_string = MongoCatalog::GetConnectionString(context, attach_path, secret_name);
	string database_name = ""; // Specific database to use (empty means all databases)

	// If the path doesn't start with "mongodb://", it's likely a key=value format
	// Parse it and convert to MongoDB connection string format
	if (!StringUtil::StartsWith(connection_string, "mongodb://") &&
	    !StringUtil::StartsWith(connection_string, "mongodb+srv://")) {
		// Parse key=value pairs
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

		// Build MongoDB connection string.
		string host = params.count("host") ? params["host"] : "localhost";
		string port = params.count("port") ? params["port"] : "27017";
		// Support both "dbname" and "database" parameters
		if (params.count("dbname")) {
			database_name = params["dbname"];
		} else if (params.count("database")) {
			database_name = params["database"];
		} else {
			database_name = "";
		}
		string username = params.count("user") ? params["user"] : "";
		string password = params.count("password") ? params["password"] : "";
		string auth_source = params.count("authsource") ? params["authsource"] : "";

		// Check if using SRV connection (for MongoDB Atlas).
		bool use_srv = false;
		if (params.count("srv")) {
			string srv_value = StringUtil::Lower(params["srv"]);
			use_srv = (srv_value == "true" || srv_value == "1" || srv_value == "yes");
		}

		// Build connection string.
		connection_string = use_srv ? "mongodb+srv://" : "mongodb://";
		if (!username.empty() || !password.empty()) {
			connection_string += username;
			if (!password.empty()) {
				connection_string += ":" + password;
			}
			connection_string += "@";
		}
		// For SRV connections, don't include the port (DNS handles it).
		if (use_srv) {
			connection_string += host;
		} else {
			connection_string += host + ":" + port;
		}
		if (!database_name.empty()) {
			connection_string += "/" + database_name;
		}

		// Build query parameters.
		vector<string> query_params;
		if (!auth_source.empty()) {
			query_params.push_back("authSource=" + auth_source);
		}
		// Add common Atlas options when using SRV.
		if (use_srv) {
			query_params.push_back("retryWrites=true");
			query_params.push_back("w=majority");
		}
		// Add any additional options from the options parameter.
		if (params.count("options")) {
			query_params.push_back(params["options"]);
		}
		if (!query_params.empty()) {
			connection_string += "?";
			for (size_t i = 0; i < query_params.size(); i++) {
				if (i > 0) {
					connection_string += "&";
				}
				connection_string += query_params[i];
			}
		}
	} else {
		// Extract database name from MongoDB URI if present.
		// Format: mongodb://[user:pass@]host[:port]/database[?options]
		// or: mongodb+srv://[user:pass@]host/database[?options]
		// We need to find the path component after the host, not just any '/'.

		// First, find where the host part starts (after "://" and optionally after "@").
		size_t scheme_end = connection_string.find("://");
		if (scheme_end != string::npos) {
			size_t host_start = scheme_end + 3; // Skip "://"

			// If there's an "@", the host starts after it (credentials are before).
			size_t at_pos = connection_string.find('@', host_start);
			if (at_pos != string::npos) {
				host_start = at_pos + 1;
			}

			// Now find the "/" after the host (which would indicate a database path).
			size_t db_start = connection_string.find('/', host_start);
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
	}

	// Create MongoDB catalog.
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
