#include "mongo_secrets.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"

namespace duckdb {

unique_ptr<SecretEntry> GetMongoSecret(ClientContext &context, const string &secret_name) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	// FIXME: this should be adjusted once the `GetSecretByName` API supports this use case
	auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "memory");
	if (secret_entry) {
		return secret_entry;
	}
	secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "local_file");
	if (secret_entry) {
		return secret_entry;
	}
	return nullptr;
}

string BuildMongoConnectionString(const KeyValueSecret &kv_secret, const string &attach_path) {
	// Extract values from secret
	string host = kv_secret.TryGetValue("host").IsNull() ? "localhost" : kv_secret.TryGetValue("host").ToString();
	string port = kv_secret.TryGetValue("port").IsNull() ? "27017" : kv_secret.TryGetValue("port").ToString();
	string user = kv_secret.TryGetValue("user").IsNull() ? "" : kv_secret.TryGetValue("user").ToString();
	string password = kv_secret.TryGetValue("password").IsNull() ? "" : kv_secret.TryGetValue("password").ToString();
	string database = kv_secret.TryGetValue("database").IsNull() ? "" : kv_secret.TryGetValue("database").ToString();
	string authsource =
	    kv_secret.TryGetValue("authsource").IsNull() ? "" : kv_secret.TryGetValue("authsource").ToString();
	string srv = kv_secret.TryGetValue("srv").IsNull() ? "" : kv_secret.TryGetValue("srv").ToString();

	// Check if using SRV connection (for MongoDB Atlas)
	bool use_srv = false;
	if (!srv.empty()) {
		string srv_lower = StringUtil::Lower(srv);
		use_srv = (srv_lower == "true" || srv_lower == "1" || srv_lower == "yes");
	}

	// Build MongoDB connection string
	string connection_string = use_srv ? "mongodb+srv://" : "mongodb://";
	if (!user.empty() || !password.empty()) {
		connection_string += user;
		if (!password.empty()) {
			connection_string += ":" + password;
		}
		connection_string += "@";
	}

	// For SRV connections, don't include the port (DNS handles it)
	if (use_srv) {
		connection_string += host;
	} else {
		connection_string += host + ":" + port;
	}

	if (!database.empty()) {
		connection_string += "/" + database;
	}

	// Build query parameters
	vector<string> query_params;
	if (!authsource.empty()) {
		query_params.push_back("authSource=" + authsource);
	}
	// Add common Atlas options when using SRV
	if (use_srv) {
		query_params.push_back("retryWrites=true");
		query_params.push_back("w=majority");
	}

	// If attach_path is a MongoDB URI, merge additional options from it
	// Otherwise, if attach_path contains key=value pairs, parse and add them
	if (!attach_path.empty()) {
		if (StringUtil::StartsWith(attach_path, "mongodb://") ||
		    StringUtil::StartsWith(attach_path, "mongodb+srv://")) {
			// Full MongoDB URI provided - extract query parameters and merge
			size_t query_start = attach_path.find('?');
			if (query_start != string::npos && query_start < attach_path.length() - 1) {
				string additional_params = attach_path.substr(query_start + 1);
				// Parse and add additional query parameters
				vector<string> additional_pairs = StringUtil::Split(additional_params, "&");
				for (const auto &pair : additional_pairs) {
					if (!pair.empty()) {
						query_params.push_back(pair);
					}
				}
			}
		} else {
			// Key=value format - parse and add as query parameters
			vector<string> pairs = StringUtil::Split(attach_path, " ");
			for (const auto &pair : pairs) {
				auto pos = pair.find('=');
				if (pos != string::npos && pos > 0) {
					string key = StringUtil::Lower(pair.substr(0, pos));
					string value = pair.substr(pos + 1);
					StringUtil::Trim(key);
					StringUtil::Trim(value);
					// Add as query parameter
					query_params.push_back(key + "=" + value);
				}
			}
		}
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

	return connection_string;
}

unique_ptr<BaseSecret> CreateMongoSecretFunction(ClientContext &context, CreateSecretInput &input) {
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, "mongo", "config", input.name);
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "host") {
			result->secret_map["host"] = named_param.second.ToString();
		} else if (lower_name == "user" || lower_name == "username") {
			result->secret_map["user"] = named_param.second.ToString();
		} else if (lower_name == "password") {
			result->secret_map["password"] = named_param.second.ToString();
		} else if (lower_name == "port") {
			result->secret_map["port"] = named_param.second.ToString();
		} else if (lower_name == "database" || lower_name == "dbname") {
			result->secret_map["database"] = named_param.second.ToString();
		} else if (lower_name == "authsource") {
			result->secret_map["authsource"] = named_param.second.ToString();
		} else if (lower_name == "srv") {
			result->secret_map["srv"] = named_param.second.ToString();
		} else {
			throw InternalException("Unknown named parameter passed to CreateMongoSecretFunction: " + lower_name);
		}
	}

	// Set redact keys
	result->redact_keys = {"password"};
	return std::move(result);
}

void SetMongoSecretParameters(CreateSecretFunction &function) {
	function.named_parameters["host"] = LogicalType::VARCHAR;
	function.named_parameters["port"] = LogicalType::VARCHAR;
	function.named_parameters["password"] = LogicalType::VARCHAR;
	function.named_parameters["user"] = LogicalType::VARCHAR;
	function.named_parameters["username"] = LogicalType::VARCHAR; // alias
	function.named_parameters["database"] = LogicalType::VARCHAR;
	function.named_parameters["dbname"] = LogicalType::VARCHAR; // alias
	function.named_parameters["authsource"] = LogicalType::VARCHAR;
	function.named_parameters["srv"] = LogicalType::VARCHAR;
}

} // namespace duckdb
