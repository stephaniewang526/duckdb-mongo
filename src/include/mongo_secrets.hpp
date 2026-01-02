#pragma once

#include "duckdb.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

// Helper function to lookup a MongoDB secret
unique_ptr<SecretEntry> GetMongoSecret(ClientContext &context, const string &secret_name);

// Helper function to build MongoDB connection string from secret
string BuildMongoConnectionString(const KeyValueSecret &kv_secret, const string &attach_path);

// Function to create MongoDB secret
unique_ptr<BaseSecret> CreateMongoSecretFunction(ClientContext &context, CreateSecretInput &input);

// Function to set MongoDB secret parameters
void SetMongoSecretParameters(CreateSecretFunction &function);

} // namespace duckdb
