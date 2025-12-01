//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mongo_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "mongo_catalog.hpp"
#include "mongo_transaction.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {

// Transaction manager for MongoDB.
// Manages the lifecycle of MongoDB transactions.
class MongoTransactionManager : public TransactionManager {
public:
	MongoTransactionManager(AttachedDatabase &db_p, MongoCatalog &mongo_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	MongoCatalog &mongo_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<MongoTransaction>> transactions;
};

} // namespace duckdb

