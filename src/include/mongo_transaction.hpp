//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mongo_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
class MongoCatalog;

// MongoDB transaction state.
enum class MongoTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

// MongoDB transaction implementation.
// MongoDB doesn't support traditional ACID transactions in the same way as SQL databases,
// so this is a simplified implementation for read-only operations.
class MongoTransaction : public Transaction {
public:
	MongoTransaction(MongoCatalog &mongo_catalog, TransactionManager &manager, ClientContext &context);
	~MongoTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	static MongoTransaction &Get(ClientContext &context, Catalog &catalog);

private:
	MongoTransactionState transaction_state;
};

} // namespace duckdb
