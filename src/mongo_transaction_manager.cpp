#include "mongo_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

MongoTransactionManager::MongoTransactionManager(AttachedDatabase &db_p, MongoCatalog &mongo_catalog)
    : TransactionManager(db_p), mongo_catalog(mongo_catalog) {
}

Transaction &MongoTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<MongoTransaction>(mongo_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData MongoTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &mongo_transaction = transaction.Cast<MongoTransaction>();
	mongo_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void MongoTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &mongo_transaction = transaction.Cast<MongoTransaction>();
	mongo_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void MongoTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// MongoDB doesn't have a checkpoint concept like traditional SQL databases.
	// This is a no-op.
}

} // namespace duckdb

