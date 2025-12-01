#include "mongo_transaction.hpp"
#include "mongo_catalog.hpp"

namespace duckdb {

MongoTransaction::MongoTransaction(MongoCatalog &mongo_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context) {
}

MongoTransaction::~MongoTransaction() = default;

void MongoTransaction::Start() {
	transaction_state = MongoTransactionState::TRANSACTION_NOT_YET_STARTED;
}

void MongoTransaction::Commit() {
	if (transaction_state == MongoTransactionState::TRANSACTION_STARTED) {
		transaction_state = MongoTransactionState::TRANSACTION_FINISHED;
		// MongoDB doesn't require explicit commit for read operations.
	}
}

void MongoTransaction::Rollback() {
	if (transaction_state == MongoTransactionState::TRANSACTION_STARTED) {
		transaction_state = MongoTransactionState::TRANSACTION_FINISHED;
		// MongoDB doesn't require explicit rollback for read operations.
	}
}

MongoTransaction &MongoTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<MongoTransaction>();
}

} // namespace duckdb
