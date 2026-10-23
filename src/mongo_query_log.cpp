#include "mongo_query_log.hpp"

#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/database.hpp"

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>

namespace duckdb {

string MongoQueryLogType::ConstructLogMessage(const string &database, const string &collection, const string &command,
                                              const bsoncxx::document::view &body) {
	bsoncxx::builder::basic::document doc;
	doc.append(bsoncxx::builder::basic::kvp("database", database));
	doc.append(bsoncxx::builder::basic::kvp("collection", collection));
	doc.append(bsoncxx::builder::basic::kvp(command, body));
	return bsoncxx::to_json(doc.view(), bsoncxx::ExtendedJsonMode::k_relaxed);
}

void RegisterMongoQueryLogType(DatabaseInstance &db) {
	auto &log_manager = db.GetLogManager();
	if (!log_manager.LookupLogType(MongoQueryLogType::NAME)) {
		log_manager.RegisterLogType(make_uniq<MongoQueryLogType>());
	}
}

} // namespace duckdb
