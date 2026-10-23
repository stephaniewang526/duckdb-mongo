#pragma once

#include "duckdb/logging/log_type.hpp"

#include <bsoncxx/document/view.hpp>

namespace duckdb {

class DatabaseInstance;

// Logs the find() and aggregate() commands a scan sends, as one JSON document per command. Enable with
// CALL enable_logging('MongoQuery') and read with SELECT message FROM duckdb_logs WHERE type = 'MongoQuery'.
class MongoQueryLogType : public LogType {
public:
	static constexpr const char *NAME = "MongoQuery";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	MongoQueryLogType() : LogType(NAME, LEVEL) {
	}

	// {"database": ..., "collection": ..., <command>: <body>}
	static string ConstructLogMessage(const string &database, const string &collection, const string &command,
	                                  const bsoncxx::document::view &body);
};

void RegisterMongoQueryLogType(DatabaseInstance &db);

} // namespace duckdb
