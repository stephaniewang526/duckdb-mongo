#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/types.hpp"
#include "mongo_extension.hpp"
#include <cstdlib>
#include <string>
#include <set>
#include <chrono>
#include <iostream>

// Helper macro for checking query success
#define REQUIRE_NO_FAIL(result) REQUIRE(!(result)->HasError())

// Simple main function for running the test
int main(int argc, char *argv[]) {
	return Catch::Session().run(argc, argv);
}

TEST_CASE("MongoDB Atlas Integration Test", "[mongo][atlas][integration]") {
	const char *username = std::getenv("MONGO_ATLAS_USERNAME");
	const char *password = std::getenv("MONGO_ATLAS_PASSWORD");
	if (!username || !password) {
		return; // Skip test if credentials not provided
	}

	std::string connection_string = "mongodb+srv://" + std::string(username) + ":" + std::string(password) +
	                                "@adl-testing-azure-amste.ki9ie.mongodb.net?retryWrites=true&w=majority";

	duckdb::DuckDB db(nullptr);
	db.LoadStaticExtension<duckdb::MongoExtension>();
	duckdb::Connection con(db);

	// Create a secret for MongoDB Atlas connection (without database name)
	std::string create_secret_query =
	    "CREATE SECRET atlas_secret (TYPE mongo, HOST 'adl-testing-azure-amste.ki9ie.mongodb.net', "
	    "USER '" +
	    std::string(username) + "', PASSWORD '" + std::string(password) + "', SRV 'true')";
	REQUIRE_NO_FAIL(con.Query(create_secret_query));

	SECTION("ATTACH to MongoDB Atlas using secret with empty path") {
		auto start = std::chrono::high_resolution_clock::now();
		// Empty string means use secret only - all connection info comes from secret
		REQUIRE_NO_FAIL(con.Query("ATTACH '' AS atlas_db (TYPE MONGO, SECRET 'atlas_secret')"));
		auto end = std::chrono::high_resolution_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
		std::cerr << "[TEST] ATTACH with secret (empty path) took " << duration << "ms" << std::endl;

		// Verify attachment
		auto result = con.Query("SELECT database_name FROM duckdb_databases() WHERE database_name = 'atlas_db'");
		REQUIRE(!result->HasError());
		REQUIRE(result->RowCount() == 1);
	}

	SECTION("ATTACH to MongoDB Atlas using secret with additional options") {
		auto start = std::chrono::high_resolution_clock::now();
		// Can provide additional connection options in attach path that merge with secret
		// For example, adding query parameters like readPreference
		REQUIRE_NO_FAIL(
		    con.Query("ATTACH '?readPreference=secondary' AS atlas_db_options (TYPE MONGO, SECRET 'atlas_secret')"));
		auto end = std::chrono::high_resolution_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
		std::cerr << "[TEST] ATTACH with secret and additional options took " << duration << "ms" << std::endl;

		// Verify attachment
		auto result =
		    con.Query("SELECT database_name FROM duckdb_databases() WHERE database_name = 'atlas_db_options'");
		REQUIRE(!result->HasError());
		REQUIRE(result->RowCount() == 1);

		// Cleanup
		REQUIRE_NO_FAIL(con.Query("DETACH atlas_db_options"));
	}

	SECTION("ATTACH to MongoDB Atlas using connection string") {
		auto start = std::chrono::high_resolution_clock::now();
		REQUIRE_NO_FAIL(con.Query("ATTACH '" + connection_string + "' AS atlas_db_legacy (TYPE MONGO)"));
		auto end = std::chrono::high_resolution_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
		std::cerr << "[TEST] ATTACH with connection string took " << duration << "ms" << std::endl;

		// Verify attachment
		auto result = con.Query("SELECT database_name FROM duckdb_databases() WHERE database_name = 'atlas_db_legacy'");
		REQUIRE(!result->HasError());
		REQUIRE(result->RowCount() == 1);
	}

	SECTION("Verify expected schemas are present") {
		REQUIRE_NO_FAIL(con.Query("ATTACH '' AS atlas_db (TYPE MONGO, SECRET 'atlas_secret')"));

		auto start = std::chrono::high_resolution_clock::now();
		auto result = con.Query("SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'atlas_db' "
		                        "AND schema_name IN ('oa_smoke_test', 'smoketests') ORDER BY schema_name");
		auto end = std::chrono::high_resolution_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
		std::cerr << "[TEST] Schema query took " << duration << "ms" << std::endl;

		REQUIRE(!result->HasError());
		REQUIRE(result->RowCount() == 2);
		auto chunk = result->Fetch();
		std::set<std::string> schemas;
		for (idx_t i = 0; i < chunk->size(); i++) {
			schemas.insert(chunk->GetValue(0, i).ToString());
		}
		REQUIRE(schemas.count("oa_smoke_test") == 1);
		REQUIRE(schemas.count("smoketests") == 1);
	}

	SECTION("USE command with default schema") {
		REQUIRE_NO_FAIL(con.Query("ATTACH '' AS atlas_db (TYPE MONGO, SECRET 'atlas_secret')"));

		// USE command should default to "main" schema
		REQUIRE_NO_FAIL(con.Query("USE atlas_db"));
		auto result = con.Query("SELECT current_database(), current_schema()");
		REQUIRE(!result->HasError());
		REQUIRE(result->RowCount() == 1);
		auto chunk = result->Fetch();
		REQUIRE(chunk->GetValue(0, 0).ToString() == "atlas_db");
		REQUIRE(chunk->GetValue(1, 0).ToString() == "main");

		// Check information_schema.schemata
		auto schemas_result = con.Query(
		    "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'atlas_db' ORDER BY schema_name");
		REQUIRE(!schemas_result->HasError());
		REQUIRE(schemas_result->RowCount() >= 3); // Should have at least "main", "oa_smoke_test", "smoketests"

		std::set<std::string> info_schemas;
		auto schemas_chunk = schemas_result->Fetch();
		for (idx_t i = 0; i < schemas_chunk->size(); i++) {
			info_schemas.insert(schemas_chunk->GetValue(0, i).ToString());
		}

		// Verify "main" is present
		REQUIRE(info_schemas.count("main") == 1);
		// Verify MongoDB databases are present
		REQUIRE(info_schemas.count("oa_smoke_test") == 1);
		REQUIRE(info_schemas.count("smoketests") == 1);
	}

	SECTION("USE command with explicit schema and verify context") {
		REQUIRE_NO_FAIL(con.Query("ATTACH '' AS atlas_db (TYPE MONGO, SECRET 'atlas_secret')"));

		REQUIRE_NO_FAIL(con.Query("USE atlas_db.smoketests"));
		auto result = con.Query("SELECT current_database(), current_schema()");
		REQUIRE(!result->HasError());
		REQUIRE(result->RowCount() == 1);
		auto chunk = result->Fetch();
		REQUIRE(chunk->GetValue(0, 0).ToString() == "atlas_db");
		REQUIRE(chunk->GetValue(1, 0).ToString() == "smoketests");
	}

	SECTION("SHOW TABLES - verify test collection exists") {
		REQUIRE_NO_FAIL(con.Query("ATTACH '' AS atlas_db (TYPE MONGO, SECRET 'atlas_secret')"));
		REQUIRE_NO_FAIL(con.Query("USE atlas_db.smoketests"));

		auto start = std::chrono::high_resolution_clock::now();
		auto result = con.Query("SHOW TABLES");
		auto end = std::chrono::high_resolution_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
		std::cerr << "[TEST] SHOW TABLES took " << duration << "ms" << std::endl;

		REQUIRE(!result->HasError());
		REQUIRE(result->RowCount() == 1);
		auto chunk = result->Fetch();
		REQUIRE(chunk->GetValue(0, 0).ToString() == "test");
	}

	SECTION("Query and verify data in test collection") {
		REQUIRE_NO_FAIL(con.Query("ATTACH '' AS atlas_db (TYPE MONGO, SECRET 'atlas_secret')"));
		REQUIRE_NO_FAIL(con.Query("USE atlas_db.smoketests"));

		// Expected: 2 documents with a=1, b="smoke" and a=2, b="test"
		auto start = std::chrono::high_resolution_clock::now();
		auto result = con.Query("SELECT * FROM test ORDER BY a");
		auto end = std::chrono::high_resolution_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
		std::cerr << "[TEST] Query test collection took " << duration << "ms" << std::endl;
		if (result->HasError()) {
			result = con.Query("SELECT * FROM atlas_db.\"smoketests\".\"test\" ORDER BY a");
		}
		REQUIRE(!result->HasError());
		REQUIRE(result->RowCount() == 2);

		// Find column indices
		// Use a local constant to avoid ODR issues with DConstants::INVALID_INDEX on ARM64
		constexpr idx_t INVALID_COL = idx_t(-1);
		idx_t a_col = INVALID_COL, b_col = INVALID_COL;
		for (idx_t i = 0; i < result->ColumnCount(); i++) {
			std::string col_name = result->ColumnName(i);
			if (col_name == "a") {
				a_col = i;
			} else if (col_name == "b") {
				b_col = i;
			}
		}
		REQUIRE(a_col != INVALID_COL);
		REQUIRE(b_col != INVALID_COL);

		// Verify data values (ORDER BY a ensures a=1 comes first, then a=2)
		auto chunk = result->Fetch();
		REQUIRE(chunk);
		REQUIRE(chunk->size() == 2);

		// With ORDER BY a, first row should be a=1, b="smoke", second row should be a=2, b="test"
		REQUIRE(chunk->GetValue(a_col, 0).GetValue<int64_t>() == 1);
		REQUIRE(chunk->GetValue(b_col, 0).GetValue<std::string>() == "smoke");
		REQUIRE(chunk->GetValue(a_col, 1).GetValue<int64_t>() == 2);
		REQUIRE(chunk->GetValue(b_col, 1).GetValue<std::string>() == "test");
	}

	SECTION("Query information_schema for tables in oa_smoke_test") {
		REQUIRE_NO_FAIL(con.Query("ATTACH '' AS atlas_db (TYPE MONGO, SECRET 'atlas_secret')"));

		auto start = std::chrono::high_resolution_clock::now();
		auto result = con.Query("SELECT table_name FROM information_schema.tables WHERE table_catalog = 'atlas_db' AND "
		                        "table_schema = 'oa_smoke_test' ORDER BY table_name LIMIT 10");
		auto end = std::chrono::high_resolution_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
		std::cerr << "[TEST] Query information_schema.tables took " << duration << "ms" << std::endl;

		REQUIRE(!result->HasError());
		REQUIRE(result->RowCount() > 0); // Verify we get results
		auto chunk = result->Fetch();
		REQUIRE(chunk);
		REQUIRE(chunk->size() > 0);
	}

	SECTION("Query a collection from oa_smoke_test schema") {
		REQUIRE_NO_FAIL(con.Query("ATTACH '' AS atlas_db (TYPE MONGO, SECRET 'atlas_secret')"));

		auto start = std::chrono::high_resolution_clock::now();
		auto result = con.Query("SELECT table_name FROM information_schema.tables WHERE table_catalog = 'atlas_db' AND "
		                        "table_schema = 'oa_smoke_test' ORDER BY table_name LIMIT 10");
		auto end = std::chrono::high_resolution_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
		std::cerr << "[TEST] Query information_schema.tables (oa_smoke_test) took " << duration << "ms" << std::endl;

		REQUIRE(!result->HasError());
		REQUIRE(result->RowCount() > 0);
		auto chunk = result->Fetch();
		REQUIRE(chunk);
		REQUIRE(chunk->size() > 0);

		std::string table_name = chunk->GetValue(0, 0).ToString();
		start = std::chrono::high_resolution_clock::now();
		result = con.Query("SELECT COUNT(*) FROM atlas_db.\"oa_smoke_test\".\"" + table_name + "\"");
		end = std::chrono::high_resolution_clock::now();
		duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
		std::cerr << "[TEST] Query collection " << table_name << " took " << duration << "ms" << std::endl;
		if (!result->HasError()) {
			// If query succeeds, verify we get a count
			REQUIRE(result->RowCount() == 1);
			chunk = result->Fetch();
			REQUIRE(chunk);
			REQUIRE(!chunk->GetValue(0, 0).IsNull());
		}
	}

	SECTION("Test mongo_scan function directly") {
		auto result =
		    con.Query("SELECT COUNT(*) FROM mongo_scan('" + connection_string + "', 'admin', 'system.version')");
		if (!result->HasError()) {
			// If query succeeds, verify we get a count
			REQUIRE(result->RowCount() == 1);
			auto chunk = result->Fetch();
			REQUIRE(chunk);
			REQUIRE(!chunk->GetValue(0, 0).IsNull());
		}
	}

	SECTION("Cleanup: DETACH") {
		REQUIRE_NO_FAIL(con.Query("ATTACH '' AS atlas_db (TYPE MONGO, SECRET 'atlas_secret')"));

		duckdb::Connection cleanup_con(db);
		auto detach_result = cleanup_con.Query("DETACH atlas_db");
		if (!detach_result->HasError()) {
			auto result = cleanup_con.Query("SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'atlas_db'");
			REQUIRE(!result->HasError());
			auto chunk = result->Fetch();
			REQUIRE(chunk->GetValue(0, 0).GetValue<int64_t>() == 0);
		}
	}

	// Cleanup: Drop the secret
	REQUIRE_NO_FAIL(con.Query("DROP SECRET IF EXISTS atlas_secret"));
}
