// Stub implementation of ExtensionHelper::LoadAllExtensions for DuckDB main compatibility
// This function is referenced by DuckDB's constructor when load_extensions=true,
// but may not be available in extension builds. This stub provides a minimal implementation
// that loads core_functions if available.
//
// Note: In DuckDB v1.4.3 (stable), LoadAllExtensions is available in the static library,
// so this stub should only be compiled when building against DuckDB main.
// We check if the symbol exists at link time - if it does, the linker will use the one from
// the static library and ignore this stub.

#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

// This stub is only needed for DuckDB main where LoadAllExtensions may not be in the static library
// For DuckDB stable (v1.4.3), LoadAllExtensions exists in extension_helper.cpp, so the linker
// will use that version and this stub won't cause duplicate symbol errors if we use weak linking
// However, weak linking for C++ member functions may not work reliably, so we'll conditionally
// compile this based on whether we detect the symbol at build time
#ifdef DUCKDB_MAIN_BUILD
void ExtensionHelper::LoadAllExtensions(DuckDB &db) {
	// Stub implementation - try to auto-load core_functions extension if available
	// This avoids linking errors when ExtensionHelper::LoadAllExtensions is not available
	// in DuckDB main extension builds, while still providing basic functionality
	try {
		ExtensionHelper::TryAutoLoadExtension(*db.instance, "core_functions");
	} catch (...) {
		// Ignore exceptions - this is a stub implementation
		// core_functions may not be available or already loaded
	}
}
#endif

} // namespace duckdb
