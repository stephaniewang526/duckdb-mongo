# Bump DuckDB Submodule Version

Update the DuckDB submodule to a new release and fix everything needed to make it work.

## Steps

### 1. Update submodules

Update both `duckdb` and `extension-ci-tools` to the target version:

```bash
cd duckdb && git fetch --tags && git checkout <new-version-tag> && cd ..
cd extension-ci-tools && git fetch --tags && git checkout <new-version-tag> && cd ..
```

If `extension-ci-tools` doesn't have a matching tag yet, use the closest available version or `main`.

### 2. Build and fix compilation issues

```bash
make release
```

If there are compilation errors:
- The repo targets a single DuckDB version (no `#if` compatibility layer). Update call sites directly for moved headers or changed APIs; shared helpers live in `src/include/mongo_duckdb_utils.hpp`.
- Check `CMakeLists.txt` — link targets or build system changes may be needed for major version bumps.
- Fix source files as needed for removed or renamed APIs.

Iterate until the build succeeds.

### 3. Update CI/CD

In `.github/workflows/MainDistributionPipeline.yml`, update **three** places:
- `duckdb-stable-build` job: `duckdb_version`, `ci_tools_version`, and the workflow `@tag`
- `code-quality-check` job: `duckdb_version`, `ci_tools_version`, and the workflow `@tag`
- Leave `duckdb-next-build` pointing to `main`

`.github/workflows/MongoDBTests.yml` typically doesn't need changes (it tests `submodule` + `main` matrix automatically).

### 4. Update README.md

Update the version in the announcement near the top:

```markdown
We currently support DuckDB `v1.X.Y`.
```

### 5. Run tests

Follow `.agents/skills/test.md` to run the full test suite. All tests must pass. Fix any test failures caused by the version bump (e.g., changed output formats, new column types).

### 6. Verify

- Build succeeds
- All unit tests pass
- CI workflows reference the correct version in all places
- README matches the new version
- The `duckdb-next-build` CI job (DuckDB `main`) still compiles; fix forward-incompatibilities at the call sites

## Key files to check

| File | What to update |
|---|---|
| `duckdb` (submodule) | Checkout new version tag |
| `extension-ci-tools` (submodule) | Checkout matching version tag |
| `.github/workflows/MainDistributionPipeline.yml` | `duckdb_version`, `ci_tools_version`, workflow `@tags` |
| `README.md` | Version announcement |
| `src/**/*.cpp`, `src/include/*.hpp` | Call-site updates for API changes |
| `CMakeLists.txt` | Link targets (major version bumps only) |

## Notes

- Patch releases within the same minor version (e.g., v1.5.1 → v1.5.2) are usually API-stable and require minimal source changes.
- Major or minor version bumps (e.g., v1.5.x → v1.6.0) are more likely to require source updates and possibly `CMakeLists.txt` changes.
- The vcpkg commit in CI workflows (`vcpkgGitCommitId`) is pinned for MongoDB C++ driver compatibility. Only update if there's a specific reason.
- `extension-ci-tools` may publish a release branch (e.g. `v2.0-cyanoptera`) before a version tag exists. Use `git ls-remote --heads --tags origin` to find it; pass it as `ci_tools_version` and the workflow `@ref`, keeping `duckdb_version` as the exact DuckDB tag.
- Since v2.0, `make test_release` runs `duckdb/scripts/ci/run_tests.py` with parallel workers. Tests must not depend on collections that other tests create or drop. The summary line is `ran tests: N passed[, M failed], K skipped`, which `MongoDBTests.yml` parses.
- Since v2.0, `make format-check` also runs `cmake-format` on `CMakeLists.txt` (CI's Format Check job). Run `make format-fix` before pushing.
- Local `make release` builds report `vX.Y.0-devN` unless `DUCKDB_VERSION` is set; CI sets it from `duckdb_version`.
- If TPC-H tests fail with mismatched string values, the bundled `dbgen` changed. Regenerate local data with `bash test/create-tpch-test-db.sh` (CI does this on every run).
