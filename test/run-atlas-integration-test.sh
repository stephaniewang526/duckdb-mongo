#!/bin/bash
# Script to run MongoDB Atlas integration tests
# This script builds and runs the C++ integration test for MongoDB Atlas connectivity

set -e

echo "=== MongoDB Atlas Integration Test ==="
echo ""

# Check for required environment variables
if [ -z "$MONGO_ATLAS_USERNAME" ] || [ -z "$MONGO_ATLAS_PASSWORD" ]; then
    echo "Error: MongoDB Atlas credentials not set."
    echo ""
    echo "Please set the following environment variables:"
    echo "  export MONGO_ATLAS_USERNAME='your-username'"
    echo "  export MONGO_ATLAS_PASSWORD='your-password'"
    echo ""
    echo "These credentials will be used to connect to MongoDB Atlas for integration testing."
    exit 1
fi

echo "MongoDB Atlas credentials found."
echo "Username: $MONGO_ATLAS_USERNAME"
echo ""

TEST_EXECUTABLE="./build/release/extension/mongo/test_atlas_integration"

# Always build so the test binary matches the current sources and DuckDB submodule.
# The target is opt-in (see CMakeLists.txt); the build is incremental.
echo "Building the extension with the integration test enabled..."
echo ""
EXT_FLAGS="${EXT_FLAGS:-} -DMONGO_BUILD_ATLAS_INTEGRATION_TEST=ON" make release

if [ ! -f "$TEST_EXECUTABLE" ]; then
    echo ""
    echo "Error: Failed to build integration test executable at: $TEST_EXECUTABLE"
    exit 1
fi

echo "Running MongoDB Atlas integration test..."
echo ""

# Run the integration test
# The test uses Catch2 test framework
# We can filter tests using tags: [mongo][atlas][integration]
$TEST_EXECUTABLE "[mongo][atlas][integration]"

TEST_RESULT=$?

echo ""
if [ $TEST_RESULT -eq 0 ]; then
    echo "=== Integration test PASSED ==="
else
    echo "=== Integration test FAILED ==="
    echo "Exit code: $TEST_RESULT"
fi

exit $TEST_RESULT
