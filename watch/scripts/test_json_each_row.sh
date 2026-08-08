#!/bin/bash

# Test script to verify JSONEachRow format with clickhouse-rs
# This script compiles and runs the test

echo "Testing JSONEachRow format with clickhouse-rs..."
echo "================================================"
echo ""

# Check if ClickHouse URL is set
if [ -z "$CLICKHOUSE_URL" ]; then
    echo "CLICKHOUSE_URL not set, using default: http://localhost:8123"
    echo "Set CLICKHOUSE_URL environment variable to use a different URL"
    echo ""
fi

# Check if .env file exists and load it
if [ -f .env ]; then
    echo "Loading environment variables from .env..."
    export $(cat .env | grep CLICKHOUSE_URL | xargs)
fi

# Compile and run the test
echo "Compiling test..."
cargo build --example test_json_each_row 2>&1 | tail -5

if [ $? -eq 0 ]; then
    echo ""
    echo "Running test..."
    echo ""
    cargo run --example test_json_each_row
else
    echo ""
    echo "Building as a regular binary test instead..."
    cargo build --bin test_json_each_row 2>&1 | tail -5
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "Running test..."
        echo ""
        cargo run --bin test_json_each_row
    else
        echo ""
        echo "Error: Could not compile test. Trying alternative approach..."
        echo ""
        echo "Run the test manually with:"
        echo "  cargo test --test test_json_each_row -- --nocapture"
    fi
fi


