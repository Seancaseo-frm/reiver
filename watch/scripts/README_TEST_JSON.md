# Testing JSONEachRow Format

This test verifies if the `clickhouse-rs` crate supports the JSONEachRow format as we expect it to work in our GraphQL implementation.

## Quick Start

```bash
# Make sure ClickHouse is running and accessible
# Set CLICKHOUSE_URL if needed (default: http://localhost:8123)

# Run the test
cargo run --example test_json_each_row
```

## What It Tests

1. **JSONEachRow Format Support**: Checks if `clickhouse-rs` returns `Vec<String>` when using `FORMAT JSONEachRow`
2. **JSON Parsing**: Verifies that the returned strings are valid JSON
3. **Multiple Rows**: Tests with queries that return multiple rows

## Expected Results

### ✅ Success Case
If the test succeeds, it means:
- The current implementation in `src/graphql.rs` (lines 295-308) is correct
- We can use `Vec<String>` with `fetch_all()` when using `FORMAT JSONEachRow`
- No changes needed to the GraphQL implementation

### ❌ Failure Case
If the test fails, it means:
- The `clickhouse-rs` crate doesn't support JSONEachRow in the way we expect
- We need to implement an alternative approach:
  - Use typed structs with all fields (loses dynamic field selection)
  - Find another way to get raw JSON from ClickHouse
  - Use a different ClickHouse client library

## Environment Variables

- `CLICKHOUSE_URL`: ClickHouse server URL (default: `http://localhost:8123`)

## Example Output

```
Testing JSONEachRow format with clickhouse-rs...

Connecting to ClickHouse at: http://localhost:8123

=== Test 1: JSONEachRow with Vec<String> ===
Query: SELECT 'test-id-123' as id, 'test-project-456' as project_id...
Attempting to fetch as Vec<String>...
✓ fetch_all() succeeded!
Result count: 1 lines

Parsing JSON lines...
Line 1: {"id":"test-id-123","project_id":"test-project-456","level":"info","message":"Test message"}
  ✓ Valid JSON
    {
      "id": "test-id-123",
      "project_id": "test-project-456",
      "level": "info",
      "message": "Test message"
    }

✅ JSON parsing successful!

✅ SUCCESS: JSONEachRow returns Vec<String> as expected!
The current implementation in graphql.rs should work correctly.
```

## Next Steps

After running the test:
- If successful: Proceed with testing the GraphQL endpoint
- If failed: We'll need to fix the implementation in `src/graphql.rs`


