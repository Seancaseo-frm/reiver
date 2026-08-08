#!/bin/bash
# Generate Parquet test data for MinIO

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Generating events.parquet..."
python3 "$SCRIPT_DIR/generate_events.py"

if [ -f "$SCRIPT_DIR/events.parquet" ]; then
    echo "Successfully generated events.parquet"
    ls -lh "$SCRIPT_DIR/events.parquet"
else
    echo "ERROR: Failed to generate events.parquet"
    exit 1
fi
