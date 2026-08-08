#!/bin/bash
#
# Convenient wrapper script for generating test data
# Usage: ./scripts/generate_test_data.sh
#

set -e

echo "🎯 Reiver Test Data Generator"
echo "=============================="
echo ""

# Hardcoded credentials for convenience
PROJECT_ID="2c60e43d-e9c0-4275-8091-5387b75622bc"
API_KEY="RzohwTxWGVVM8Vg54ehJulN6AkQz0iJn"

# Check if hours are provided as argument
if [ $# -ge 1 ]; then
    HOURS="$1"
else
    HOURS="24"
fi

echo ""
echo "📊 Configuration:"
echo "   Project ID: $PROJECT_ID"
echo "   API Key: ${API_KEY:0:10}..."
echo "   Time Range: $HOURS hours"
echo ""

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: Python 3 is not installed"
    echo "   Please install Python 3.7+ to use this script"
    exit 1
fi

# Check if requests library is installed
if ! python3 -c "import requests" 2>/dev/null; then
    echo "⚠️  Warning: 'requests' library not found"
    echo "   Installing requests..."
    pip3 install requests || {
        echo "❌ Error: Failed to install requests library"
        echo "   Please run: pip3 install requests"
        exit 1
    }
fi

# Run the data generation script
echo "🚀 Starting data generation..."
echo ""

python3 "$(dirname "$0")/generate_realistic_data.py" "$HOURS"

echo ""
echo "✨ Done! You can now view your data in the Reiver UI."
