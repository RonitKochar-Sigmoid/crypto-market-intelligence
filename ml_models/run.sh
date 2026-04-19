#!/bin/bash

# Crypto Prediction API - Quick Start Script
# Author: <<YOUR_EMAIL>>

echo "=========================================================================="
echo "🚀 Crypto Prediction API - Quick Start"
echo "=========================================================================="
echo ""

# Configuration
export DATABRICKS_SERVER_HOSTNAME="<<YOUR_DATABRICKS_WORKSPACE_URL>>"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<<YOUR_WAREHOUSE_ID>>"

# Check if token is provided
if [ -z "$DATABRICKS_TOKEN" ]; then
    echo "❌ ERROR: DATABRICKS_TOKEN environment variable not set!"
    echo ""
    echo "📝 Options:"
    echo ""
    echo "1. Export token before running this script:"
    echo "   export DATABRICKS_TOKEN='your-token-here'"
    echo "   ./run_api.sh"
    echo ""
    echo "2. Pass token inline:"
    echo "   DATABRICKS_TOKEN='your-token-here' ./run_api.sh"
    echo ""
    echo "3. Generate a token at:"
    echo "   User Settings → Developer → Access Tokens"
    echo ""
    exit 1
fi

# Install dependencies if needed
echo "📦 Checking dependencies..."
if ! python3 -c "import flask" 2>/dev/null; then
    echo "Installing Flask..."
    pip install flask databricks-sql-connector
fi

echo "✅ Dependencies OK"
echo ""

# Display configuration
echo "🔧 Configuration:"
echo "   Server: $DATABRICKS_SERVER_HOSTNAME"
echo "   Warehouse: Serverless Starter Warehouse"
echo "   HTTP Path: $DATABRICKS_HTTP_PATH"
echo "   Token: ${DATABRICKS_TOKEN:0:20}...${DATABRICKS_TOKEN: -10}"
echo ""

echo "🌐 Starting API server on http://localhost:5000"
echo ""
echo "📍 Available endpoints:"
echo "   GET  /health"
echo "   GET  /coins"
echo "   GET  /coins/<symbol>/prediction"
echo ""
echo "💡 Test with:"
echo "   curl http://localhost:5000/health"
echo "   curl http://localhost:5000/coins/BTC/prediction"
echo ""
echo "=========================================================================="
echo ""

# Start the API
python3 crypto_prediction_api.py
