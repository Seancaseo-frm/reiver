#!/bin/bash

# Test data generator script for Reiver
# Continuously sends sample metrics, traces, exceptions to test the system
# Press Ctrl+C to stop
#
# Usage:
#   ./send_test_data.sh              - Continuous mode (default)
#   ./send_test_data.sh --correlated - Send a single correlated exception + traces + logs

set -e

API_KEY="${API_KEY:-RzohwTxWGVVM8Vg54ehJulN6AkQz0iJn}"
BASE_URL="${BASE_URL:-http://localhost:3000}"
INTERVAL="${INTERVAL:-1}"  # Send data every 1 second by default

# Check for --correlated flag
CORRELATED_MODE=false
if [ "$1" = "--correlated" ]; then
    CORRELATED_MODE=true
fi

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Counter for iterations
ITERATION=0

# Function to send request
send_request() {
    local method=$1
    local endpoint=$2
    local data=$3
    local description=$4
    
    echo -e "${BLUE}→ $description${NC}"
    response=$(curl -s -w "\n%{http_code}" -X "$method" \
        "$BASE_URL$endpoint" \
        -H "Content-Type: application/json" \
        -H "x-api-key: $API_KEY" \
        -d "$data" 2>/dev/null || echo -e "\n000")
    
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | sed '$d')
    
    if [ "$http_code" = "200" ] || [ "$http_code" = "201" ] || [ "$http_code" = "204" ]; then
        echo -e "${GREEN}✓ Success (HTTP $http_code)${NC}\n"
        return 0
    else
        echo -e "${YELLOW}✗ Failed (HTTP $http_code): $body${NC}\n"
        return 1
    fi
}

# Function to generate timestamps
generate_timestamps() {
    NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    TEN_SEC_AGO=$(date -u -v-10S +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -d "10 seconds ago" +"%Y-%m-%dT%H:%M:%SZ")
}

# Function to send a single correlated batch (exception + traces + logs with same trace_id)
# This is useful for testing the correlation features in the UI
send_correlated_batch() {
    generate_timestamps
    
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}Sending Correlated Test Data - $(date '+%Y-%m-%d %H:%M:%S')${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    
    # Generate shared IDs for correlation
    TRACE_SUFFIX=$(date +%s)-$RANDOM
    TRACE_ID="trace-correlated-$TRACE_SUFFIX"
    SPAN_ID_ROOT="span-root-$TRACE_SUFFIX"
    SPAN_ID_DB="span-db-$TRACE_SUFFIX"
    SPAN_ID_CACHE="span-cache-$TRACE_SUFFIX"
    
    # Deployment context attributes (shared across exception and spans)
    SERVICE_NAME="payment-service"
    VERSION="2.4.1"
    DEPLOYMENT_ID="deploy-$(date +%Y%m%d)-abc123"
    ENVIRONMENT="production"
    REGION="us-east-1"
    HOST_NAME="payment-worker-7b8f9d6c4-xk2mn"
    RUNTIME="node.js 20.10.0"
    POD_NAME="payment-service-7b8f9d6c4-xk2mn"
    CLUSTER_NAME="prod-cluster-east"
    CONTAINER_ID="docker://a1b2c3d4e5f6"
    
    echo -e "${YELLOW}Correlation IDs:${NC}"
    echo "  trace_id: $TRACE_ID"
    echo "  root span_id: $SPAN_ID_ROOT"
    echo "  service: $SERVICE_NAME"
    echo "  version: $VERSION"
    echo "  deployment_id: $DEPLOYMENT_ID"
    echo ""
    
    # 1. Send root span (API request that will fail)
    echo "🔍 Sending traces (3 spans with same trace_id)..."
    send_request "POST" "/api/v1/spans" "{
      \"project_key\": \"$API_KEY\",
      \"trace_id\": \"$TRACE_ID\",
      \"span_id\": \"$SPAN_ID_ROOT\",
      \"parent_span_id\": null,
      \"operation_name\": \"POST /api/payments/process\",
      \"service_name\": \"$SERVICE_NAME\",
      \"start_time\": \"$TEN_SEC_AGO\",
      \"duration_ms\": 450,
      \"status\": \"error\",
      \"tags\": {
        \"http.method\": \"POST\",
        \"http.url\": \"/api/payments/process\",
        \"http.status_code\": 500,
        \"service.version\": \"$VERSION\",
        \"deployment.environment\": \"$ENVIRONMENT\",
        \"deployment.id\": \"$DEPLOYMENT_ID\",
        \"cloud.region\": \"$REGION\",
        \"host.name\": \"$HOST_NAME\",
        \"process.runtime.name\": \"$RUNTIME\",
        \"k8s.pod.name\": \"$POD_NAME\",
        \"k8s.cluster.name\": \"$CLUSTER_NAME\",
        \"container.id\": \"$CONTAINER_ID\",
        \"user.id\": \"user-98765\"
      }
    }" "Root span (POST /api/payments/process - error)"

    # 2. Send database span (child of root)
    send_request "POST" "/api/v1/spans" "{
      \"project_key\": \"$API_KEY\",
      \"trace_id\": \"$TRACE_ID\",
      \"span_id\": \"$SPAN_ID_DB\",
      \"parent_span_id\": \"$SPAN_ID_ROOT\",
      \"operation_name\": \"db.query SELECT payment\",
      \"service_name\": \"postgres\",
      \"start_time\": \"$TEN_SEC_AGO\",
      \"duration_ms\": 120,
      \"status\": \"ok\",
      \"tags\": {
        \"db.system\": \"postgresql\",
        \"db.statement\": \"SELECT * FROM payments WHERE id = $1\",
        \"db.name\": \"payments_db\",
        \"service.version\": \"15.2\"
      }
    }" "Database span (SELECT payment)"

    # 3. Send cache span (child of root, this one times out causing the error)
    send_request "POST" "/api/v1/spans" "{
      \"project_key\": \"$API_KEY\",
      \"trace_id\": \"$TRACE_ID\",
      \"span_id\": \"$SPAN_ID_CACHE\",
      \"parent_span_id\": \"$SPAN_ID_ROOT\",
      \"operation_name\": \"redis.get payment_lock\",
      \"service_name\": \"redis\",
      \"start_time\": \"$TEN_SEC_AGO\",
      \"duration_ms\": 30005,
      \"status\": \"error\",
      \"tags\": {
        \"db.system\": \"redis\",
        \"redis.command\": \"GET\",
        \"error\": true,
        \"error.message\": \"Connection timeout after 30s\",
        \"service.version\": \"7.2.0\"
      }
    }" "Cache span (redis timeout - error)"

    # 4. Send exception linked to the trace and span
    echo ""
    echo "❌ Sending exception (linked to trace and span)..."
    send_request "POST" "/api/v1/exceptions" "{
      \"project_key\": \"$API_KEY\",
      \"timestamp\": \"$NOW\",
      \"level\": \"error\",
      \"message\": \"Payment processing failed: Redis connection timeout\",
      \"trace_id\": \"$TRACE_ID\",
      \"span_id\": \"$SPAN_ID_ROOT\",
      \"service_name\": \"$SERVICE_NAME\",
      \"environment\": \"$ENVIRONMENT\",
      \"version\": \"$VERSION\",
      \"deployment_id\": \"$DEPLOYMENT_ID\",
      \"region\": \"$REGION\",
      \"host_name\": \"$HOST_NAME\",
      \"runtime\": \"$RUNTIME\",
      \"pod_name\": \"$POD_NAME\",
      \"cluster_name\": \"$CLUSTER_NAME\",
      \"container_id\": \"$CONTAINER_ID\",
      \"http_method\": \"POST\",
      \"http_url\": \"/api/payments/process\",
      \"user_id\": \"user-98765\",
      \"exception\": {
        \"type\": \"RedisConnectionError\",
        \"value\": \"Connection timeout after 30s - unable to acquire payment lock\",
        \"stacktrace\": [
          {\"filename\": \"node_modules/ioredis/built/redis/index.js\", \"function\": \"Redis.connect\", \"lineno\": 154},
          {\"filename\": \"src/services/cache.ts\", \"function\": \"CacheService.get\", \"lineno\": 87},
          {\"filename\": \"src/services/payment.ts\", \"function\": \"PaymentService.acquireLock\", \"lineno\": 234},
          {\"filename\": \"src/services/payment.ts\", \"function\": \"PaymentService.process\", \"lineno\": 156},
          {\"filename\": \"src/controllers/payment.ts\", \"function\": \"PaymentController.processPayment\", \"lineno\": 42},
          {\"filename\": \"src/routes/payments.ts\", \"function\": \"router.post\", \"lineno\": 18}
        ]
      },
      \"context\": {
        \"payment_id\": \"pay_abc123xyz\",
        \"amount\": 149.99,
        \"currency\": \"USD\",
        \"customer_id\": \"cust_98765\",
        \"request_id\": \"req-$TRACE_SUFFIX\"
      },
      \"tags\": {
        \"environment\": \"$ENVIRONMENT\",
        \"service\": \"$SERVICE_NAME\",
        \"version\": \"$VERSION\"
      }
    }" "Exception with deployment context"

    # 5. Send logs with same trace_id
    echo ""
    echo "📝 Sending logs (linked to same trace)..."
    
    # Log 1: Request received
    send_request "POST" "/api/logs/ingest" "{
      \"message\": \"Payment request received for customer cust_98765, amount: 149.99 USD\",
      \"level\": \"info\",
      \"timestamp\": \"$TEN_SEC_AGO\",
      \"service\": \"$SERVICE_NAME\",
      \"trace_id\": \"$TRACE_ID\",
      \"span_id\": \"$SPAN_ID_ROOT\",
      \"source\": \"application\"
    }" "Log: Request received"
    
    # Log 2: Database query successful
    send_request "POST" "/api/logs/ingest" "{
      \"message\": \"Payment record found in database, validating payment details\",
      \"level\": \"info\",
      \"timestamp\": \"$TEN_SEC_AGO\",
      \"service\": \"$SERVICE_NAME\",
      \"trace_id\": \"$TRACE_ID\",
      \"span_id\": \"$SPAN_ID_DB\",
      \"source\": \"application\"
    }" "Log: DB query success"
    
    # Log 3: Warning before error
    send_request "POST" "/api/logs/ingest" "{
      \"message\": \"Redis connection slow, retrying... attempt 2 of 3\",
      \"level\": \"warn\",
      \"timestamp\": \"$NOW\",
      \"service\": \"$SERVICE_NAME\",
      \"trace_id\": \"$TRACE_ID\",
      \"span_id\": \"$SPAN_ID_CACHE\",
      \"source\": \"application\"
    }" "Log: Warning (retry)"
    
    # Log 4: Error log
    send_request "POST" "/api/logs/ingest" "{
      \"message\": \"Failed to acquire payment lock: Redis connection timeout after 30s\",
      \"level\": \"error\",
      \"timestamp\": \"$NOW\",
      \"service\": \"$SERVICE_NAME\",
      \"trace_id\": \"$TRACE_ID\",
      \"span_id\": \"$SPAN_ID_CACHE\",
      \"source\": \"application\"
    }" "Log: Error (timeout)"

    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}✅ Correlated test data sent successfully!${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "${CYAN}You can now check the UI:${NC}"
    echo "  1. Go to Exceptions page - look for 'Payment processing failed: Redis connection timeout'"
    echo "  2. Click on the exception to see deployment context (version, environment, region, etc.)"
    echo "  3. Check the linked trace (trace_id: $TRACE_ID)"
    echo "  4. View 'Logs around this exception' section"
    echo ""
}

# Function to send all test data in one batch
send_test_batch() {
    local iteration=$1
    generate_timestamps
    
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}Batch #$iteration - $(date '+%Y-%m-%d %H:%M:%S')${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    
    # 1. Send Metrics (every iteration)
    echo "📊 Sending metrics..."
    # Generate some variation in values
    HTTP_REQUESTS=$((150 + RANDOM % 50))
    HTTP_ERRORS=$((5 + RANDOM % 10))
    CPU_USAGE=$(awk "BEGIN {printf \"%.1f\", 50 + rand() * 30}")
    MEMORY_USAGE=$(awk "BEGIN {printf \"%.1f\", 40 + rand() * 20}")
    DURATION=$(awk "BEGIN {printf \"%.1f\", 100 + rand() * 100}")
    
    send_request "POST" "/api/v1/metrics" "{
      \"metrics\": [
        {
          \"name\": \"http.requests\",
          \"value\": $HTTP_REQUESTS,
          \"labels\": {\"service\": \"api\", \"method\": \"GET\", \"status\": \"200\"}
        },
        {
          \"name\": \"http.requests\",
          \"value\": $HTTP_ERRORS,
          \"labels\": {\"service\": \"api\", \"method\": \"GET\", \"status\": \"500\"}
        },
        {
          \"name\": \"http.request.duration\",
          \"value\": $DURATION,
          \"labels\": {\"service\": \"api\", \"method\": \"GET\"}
        },
        {
          \"name\": \"cpu.usage\",
          \"value\": $CPU_USAGE,
          \"labels\": {\"service\": \"api\", \"host\": \"web-01\"}
        },
        {
          \"name\": \"memory.usage\",
          \"value\": $MEMORY_USAGE,
          \"labels\": {\"service\": \"api\", \"host\": \"web-01\"}
        }
      ]
    }" "Metrics (5 points)"

    # 2. Send Exceptions (randomly, not every iteration)
    if [ $((iteration % 3)) -eq 0 ]; then
        echo "❌ Sending exceptions..."
        ERROR_ID=$(shuf -i 1000-9999 -n 1 2>/dev/null || echo $((1000 + RANDOM % 9000)))
        send_request "POST" "/api/v1/exceptions" "{
          \"project_key\": \"$API_KEY\",
          \"timestamp\": \"$NOW\",
          \"level\": \"error\",
          \"message\": \"Database connection failed (Error #$ERROR_ID)\",
          \"exception\": {
            \"type\": \"DatabaseError\",
            \"value\": \"Connection timeout after 30s\",
            \"stacktrace\": [
              {\"filename\": \"db.js\", \"function\": \"connect\", \"lineno\": 45},
              {\"filename\": \"server.js\", \"function\": \"init\", \"lineno\": 102}
            ]
          },
          \"context\": {\"user_id\": \"12345\", \"request_id\": \"req-abc-$ERROR_ID\"},
          \"tags\": {\"environment\": \"production\", \"service\": \"api\"}
        }" "Exception (random)"
    fi

    # 3. Send Spans/Traces (randomly, not every iteration)
    if [ $((iteration % 2)) -eq 0 ]; then
        echo "🔍 Sending traces..."
        TRACE_SUFFIX=$(date +%s)-$RANDOM
        TRACE_ID="trace-$TRACE_SUFFIX"
        SPAN_ID_1="span-1-$TRACE_SUFFIX"
        SPAN_ID_2="span-2-$TRACE_SUFFIX"
        SPAN_ID_3="span-3-$TRACE_SUFFIX"
        
        DURATION_ROOT=$((200 + RANDOM % 100))
        DURATION_DB=$((80 + RANDOM % 80))
        DURATION_CACHE=$((10 + RANDOM % 20))
        
        send_request "POST" "/api/v1/spans" "{
          \"project_key\": \"$API_KEY\",
          \"trace_id\": \"$TRACE_ID\",
          \"span_id\": \"$SPAN_ID_1\",
          \"parent_span_id\": null,
          \"operation_name\": \"GET /api/users\",
          \"service_name\": \"api-service\",
          \"start_time\": \"$TEN_SEC_AGO\",
          \"duration_ms\": $DURATION_ROOT,
          \"status\": \"ok\",
          \"tags\": {\"http.method\": \"GET\", \"http.status_code\": 200}
        }" "Root span"

        send_request "POST" "/api/v1/spans" "{
          \"project_key\": \"$API_KEY\",
          \"trace_id\": \"$TRACE_ID\",
          \"span_id\": \"$SPAN_ID_2\",
          \"parent_span_id\": \"$SPAN_ID_1\",
          \"operation_name\": \"db.query\",
          \"service_name\": \"database\",
          \"start_time\": \"$TEN_SEC_AGO\",
          \"duration_ms\": $DURATION_DB,
          \"status\": \"ok\",
          \"tags\": {\"db.statement\": \"SELECT * FROM users\"}
        }" "Database span"

        send_request "POST" "/api/v1/spans" "{
          \"project_key\": \"$API_KEY\",
          \"trace_id\": \"$TRACE_ID\",
          \"span_id\": \"$SPAN_ID_3\",
          \"parent_span_id\": \"$SPAN_ID_1\",
          \"operation_name\": \"redis.get\",
          \"service_name\": \"cache\",
          \"start_time\": \"$TEN_SEC_AGO\",
          \"duration_ms\": $DURATION_CACHE,
          \"status\": \"ok\",
          \"tags\": {\"redis.command\": \"GET\", \"cache.hit\": true}
        }" "Cache span"
    fi

    echo -e "${GREEN}✅ Batch #$iteration completed!${NC}\n"
}

# Trap Ctrl+C to exit gracefully
trap 'echo ""; echo -e "${YELLOW}🛑 Stopping test data generator...${NC}"; exit 0' INT

# Main execution
if [ "$CORRELATED_MODE" = true ]; then
    # Send a single correlated batch and exit
    echo "🚀 Sending correlated test data (exception + traces + logs)..."
    echo "API Key: ${API_KEY:0:10}..."
    echo "Base URL: $BASE_URL"
    echo ""
    send_correlated_batch
else
    # Continuous mode
    echo "🚀 Starting continuous test data generator for Reiver..."
    echo "API Key: ${API_KEY:0:10}..."
    echo "Base URL: $BASE_URL"
    echo "Interval: ${INTERVAL} second(s)"
    echo "Press Ctrl+C to stop"
    echo ""
    
    # Main loop
    while true; do
        ITERATION=$((ITERATION + 1))
        send_test_batch $ITERATION
        
        if [ $ITERATION -eq 1 ]; then
            echo -e "${CYAN}📝 Note: This will continue sending data every ${INTERVAL} seconds${NC}"
            echo -e "${CYAN}   Press Ctrl+C to stop${NC}\n"
        fi
        
        echo -e "${CYAN}⏳ Waiting ${INTERVAL} seconds until next batch...${NC}\n"
        sleep $INTERVAL
    done
fi
