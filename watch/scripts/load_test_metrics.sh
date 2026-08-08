#!/bin/bash
# Load testing script for metrics API
# This script generates synthetic metrics data and runs queries against it

set -e

# Configuration
API_BASE="http://localhost:8080"
PROJECT_ID="${PROJECT_ID:-2c60e43d-e9c0-4275-8091-5387b75622bc}"
METRIC_NAME="test_load_metric"
DURATION_SECONDS="${DURATION_SECONDS:-60}"
CONCURRENT_REQUESTS="${CONCURRENT_REQUESTS:-10}"

echo "Starting metrics load test..."
echo "API: $API_BASE"
echo "Project: $PROJECT_ID"
echo "Duration: ${DURATION_SECONDS}s"
echo "Concurrent requests: $CONCURRENT_REQUESTS"
echo "=========================================="

# Function to generate random metric data
generate_metric() {
    local timestamp=$(date +%s%3N)  # milliseconds
    local value=$((RANDOM % 1000))
    local service="service-$((RANDOM % 5))"
    local env="env-$((RANDOM % 3))"

    cat <<EOF
{
  "name": "$METRIC_NAME",
  "value": $value,
  "timestamp": $timestamp,
  "labels": {
    "service": "$service",
    "environment": "$env"
  },
  "temporality": "Unspecified",
  "metric_type": "Gauge"
}
EOF
}

# Function to send metrics
send_metrics() {
    local batch_size=10
    local metrics="["

    for ((i=1; i<=batch_size; i++)); do
        metrics+="$(generate_metric)"
        if [ $i -lt $batch_size ]; then
            metrics+=","
        fi
    done
    metrics+="]"

    curl -s -X POST "$API_BASE/api/v1/metrics" \
         -H "Content-Type: application/json" \
         -d "{\"project_id\": \"$PROJECT_ID\", \"metrics\": $metrics}" \
         >/dev/null
}

# Function to run queries
run_query() {
    local end=$(date +%s)
    local start=$((end - 3600))  # Last hour

    curl -s -X POST "$API_BASE/api/v1/metrics/query" \
         -H "Content-Type: application/json" \
         -d "{
           \"project_id\": \"$PROJECT_ID\",
           \"metric_name\": \"$METRIC_NAME\",
           \"start\": \"$start\",
           \"end\": \"$end\",
           \"step\": 60,
           \"time_aggregation\": \"avg\",
           \"space_aggregation\": \"sum\",
           \"filters\": {\"service\": \"service-1\"}
         }" \
         >/dev/null
}

# Function to run a test worker
run_worker() {
    local worker_id=$1
    local action=$2
    local duration=$3

    local start_time=$(date +%s)
    local requests=0

    echo "Worker $worker_id started ($action)"

    while [ $(($(date +%s) - start_time)) -lt $duration ]; do
        if [ "$action" = "ingest" ]; then
            send_metrics
        elif [ "$action" = "query" ]; then
            run_query
        fi
        requests=$((requests + 1))
    done

    echo "Worker $worker_id completed: $requests requests"
    echo "$worker_id:$requests"
}

# Function to collect results from workers
collect_results() {
    local total_requests=0
    local worker_count=0

    while read -r result; do
        if [[ $result =~ ^([0-9]+):([0-9]+)$ ]]; then
            worker_id="${BASH_REMATCH[1]}"
            requests="${BASH_REMATCH[2]}"
            total_requests=$((total_requests + requests))
            worker_count=$((worker_count + 1))
        fi
    done

    local avg_rps=$((total_requests / DURATION_SECONDS))

    echo "=========================================="
    echo "Load test completed!"
    echo "Total requests: $total_requests"
    echo "Average RPS: $avg_rps"
    echo "=========================================="
}

# Main test execution
echo "Phase 1: Ingesting test data..."

# Run ingestion workers
for ((i=1; i<=CONCURRENT_REQUESTS; i++)); do
    run_worker $i "ingest" $DURATION_SECONDS &
    pids[$i]=$!
done

# Wait for ingestion to complete
for pid in "${pids[@]}"; do
    wait $pid
done

echo "Phase 2: Query load test..."

# Run query workers
query_pids=()
for ((i=1; i<=CONCURRENT_REQUESTS; i++)); do
    run_worker $i "query" $DURATION_SECONDS &
    query_pids[$i]=$!
done

# Collect results from query workers
for pid in "${query_pids[@]}"; do
    wait $pid
done | collect_results

echo "Load test finished successfully!"