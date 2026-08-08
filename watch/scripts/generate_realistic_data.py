#!/usr/bin/env python3
"""
Realistic Data Generator for Reiver
Simulates a production environment with diverse observability data
"""

import requests
import random
import re
import time
from datetime import datetime, timedelta
from typing import Dict, List
import sys

# Configuration
BASE_URL = "http://localhost:3000"
PROJECT_ID = "2c60e43d-e9c0-4275-8091-5387b75622bc"
API_KEY = "RzohwTxWGVVM8Vg54ehJulN6AkQz0iJn"

# Realistic service names for a microservices architecture
SERVICES = [
    "api-gateway",
    "auth-service",
    "user-service",
    "payment-service",
    "order-service",
    "inventory-service",
    "notification-service",
    "analytics-service",
    "search-service",
    "recommendation-service",
]

# Realistic environments
ENVIRONMENTS = ["production", "staging", "development"]

# Common error types and messages
ERROR_SCENARIOS = [
    {
        "type": "TypeError",
        "message": "Cannot read property 'id' of undefined",
        "service": "user-service",
        "file": "src/controllers/user.controller.js",
        "line": 145,
        "severity": "error",
    },
    {
        "type": "ValidationError",
        "message": "Invalid email format provided",
        "service": "auth-service",
        "file": "src/validators/auth.validator.js",
        "line": 67,
        "severity": "warning",
    },
    {
        "type": "DatabaseConnectionError",
        "message": "Connection pool exhausted - timeout acquiring connection",
        "service": "payment-service",
        "file": "src/db/connection.js",
        "line": 89,
        "severity": "error",
    },
    {
        "type": "RateLimitError",
        "message": "Rate limit exceeded: 1000 requests per minute",
        "service": "api-gateway",
        "file": "src/middleware/ratelimit.js",
        "line": 34,
        "severity": "warning",
    },
    {
        "type": "PaymentProcessingError",
        "message": "Payment gateway timeout after 30s",
        "service": "payment-service",
        "file": "src/services/stripe.service.js",
        "line": 203,
        "severity": "error",
    },
    {
        "type": "AuthenticationError",
        "message": "JWT token expired",
        "service": "auth-service",
        "file": "src/middleware/auth.middleware.js",
        "line": 45,
        "severity": "warning",
    },
    {
        "type": "NotFoundError",
        "message": "User with ID 12345 not found",
        "service": "user-service",
        "file": "src/repositories/user.repository.js",
        "line": 112,
        "severity": "info",
    },
    {
        "type": "RedisConnectionError",
        "message": "Redis connection lost - reconnecting",
        "service": "analytics-service",
        "file": "src/cache/redis.client.js",
        "line": 78,
        "severity": "error",
    },
    {
        "type": "ElasticsearchError",
        "message": "Search query timeout after 5s",
        "service": "search-service",
        "file": "src/services/elasticsearch.service.js",
        "line": 156,
        "severity": "warning",
    },
    {
        "type": "KafkaProducerError",
        "message": "Failed to produce message to topic 'orders'",
        "service": "order-service",
        "file": "src/queue/kafka.producer.js",
        "line": 91,
        "severity": "error",
    },
]

# Realistic API endpoints
ENDPOINTS = {
    "api-gateway": [
        "/api/v1/health",
        "/api/v1/users",
        "/api/v1/products",
        "/api/v1/orders",
        "/api/v1/search",
    ],
    "auth-service": [
        "/auth/login",
        "/auth/register",
        "/auth/logout",
        "/auth/refresh",
        "/auth/verify",
    ],
    "user-service": [
        "/users/:id",
        "/users/:id/profile",
        "/users/:id/orders",
        "/users/:id/preferences",
    ],
    "payment-service": [
        "/payments/create",
        "/payments/:id/status",
        "/payments/:id/refund",
        "/payments/webhook",
    ],
    "order-service": [
        "/orders/create",
        "/orders/:id",
        "/orders/:id/status",
        "/orders/:id/cancel",
    ],
    "inventory-service": [
        "/inventory/check",
        "/inventory/reserve",
        "/inventory/release",
        "/inventory/sync",
    ],
    "notification-service": [
        "/notifications/email",
        "/notifications/sms",
        "/notifications/push",
    ],
    "analytics-service": [
        "/analytics/events",
        "/analytics/metrics",
        "/analytics/reports",
    ],
    "search-service": [
        "/search/products",
        "/search/users",
        "/search/autocomplete",
    ],
    "recommendation-service": [
        "/recommendations/products",
        "/recommendations/similar",
    ],
}

# Log message templates
LOG_TEMPLATES = [
    "User {user_id} logged in successfully from IP {ip}",
    "Processing payment for order {order_id} - amount: ${amount}",
    "Order {order_id} created with {item_count} items",
    "Sending email notification to {email}",
    "Cache miss for key: {cache_key}",
    "Database query executed in {duration}ms: {query}",
    "API request: {method} {endpoint} - {status_code} in {duration}ms",
    "Background job {job_id} started",
    "Background job {job_id} completed in {duration}s",
    "Rate limit warning: {endpoint} has {count} requests in last minute",
    "Inventory updated: Product {product_id} - {quantity} units remaining",
    "Search query executed: '{query}' - {result_count} results in {duration}ms",
]

class ReiverDataGenerator:
    def __init__(self, project_id: str, api_key: str):
        self.project_id = project_id
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "X-API-Key": api_key,
        })

    def generate_trace_id(self) -> str:
        """Generate a realistic trace ID"""
        return f"{random.randint(1000000000000000, 9999999999999999):016x}"

    def generate_span_id(self) -> str:
        """Generate a realistic span ID"""
        return f"{random.randint(100000000000, 999999999999):012x}"

    def _build_exception_payload(self, scenario: Dict, timestamp: datetime) -> Dict:
        """Build an ExceptionPayload dict (for single or batch)."""
        user_id = f"user_{random.randint(1000, 9999)}"
        return {
            "project_key": self.api_key,
            "timestamp": None,  # Let backend use current time
            "level": scenario["severity"],
            "message": scenario["message"],
            "exception": {
                "type": scenario["type"],
                "value": scenario["message"],
                "stacktrace": [
                    {"filename": scenario["file"], "function": f"handle{scenario['service'].replace('-', '_')}", "lineno": scenario["line"], "code": f"  throw new {scenario['type']}('{scenario['message']}');"},
                    {"filename": "src/middleware/error.handler.js", "function": "errorHandler", "lineno": 23, "code": "  await next();"},
                    {"filename": "node_modules/express/lib/router/index.js", "function": "handle", "lineno": 275, "code": "  fn(req, res, next);"},
                ],
            },
            "context": None,
            "tags": {"version": f"1.{random.randint(0, 5)}.{random.randint(0, 20)}", "region": random.choice(["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"])},
            "user": {"id": user_id, "email": f"{user_id}@example.com"},
            "service_name": scenario["service"],
        }

    def send_exceptions_batch(self, payloads: List[Dict]) -> int:
        """Send up to 100 exceptions via POST /api/v1/exceptions/batch. Returns count sent on success, 0 on failure."""
        if not payloads:
            return 0
        try:
            r = self.session.post(f"{BASE_URL}/api/v1/exceptions/batch", json=payloads, timeout=30)
            if r.status_code in (200, 201, 202):
                return len(payloads)
            print(f"Failed to send exceptions batch: HTTP {r.status_code} - {r.text}")
            return 0
        except Exception as e:
            print(f"Error sending exceptions batch: {e}")
            return 0

    def build_trace_spans(self, service: str, timestamp: datetime, duration_ms: float, status: str = "ok") -> List[Dict]:
        """Build span dicts for one trace (2–4 spans). Use with send_spans_batch for batching."""
        trace_id = self.generate_trace_id()
        parent_span_id = self.generate_span_id()
        endpoint = random.choice(ENDPOINTS[service])

        def to_span(name: str, span_id: str, parent: str, start: datetime, dur_ms: float, st: str, attrs: dict) -> dict:
            return {
                "project_key": self.api_key,
                "trace_id": trace_id,
                "span_id": span_id,
                "parent_span_id": parent if parent else None,
                "operation_name": name,
                "service_name": service,
                "start_time": start.isoformat(),
                "duration_ms": int(round(dur_ms)),
                "status": st,
                "tags": attrs,
            }

        root_name = f"{random.choice(['GET', 'POST', 'PUT', 'DELETE'])} {endpoint}"
        payloads = [
            to_span(
                root_name,
                parent_span_id,
                "",
                timestamp,
                duration_ms,
                status,
                {"http.method": random.choice(["GET", "POST", "PUT", "DELETE"]), "http.route": endpoint, "http.status_code": 500 if status == "error" else (429 if duration_ms > 3000 else 200)},
            )
        ]

        db_span_id = self.generate_span_id()
        db_duration = duration_ms * random.uniform(0.2, 0.5)
        payloads.append(to_span("SELECT FROM users", db_span_id, parent_span_id, timestamp + timedelta(milliseconds=10), db_duration, "ok", {"db.system": "postgresql", "db.statement": "SELECT * FROM users WHERE id = $1"}))

        if random.random() > 0.3:
            cache_span_id = self.generate_span_id()
            payloads.append(to_span("redis.get", cache_span_id, parent_span_id, timestamp, random.uniform(1, 10), "ok", {"cache.hit": random.choice([True, False]), "cache.key": f"user:{random.randint(1000, 9999)}"}))

        if random.random() > 0.5:
            http_span_id = self.generate_span_id()
            downstream = random.choice([s for s in SERVICES if s != service])
            payloads.append(to_span(f"HTTP GET {downstream}", http_span_id, parent_span_id, timestamp + timedelta(milliseconds=20), duration_ms * random.uniform(0.3, 0.6), "ok", {"http.method": "GET", "http.url": f"http://{downstream}:8080/api/v1/data", "peer.service": downstream}))

        return payloads

    def send_spans_batch(self, spans: List[Dict]) -> int:
        """Send up to 1000 spans via POST /api/v1/spans/batch. Returns count sent on success, 0 on failure."""
        if not spans:
            return 0
        try:
            r = self.session.post(f"{BASE_URL}/api/v1/spans/batch", json=spans, timeout=30)
            if r.status_code in (200, 201, 202):
                return len(spans)
            return 0
        except Exception as e:
            print(f"Error sending spans batch: {e}")
            return 0

    def _format_log_message(self, service: str) -> str:
        """Format a random log template with placeholders. Used when batching logs."""
        template = random.choice(LOG_TEMPLATES)
        context = {
            "user_id": f"user_{random.randint(1000, 9999)}",
            "order_id": f"order_{random.randint(10000, 99999)}",
            "product_id": f"prod_{random.randint(100, 999)}",
            "email": f"user{random.randint(1000, 9999)}@example.com",
            "ip": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "amount": round(random.uniform(10, 500), 2),
            "item_count": random.randint(1, 10),
            "cache_key": f"session:{random.randint(100000, 999999)}",
            "duration": random.randint(10, 500),
            "query": "SELECT * FROM products WHERE category = $1 LIMIT 20",
            "method": random.choice(["GET", "POST", "PUT", "DELETE"]),
            "endpoint": random.choice(ENDPOINTS[service]),
            "status_code": random.choice([200, 201, 400, 404, 500]),
            "job_id": f"job_{random.randint(1000, 9999)}",
            "count": random.randint(50, 200),
            "quantity": random.randint(0, 100),
            "result_count": random.randint(0, 50),
        }
        placeholder_keys = re.findall(r'\{(\w+)\}', template)
        return template.format(**{k: context.get(k, "N/A") for k in placeholder_keys})

    def send_logs_batch(self, service: str, entries: List[tuple]) -> int:
        """Send many logs in one OTLP ExportLogsServiceRequest. entries = [(timestamp, level, message), ...]. Returns count on success, 0 on failure."""
        if not entries:
            return 0
        log_records = [
            {"timeUnixNano": int(ts.timestamp() * 1_000_000_000), "severityText": lvl, "body": {"stringValue": msg}}
            for (ts, lvl, msg) in entries
        ]
        payload = {
            "resourceLogs": [{
                "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": service}}]},
                "scopeLogs": [{"logRecords": log_records}],
            }],
        }
        try:
            r = self.session.post(f"{BASE_URL}/api/v1/v1/logs", json=payload, timeout=30)
            if r.status_code in (200, 201, 202):
                return len(entries)
            return 0
        except Exception as e:
            print(f"Error sending logs batch: {e}")
            return 0

    def _build_metric_payload(self, service: str, timestamp: datetime) -> Dict:
        """Build one metric point dict. Use with send_metrics_batch."""
        metric_types = [
            {"name": "http_requests_total", "value": random.randint(100, 1000), "type": "sum"},
            {"name": "http_request_duration_ms", "value": random.uniform(50, 500), "type": "gauge"},
            {"name": "active_connections", "value": random.randint(10, 100), "type": "gauge"},
            {"name": "cpu_usage_percent", "value": random.uniform(20, 90), "type": "gauge"},
            {"name": "memory_usage_mb", "value": random.uniform(500, 2000), "type": "gauge"},
            {"name": "error_rate_percent", "value": random.uniform(0.1, 5), "type": "gauge"},
        ]
        m = random.choice(metric_types)
        env = random.choice(ENVIRONMENTS)
        region = random.choice(["us-east-1", "us-west-2", "eu-west-1"])
        endpoint = random.choice(ENDPOINTS[service])
        return {
            "name": m["name"],
            "value": m["value"],
            "type": m["type"],
            "timestamp": None,  # Let backend use current time
            "labels": {"service_name": service, "environment": env, "region": region, "endpoint": endpoint},
        }

    def send_metrics_batch(self, metrics: List[Dict]) -> int:
        """Send many metric points in one POST /api/v1/metrics. Returns count on success, 0 on failure."""
        if not metrics:
            return 0
        try:
            r = self.session.post(f"{BASE_URL}/api/v1/metrics", json={"project_key": self.api_key, "metrics": metrics}, timeout=30)
            if r.status_code in (200, 201, 202):
                return len(metrics)
            print(f"Failed to send metrics batch: HTTP {r.status_code} - {r.text}")
            return 0
        except Exception as e:
            print(f"Error sending metrics batch: {e}")
            return 0

    def generate_realistic_workload(self, hours: int = 24):
        """
        Generate a realistic workload over the specified time period

        Simulates:
        - Normal traffic patterns (higher during business hours)
        - Error spikes (occasional incidents)
        - Trace distribution (successful + failed requests)
        - Log volume (info, warning, error)
        - Metrics collection
        """
        print(f"🚀 Generating realistic workload for {hours} hours...")
        print(f"📊 Project ID: {PROJECT_ID}")
        print(f"🔑 API Key: {API_KEY[:10]}...")
        print()

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)

        # Counters
        counters = {
            "exceptions": 0,
            "traces": 0,
            "logs": 0,
            "metrics": 0,
            "errors": 0,
        }

        # Generate time-based patterns
        current_time = start_time
        interval_minutes = 5  # Generate data every 5 minutes

        while current_time < end_time:
            # Determine traffic intensity based on hour (simulate business hours)
            hour = current_time.hour

            # Peak hours: 9 AM - 5 PM (higher traffic)
            if 9 <= hour <= 17:
                traffic_multiplier = random.uniform(2.0, 3.0)
            # Off-hours: lower traffic
            elif 0 <= hour <= 6:
                traffic_multiplier = random.uniform(0.3, 0.7)
            # Moderate hours
            else:
                traffic_multiplier = random.uniform(1.0, 1.5)

            # Occasional error spikes (5% chance per interval)
            error_spike = random.random() < 0.05

            # Generate data for this time interval (batched to reduce HTTP round-trips)
            span_batch: List[Dict] = []
            for service in SERVICES:
                # Traces: build spans and batch up to 1000 per POST
                trace_count = int(random.randint(80, 200) * traffic_multiplier)
                for _ in range(trace_count):
                    is_error = random.random() < 0.05 or error_spike
                    duration_ms = random.uniform(50, 300) if not is_error else random.uniform(3000, 10000)
                    status = "error" if is_error else "ok"
                    spans = self.build_trace_spans(service, current_time, duration_ms, status)
                    span_batch.extend(spans)
                    counters["traces"] += 1
                    while len(span_batch) >= 1000:
                        self.send_spans_batch(span_batch[:1000])
                        span_batch = span_batch[1000:]

                # Logs: batch up to 200 per OTLP request
                log_count = int(random.randint(200, 500) * traffic_multiplier)
                log_batch: List[tuple] = []
                for _ in range(log_count):
                    rand = random.random()
                    level = "error" if (error_spike and rand > 0.7) else ("info" if rand < 0.80 else ("warning" if rand < 0.95 else "error"))
                    message = self._format_log_message(service)
                    log_batch.append((current_time, level, message))
                    if len(log_batch) >= 200:
                        counters["logs"] += self.send_logs_batch(service, log_batch)
                        log_batch = []
                if log_batch:
                    counters["logs"] += self.send_logs_batch(service, log_batch)

                # Exceptions: batch up to 100 per POST
                exception_count = int(random.randint(2, 10) * traffic_multiplier)
                if error_spike:
                    exception_count *= 5
                exception_batch: List[Dict] = []
                for _ in range(exception_count):
                    scenarios = [s for s in ERROR_SCENARIOS if s["service"] == service]
                    scenario = random.choice(scenarios if scenarios else ERROR_SCENARIOS)
                    if not scenarios:
                        scenario = {**scenario, "service": service}
                    exception_batch.append(self._build_exception_payload(scenario, current_time))
                    if len(exception_batch) >= 100:
                        counters["exceptions"] += self.send_exceptions_batch(exception_batch)
                        exception_batch = []
                if exception_batch:
                    counters["exceptions"] += self.send_exceptions_batch(exception_batch)

                # Metrics: one POST with all 10 points per service
                metric_count = 10
                metrics_list = [self._build_metric_payload(service, current_time) for _ in range(metric_count)]
                counters["metrics"] += self.send_metrics_batch(metrics_list)

            # Flush any remaining spans for this interval
            if span_batch:
                self.send_spans_batch(span_batch)

            # Progress indicator
            progress = ((current_time - start_time).total_seconds() / (end_time - start_time).total_seconds()) * 100
            print(f"⏳ Progress: {progress:.1f}% | "
                  f"Exceptions: {counters['exceptions']} | "
                  f"Traces: {counters['traces']} | "
                  f"Logs: {counters['logs']} | "
                  f"Metrics: {counters['metrics']}", end="\r")

            # Move to next interval
            current_time += timedelta(minutes=interval_minutes)

            # Small delay to avoid overwhelming the server
            time.sleep(0.1)

        print()
        print()
        print("✅ Data generation complete!")
        print(f"📈 Summary:")
        print(f"   - Exceptions: {counters['exceptions']:,}")
        print(f"   - Traces: {counters['traces']:,}")
        print(f"   - Logs: {counters['logs']:,}")
        print(f"   - Metrics: {counters['metrics']:,}")
        print()
        print(f"🌐 View your data at: {BASE_URL}/projects/{self.project_id}/observability")

def main():
    hours = int(sys.argv[1]) if len(sys.argv) > 1 else 24

    generator = ReiverDataGenerator(PROJECT_ID, API_KEY)
    generator.generate_realistic_workload(hours)

if __name__ == "__main__":
    main()
