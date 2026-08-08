using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using UnityEngine;
using UnityEngine.Networking;

namespace DataHippo
{
    /// <summary>
    /// Metric types following OTel conventions.
    /// </summary>
    public enum MetricType
    {
        Gauge,
        Counter,
        Histogram
    }

    /// <summary>
    /// Handles batching and exporting telemetry to DataHippo.
    /// Uses a background thread for network I/O to avoid blocking the main thread.
    /// </summary>
    internal class TelemetryExporter
    {
        private readonly DataHippoConfig _config;
        private readonly Queue<TelemetryItem> _queue;
        private readonly object _lock = new object();
        private readonly Dictionary<string, object> _resourceAttributes;
        private bool _running;
        private Thread _exportThread;
        private readonly AutoResetEvent _flushEvent;

        public TelemetryExporter(DataHippoConfig config)
        {
            _config = config;
            _queue = new Queue<TelemetryItem>();
            _flushEvent = new AutoResetEvent(false);

            // Build resource attributes once
            _resourceAttributes = BuildResourceAttributes();

            // Start background export thread
            _running = true;
            _exportThread = new Thread(ExportLoop)
            {
                Name = "DataHippo-Exporter",
                IsBackground = true
            };
            _exportThread.Start();
        }

        private Dictionary<string, object> BuildResourceAttributes()
        {
            var attrs = new Dictionary<string, object>
            {
                ["service.name"] = string.IsNullOrEmpty(_config.gameName) ? Application.productName : _config.gameName,
                ["service.version"] = Application.version,
                ["game.name"] = string.IsNullOrEmpty(_config.gameName) ? Application.productName : _config.gameName,
                ["game.version"] = Application.version,
                ["game.engine"] = "Unity",
                ["game.engine.version"] = Application.unityVersion,
                ["game.platform"] = PlatformUtils.GetPlatformString(),

                // Device attributes (OTel device.* namespace)
                ["device.manufacturer"] = PlatformUtils.GetDeviceManufacturer(),
                ["device.model.name"] = SystemInfo.deviceModel,

                // GPU attributes (OTel hw.gpu.* namespace)
                ["hw.gpu.vendor"] = SystemInfo.graphicsDeviceVendor,
                ["hw.gpu.model"] = SystemInfo.graphicsDeviceName,

                // System info
                ["os.type"] = SystemInfo.operatingSystemFamily.ToString().ToLower(),
                ["os.description"] = SystemInfo.operatingSystem,
                ["host.arch"] = PlatformUtils.GetArchitecture()
            };

            if (_config.sendDeviceId)
            {
                attrs["device.id"] = SystemInfo.deviceUniqueIdentifier;
            }

            return attrs;
        }

        /// <summary>
        /// Record a metric data point.
        /// </summary>
        public void RecordMetric(string name, double value, MetricType type, Dictionary<string, string> attributes = null)
        {
            var item = new TelemetryItem
            {
                Type = TelemetryType.Metric,
                Name = name,
                Value = value,
                MetricType = type,
                Timestamp = DateTimeOffset.UtcNow,
                Attributes = attributes
            };

            Enqueue(item);
        }

        /// <summary>
        /// Record an event (log with structured data).
        /// </summary>
        public void RecordEvent(string name, Dictionary<string, object> attributes = null)
        {
            var item = new TelemetryItem
            {
                Type = TelemetryType.Event,
                Name = name,
                Timestamp = DateTimeOffset.UtcNow,
                EventAttributes = attributes
            };

            Enqueue(item);
        }

        /// <summary>
        /// Start a span and return a disposable that ends it.
        /// </summary>
        public IDisposable StartSpan(string name, Dictionary<string, string> attributes = null)
        {
            return new SpanScope(this, name, attributes);
        }

        internal void EndSpan(string name, TimeSpan duration, Dictionary<string, string> attributes)
        {
            var item = new TelemetryItem
            {
                Type = TelemetryType.Span,
                Name = name,
                Duration = duration,
                Timestamp = DateTimeOffset.UtcNow,
                Attributes = attributes
            };

            Enqueue(item);
        }

        private void Enqueue(TelemetryItem item)
        {
            lock (_lock)
            {
                if (_queue.Count >= _config.maxQueueSize)
                {
                    // Drop oldest item
                    _queue.Dequeue();
                }
                _queue.Enqueue(item);

                // Signal export thread if batch is ready
                if (_queue.Count >= _config.batchSize)
                {
                    _flushEvent.Set();
                }
            }
        }

        /// <summary>
        /// Flush all pending telemetry synchronously.
        /// </summary>
        public void Flush()
        {
            _flushEvent.Set();
            Thread.Sleep(100); // Give export thread time to process
        }

        /// <summary>
        /// Stop the exporter.
        /// </summary>
        public void Stop()
        {
            _running = false;
            _flushEvent.Set();
            _exportThread?.Join(TimeSpan.FromSeconds(2));
        }

        private void ExportLoop()
        {
            while (_running)
            {
                _flushEvent.WaitOne(TimeSpan.FromSeconds(5));

                List<TelemetryItem> batch;
                lock (_lock)
                {
                    if (_queue.Count == 0) continue;

                    batch = new List<TelemetryItem>();
                    while (_queue.Count > 0 && batch.Count < _config.batchSize)
                    {
                        batch.Add(_queue.Dequeue());
                    }
                }

                if (batch.Count > 0)
                {
                    SendBatch(batch);
                }
            }
        }

        private void SendBatch(List<TelemetryItem> batch)
        {
            try
            {
                // Group by type and send to appropriate endpoints
                var metrics = batch.FindAll(i => i.Type == TelemetryType.Metric);
                var events = batch.FindAll(i => i.Type == TelemetryType.Event);
                var spans = batch.FindAll(i => i.Type == TelemetryType.Span);

                if (metrics.Count > 0)
                    SendMetrics(metrics);
                if (events.Count > 0)
                    SendEvents(events);
                if (spans.Count > 0)
                    SendSpans(spans);
            }
            catch (Exception ex)
            {
                Debug.LogWarning($"[DataHippo] Failed to send telemetry: {ex.Message}");
            }
        }

        private void SendMetrics(List<TelemetryItem> metrics)
        {
            var payload = BuildMetricsPayload(metrics);
            SendToEndpoint("/api/v1/metrics", payload);
        }

        private void SendEvents(List<TelemetryItem> events)
        {
            var payload = BuildEventsPayload(events);
            SendToEndpoint("/api/v1/logs", payload);
        }

        private void SendSpans(List<TelemetryItem> spans)
        {
            var payload = BuildSpansPayload(spans);
            SendToEndpoint("/api/v1/traces", payload);
        }

        private string BuildMetricsPayload(List<TelemetryItem> metrics)
        {
            var sb = new StringBuilder();
            sb.Append("{\"resourceMetrics\":[{\"resource\":{\"attributes\":");
            sb.Append(SerializeAttributes(_resourceAttributes));
            sb.Append("},\"scopeMetrics\":[{\"metrics\":[");

            for (int i = 0; i < metrics.Count; i++)
            {
                if (i > 0) sb.Append(",");
                var m = metrics[i];
                sb.Append("{\"name\":\"").Append(EscapeJson(m.Name)).Append("\"");
                sb.Append(",\"unit\":\"").Append(GetMetricUnit(m.Name)).Append("\"");

                // Use appropriate data point type
                var dataPointType = m.MetricType == MetricType.Histogram ? "histogram" : "gauge";
                sb.Append(",\"").Append(dataPointType).Append("\":{\"dataPoints\":[{");
                sb.Append("\"timeUnixNano\":").Append(m.Timestamp.ToUnixTimeMilliseconds() * 1000000);
                sb.Append(",\"asDouble\":").Append(m.Value);

                if (m.Attributes != null && m.Attributes.Count > 0)
                {
                    sb.Append(",\"attributes\":").Append(SerializeStringAttributes(m.Attributes));
                }

                sb.Append("}]}}");
            }

            sb.Append("]}]}]}");
            return sb.ToString();
        }

        private string BuildEventsPayload(List<TelemetryItem> events)
        {
            var sb = new StringBuilder();
            sb.Append("{\"resourceLogs\":[{\"resource\":{\"attributes\":");
            sb.Append(SerializeAttributes(_resourceAttributes));
            sb.Append("},\"scopeLogs\":[{\"logRecords\":[");

            for (int i = 0; i < events.Count; i++)
            {
                if (i > 0) sb.Append(",");
                var e = events[i];
                sb.Append("{\"timeUnixNano\":").Append(e.Timestamp.ToUnixTimeMilliseconds() * 1000000);
                sb.Append(",\"body\":{\"stringValue\":\"").Append(EscapeJson(e.Name)).Append("\"}");

                if (e.EventAttributes != null && e.EventAttributes.Count > 0)
                {
                    sb.Append(",\"attributes\":").Append(SerializeAttributes(e.EventAttributes));
                }

                sb.Append("}");
            }

            sb.Append("]}]}]}");
            return sb.ToString();
        }

        private string BuildSpansPayload(List<TelemetryItem> spans)
        {
            var sb = new StringBuilder();
            sb.Append("{\"resourceSpans\":[{\"resource\":{\"attributes\":");
            sb.Append(SerializeAttributes(_resourceAttributes));
            sb.Append("},\"scopeSpans\":[{\"spans\":[");

            for (int i = 0; i < spans.Count; i++)
            {
                if (i > 0) sb.Append(",");
                var s = spans[i];
                var traceId = Guid.NewGuid().ToString("N");
                var spanId = Guid.NewGuid().ToString("N").Substring(0, 16);

                sb.Append("{\"traceId\":\"").Append(traceId).Append("\"");
                sb.Append(",\"spanId\":\"").Append(spanId).Append("\"");
                sb.Append(",\"name\":\"").Append(EscapeJson(s.Name)).Append("\"");
                sb.Append(",\"startTimeUnixNano\":").Append((s.Timestamp.ToUnixTimeMilliseconds() - (long)s.Duration.TotalMilliseconds) * 1000000);
                sb.Append(",\"endTimeUnixNano\":").Append(s.Timestamp.ToUnixTimeMilliseconds() * 1000000);

                if (s.Attributes != null && s.Attributes.Count > 0)
                {
                    sb.Append(",\"attributes\":").Append(SerializeStringAttributes(s.Attributes));
                }

                sb.Append("}");
            }

            sb.Append("]}]}]}");
            return sb.ToString();
        }

        private void SendToEndpoint(string endpoint, string payload)
        {
            var url = _config.apiUrl.TrimEnd('/') + endpoint;
            var bytes = Encoding.UTF8.GetBytes(payload);

            try
            {
                // Use System.Net.HttpWebRequest for background thread compatibility
                var request = (System.Net.HttpWebRequest)System.Net.WebRequest.Create(url);
                request.Method = "POST";
                request.ContentType = "application/json";
                request.ContentLength = bytes.Length;
                request.Timeout = 10000; // 10 second timeout
                
                // Add authentication header
                request.Headers.Add("x-reiver-project-key", _config.projectKey);
                request.Headers.Add("x-api-key", _config.projectKey);

                // Write request body
                using (var requestStream = request.GetRequestStream())
                {
                    requestStream.Write(bytes, 0, bytes.Length);
                }

                // Get response (we don't need to read it, just ensure it succeeded)
                using (var response = (System.Net.HttpWebResponse)request.GetResponse())
                {
                    if ((int)response.StatusCode >= 400)
                    {
                        Debug.LogWarning($"[DataHippo] Server returned status {response.StatusCode} for {endpoint}");
                    }
                }
            }
            catch (System.Net.WebException ex)
            {
                // Log but don't throw - telemetry failures shouldn't crash the game
                Debug.LogWarning($"[DataHippo] Failed to send to {endpoint}: {ex.Message}");
            }
            catch (Exception ex)
            {
                Debug.LogWarning($"[DataHippo] Unexpected error sending to {endpoint}: {ex.Message}");
            }
        }

        private static string GetMetricUnit(string metricName)
        {
            if (metricName.Contains("duration") || metricName.Contains("rtt") || metricName.Contains("jitter"))
                return "s";
            if (metricName.Contains("rate") || metricName.Contains("fps"))
                return "{Hz}";
            if (metricName.Contains("memory") || metricName.Contains("bandwidth"))
                return "By";
            if (metricName.Contains("packet_loss"))
                return "1";
            return "1";
        }

        private static string SerializeAttributes(Dictionary<string, object> attrs)
        {
            var sb = new StringBuilder("[");
            bool first = true;
            foreach (var kvp in attrs)
            {
                if (!first) sb.Append(",");
                first = false;
                sb.Append("{\"key\":\"").Append(EscapeJson(kvp.Key)).Append("\",\"value\":{");

                if (kvp.Value is string s)
                    sb.Append("\"stringValue\":\"").Append(EscapeJson(s)).Append("\"");
                else if (kvp.Value is int i)
                    sb.Append("\"intValue\":").Append(i);
                else if (kvp.Value is long l)
                    sb.Append("\"intValue\":").Append(l);
                else if (kvp.Value is double d)
                    sb.Append("\"doubleValue\":").Append(d);
                else if (kvp.Value is float f)
                    sb.Append("\"doubleValue\":").Append(f);
                else if (kvp.Value is bool b)
                    sb.Append("\"boolValue\":").Append(b ? "true" : "false");
                else
                    sb.Append("\"stringValue\":\"").Append(EscapeJson(kvp.Value?.ToString() ?? "")).Append("\"");

                sb.Append("}}");
            }
            sb.Append("]");
            return sb.ToString();
        }

        private static string SerializeStringAttributes(Dictionary<string, string> attrs)
        {
            var sb = new StringBuilder("[");
            bool first = true;
            foreach (var kvp in attrs)
            {
                if (!first) sb.Append(",");
                first = false;
                sb.Append("{\"key\":\"").Append(EscapeJson(kvp.Key));
                sb.Append("\",\"value\":{\"stringValue\":\"").Append(EscapeJson(kvp.Value)).Append("\"}}");
            }
            sb.Append("]");
            return sb.ToString();
        }

        private static string EscapeJson(string s)
        {
            if (string.IsNullOrEmpty(s)) return "";
            return s.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\n", "\\n").Replace("\r", "\\r");
        }

        private class SpanScope : IDisposable
        {
            private readonly TelemetryExporter _exporter;
            private readonly string _name;
            private readonly Dictionary<string, string> _attributes;
            private readonly DateTime _startTime;

            public SpanScope(TelemetryExporter exporter, string name, Dictionary<string, string> attributes)
            {
                _exporter = exporter;
                _name = name;
                _attributes = attributes;
                _startTime = DateTime.UtcNow;
            }

            public void Dispose()
            {
                var duration = DateTime.UtcNow - _startTime;
                _exporter.EndSpan(_name, duration, _attributes);
            }
        }
    }

    internal enum TelemetryType
    {
        Metric,
        Event,
        Span
    }

    internal class TelemetryItem
    {
        public TelemetryType Type;
        public string Name;
        public double Value;
        public MetricType MetricType;
        public DateTimeOffset Timestamp;
        public TimeSpan Duration;
        public Dictionary<string, string> Attributes;
        public Dictionary<string, object> EventAttributes;
    }
}
