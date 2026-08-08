using System.Collections.Generic;
using UnityEngine;

namespace DataHippo
{
    /// <summary>
    /// Collects frame timing metrics: FPS, frame duration, GPU time.
    /// Uses a ring buffer to compute statistics without allocations in the hot path.
    /// </summary>
    internal class FrameMetricsCollector
    {
        private readonly DataHippoConfig _config;
        private readonly TelemetryExporter _exporter;
        private readonly float[] _frameTimeBuffer;
        private readonly float[] _sortBuffer; // Pre-allocated buffer for percentile calculation
        private int _bufferIndex;
        private int _bufferCount;
        private float _timeSinceLastReport;
        private GameObject _updater;
        private bool _running;

        // Buffer size for computing percentiles (1 second at 60fps = 60 samples)
        private const int BufferSize = 120;

        public FrameMetricsCollector(DataHippoConfig config, TelemetryExporter exporter)
        {
            _config = config;
            _exporter = exporter;
            _frameTimeBuffer = new float[BufferSize];
            _sortBuffer = new float[BufferSize]; // Pre-allocate to avoid GC during percentile calculation
        }

        public void Start()
        {
            if (_running) return;
            _running = true;

            // Create a hidden GameObject to receive Update calls
            _updater = new GameObject("DataHippo_FrameCollector")
            {
                hideFlags = HideFlags.HideAndDontSave
            };
            Object.DontDestroyOnLoad(_updater);

            var component = _updater.AddComponent<FrameUpdater>();
            component.Initialize(this);
        }

        public void Stop()
        {
            _running = false;
            if (_updater != null)
            {
                Object.Destroy(_updater);
                _updater = null;
            }
        }

        internal void OnUpdate()
        {
            if (!_running) return;

            // Record frame time (in seconds, per OTel v1.39.0)
            float frameTime = Time.unscaledDeltaTime;

            // Add to ring buffer (zero allocation)
            _frameTimeBuffer[_bufferIndex] = frameTime;
            _bufferIndex = (_bufferIndex + 1) % BufferSize;
            if (_bufferCount < BufferSize) _bufferCount++;

            _timeSinceLastReport += frameTime;

            // Report at configured interval
            if (_timeSinceLastReport >= _config.metricsIntervalSeconds)
            {
                ReportMetrics();
                _timeSinceLastReport = 0;
            }
        }

        private void ReportMetrics()
        {
            if (_bufferCount == 0) return;

            // Compute statistics from buffer
            float sum = 0;
            float min = float.MaxValue;
            float max = float.MinValue;

            for (int i = 0; i < _bufferCount; i++)
            {
                float ft = _frameTimeBuffer[i];
                sum += ft;
                if (ft < min) min = ft;
                if (ft > max) max = ft;
            }

            float avgFrameTime = sum / _bufferCount;
            float avgFps = 1.0f / avgFrameTime;

            // Compute P95 frame time (simple approximation: sort and pick)
            float p95FrameTime = ComputePercentile(0.95f);

            // Base attributes for all metrics
            var attributes = new Dictionary<string, string>();

            if (!string.IsNullOrEmpty(DataHippoSDK.CurrentMatchId))
            {
                attributes["game.match.id"] = DataHippoSDK.CurrentMatchId;
            }

            // Record frame rate (Hz)
            _exporter.RecordMetric("game.client.frame.rate", avgFps, MetricType.Gauge, attributes);

            // Record frame duration (seconds) - avg, min, max, p95
            _exporter.RecordMetric("game.client.frame.duration", avgFrameTime, MetricType.Histogram, attributes);

            // Record p95 frame time as separate metric for alerting
            var p95Attrs = new Dictionary<string, string>(attributes) { ["percentile"] = "95" };
            _exporter.RecordMetric("game.client.frame.duration.p95", p95FrameTime, MetricType.Gauge, p95Attrs);

            // Record min FPS (from max frame time) for worst-case tracking
            float minFps = 1.0f / max;
            var minAttrs = new Dictionary<string, string>(attributes) { ["aggregation"] = "min" };
            _exporter.RecordMetric("game.client.frame.rate.min", minFps, MetricType.Gauge, minAttrs);
        }

        private float ComputePercentile(float percentile)
        {
            if (_bufferCount == 0) return 0;

            // Copy to pre-allocated buffer and sort (no GC allocation)
            System.Array.Copy(_frameTimeBuffer, _sortBuffer, _bufferCount);
            System.Array.Sort(_sortBuffer, 0, _bufferCount);

            int index = Mathf.FloorToInt(percentile * (_bufferCount - 1));
            return _sortBuffer[index];
        }
    }

    /// <summary>
    /// MonoBehaviour to receive Update calls for frame timing.
    /// </summary>
    internal class FrameUpdater : MonoBehaviour
    {
        private FrameMetricsCollector _collector;

        public void Initialize(FrameMetricsCollector collector)
        {
            _collector = collector;
        }

        private void Update()
        {
            _collector?.OnUpdate();
        }
    }
}
