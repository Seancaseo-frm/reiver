using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Profiling;

namespace DataHippo
{
    /// <summary>
    /// Collects memory usage metrics.
    /// Reports at configured intervals to avoid performance impact.
    /// </summary>
    internal class MemoryMetricsCollector
    {
        private readonly DataHippoConfig _config;
        private readonly TelemetryExporter _exporter;
        private float _timeSinceLastReport;
        private long _peakMemory;
        private GameObject _updater;
        private bool _running;

        public MemoryMetricsCollector(DataHippoConfig config, TelemetryExporter exporter)
        {
            _config = config;
            _exporter = exporter;
        }

        public void Start()
        {
            if (_running) return;
            _running = true;

            _updater = new GameObject("DataHippo_MemoryCollector")
            {
                hideFlags = HideFlags.HideAndDontSave
            };
            Object.DontDestroyOnLoad(_updater);

            var component = _updater.AddComponent<MemoryUpdater>();
            component.Initialize(this);

            // Register for memory warning callback (mobile platforms)
            Application.lowMemory += OnLowMemory;
        }

        public void Stop()
        {
            _running = false;
            Application.lowMemory -= OnLowMemory;

            if (_updater != null)
            {
                Object.Destroy(_updater);
                _updater = null;
            }
        }

        internal void OnUpdate()
        {
            if (!_running) return;

            _timeSinceLastReport += Time.unscaledDeltaTime;

            // Report at configured interval (memory queries can be expensive)
            if (_timeSinceLastReport >= _config.metricsIntervalSeconds * 2) // Less frequent than frame metrics
            {
                ReportMetrics();
                _timeSinceLastReport = 0;
            }
        }

        private void ReportMetrics()
        {
            var attributes = new Dictionary<string, string>();

            if (!string.IsNullOrEmpty(DataHippoSDK.CurrentMatchId))
            {
                attributes["game.match.id"] = DataHippoSDK.CurrentMatchId;
            }

            // Total allocated memory (bytes, per OTel convention)
            long totalMemory = Profiler.GetTotalAllocatedMemoryLong();
            _exporter.RecordMetric("game.client.memory.usage", totalMemory, MetricType.Gauge, attributes);

            // Track peak memory
            if (totalMemory > _peakMemory)
            {
                _peakMemory = totalMemory;
            }

            // Reserved memory
            long reservedMemory = Profiler.GetTotalReservedMemoryLong();
            var reservedAttrs = new Dictionary<string, string>(attributes) { ["type"] = "reserved" };
            _exporter.RecordMetric("game.client.memory.reserved", reservedMemory, MetricType.Gauge, reservedAttrs);

            // Mono heap size (managed memory)
            long monoHeap = Profiler.GetMonoHeapSizeLong();
            var monoAttrs = new Dictionary<string, string>(attributes) { ["type"] = "managed" };
            _exporter.RecordMetric("game.client.memory.managed", monoHeap, MetricType.Gauge, monoAttrs);

            // Graphics memory (if available)
            long gfxMemory = Profiler.GetAllocatedMemoryForGraphicsDriver();
            if (gfxMemory > 0)
            {
                var gfxAttrs = new Dictionary<string, string>(attributes) { ["type"] = "graphics" };
                _exporter.RecordMetric("game.client.memory.graphics", gfxMemory, MetricType.Gauge, gfxAttrs);
            }
        }

        private void OnLowMemory()
        {
            var attributes = new Dictionary<string, object>
            {
                ["game.session.id"] = DataHippoSDK.SessionId,
                ["memory.total"] = Profiler.GetTotalAllocatedMemoryLong(),
                ["memory.peak"] = _peakMemory
            };

            if (!string.IsNullOrEmpty(DataHippoSDK.CurrentMatchId))
            {
                attributes["game.match.id"] = DataHippoSDK.CurrentMatchId;
            }

            _exporter.RecordEvent("game.client.memory.low", attributes);
        }

        /// <summary>
        /// Get peak memory usage during this session.
        /// </summary>
        public long GetPeakMemory() => _peakMemory;
    }

    internal class MemoryUpdater : MonoBehaviour
    {
        private MemoryMetricsCollector _collector;

        public void Initialize(MemoryMetricsCollector collector)
        {
            _collector = collector;
        }

        private void Update()
        {
            _collector?.OnUpdate();
        }
    }
}
