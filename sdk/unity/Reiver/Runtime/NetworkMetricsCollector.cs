using System.Collections.Generic;
using UnityEngine;

namespace DataHippo
{
    /// <summary>
    /// Collects network quality metrics: RTT, jitter, packet loss.
    /// Game code should report RTT/packet loss from their networking layer.
    /// </summary>
    internal class NetworkMetricsCollector
    {
        private readonly DataHippoConfig _config;
        private readonly TelemetryExporter _exporter;

        // Ring buffers for computing statistics
        private readonly double[] _rttBuffer;
        private readonly double[] _packetLossBuffer;
        private int _rttIndex;
        private int _rttCount;
        private int _packetLossIndex;
        private int _packetLossCount;

        private float _timeSinceLastReport;
        private GameObject _updater;
        private bool _running;

        private const int BufferSize = 60; // 1 minute of samples at 1/sec

        public NetworkMetricsCollector(DataHippoConfig config, TelemetryExporter exporter)
        {
            _config = config;
            _exporter = exporter;
            _rttBuffer = new double[BufferSize];
            _packetLossBuffer = new double[BufferSize];
        }

        public void Start()
        {
            if (_running) return;
            _running = true;

            _updater = new GameObject("DataHippo_NetworkCollector")
            {
                hideFlags = HideFlags.HideAndDontSave
            };
            Object.DontDestroyOnLoad(_updater);

            var component = _updater.AddComponent<NetworkUpdater>();
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

        /// <summary>
        /// Report RTT measurement (in seconds, per OTel v1.39.0).
        /// Call this from your networking code with measured RTT.
        /// </summary>
        public void ReportRtt(double rttSeconds)
        {
            if (!_running) return;

            _rttBuffer[_rttIndex] = rttSeconds;
            _rttIndex = (_rttIndex + 1) % BufferSize;
            if (_rttCount < BufferSize) _rttCount++;
        }

        /// <summary>
        /// Report packet loss ratio (0.0 to 1.0).
        /// </summary>
        public void ReportPacketLoss(double lossRatio)
        {
            if (!_running) return;

            // Clamp to valid range
            lossRatio = System.Math.Max(0, System.Math.Min(1, lossRatio));

            _packetLossBuffer[_packetLossIndex] = lossRatio;
            _packetLossIndex = (_packetLossIndex + 1) % BufferSize;
            if (_packetLossCount < BufferSize) _packetLossCount++;
        }

        internal void OnUpdate()
        {
            if (!_running) return;

            _timeSinceLastReport += Time.unscaledDeltaTime;

            // Report at configured interval
            if (_timeSinceLastReport >= _config.metricsIntervalSeconds)
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

            // Report RTT metrics
            if (_rttCount > 0)
            {
                double sumRtt = 0;
                double minRtt = double.MaxValue;
                double maxRtt = double.MinValue;

                for (int i = 0; i < _rttCount; i++)
                {
                    double rtt = _rttBuffer[i];
                    sumRtt += rtt;
                    if (rtt < minRtt) minRtt = rtt;
                    if (rtt > maxRtt) maxRtt = rtt;
                }

                double avgRtt = sumRtt / _rttCount;

                // Compute jitter (RTT variance)
                double sumSquaredDiff = 0;
                for (int i = 0; i < _rttCount; i++)
                {
                    double diff = _rttBuffer[i] - avgRtt;
                    sumSquaredDiff += diff * diff;
                }
                double jitter = System.Math.Sqrt(sumSquaredDiff / _rttCount);

                // Record RTT histogram
                _exporter.RecordMetric("game.network.rtt", avgRtt, MetricType.Histogram, attributes);

                // Record jitter
                _exporter.RecordMetric("game.network.jitter", jitter, MetricType.Gauge, attributes);

                // Record P95 RTT
                double p95Rtt = ComputePercentile(_rttBuffer, _rttCount, 0.95);
                var p95Attrs = new Dictionary<string, string>(attributes) { ["percentile"] = "95" };
                _exporter.RecordMetric("game.network.rtt.p95", p95Rtt, MetricType.Gauge, p95Attrs);
            }

            // Report packet loss
            if (_packetLossCount > 0)
            {
                double sumLoss = 0;
                for (int i = 0; i < _packetLossCount; i++)
                {
                    sumLoss += _packetLossBuffer[i];
                }
                double avgLoss = sumLoss / _packetLossCount;

                _exporter.RecordMetric("game.network.packet_loss", avgLoss, MetricType.Gauge, attributes);
            }
        }

        private static double ComputePercentile(double[] buffer, int count, double percentile)
        {
            if (count == 0) return 0;

            double[] sorted = new double[count];
            System.Array.Copy(buffer, sorted, count);
            System.Array.Sort(sorted);

            int index = (int)(percentile * (count - 1));
            return sorted[index];
        }
    }

    internal class NetworkUpdater : MonoBehaviour
    {
        private NetworkMetricsCollector _collector;

        public void Initialize(NetworkMetricsCollector collector)
        {
            _collector = collector;
        }

        private void Update()
        {
            _collector?.OnUpdate();
        }
    }
}
