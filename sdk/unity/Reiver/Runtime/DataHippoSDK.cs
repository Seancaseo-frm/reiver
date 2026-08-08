using System;
using System.Collections.Generic;
using UnityEngine;

namespace DataHippo
{
    /// <summary>
    /// Main entry point for the DataHippo SDK.
    /// Initializes telemetry collection and provides APIs for custom instrumentation.
    /// </summary>
    public static class DataHippoSDK
    {
        private static bool _initialized;
        private static DataHippoConfig _config;
        private static TelemetryExporter _exporter;
        private static FrameMetricsCollector _frameCollector;
        private static MemoryMetricsCollector _memoryCollector;
        private static NetworkMetricsCollector _networkCollector;
        private static SessionTracker _sessionTracker;
        private static CrashReporter _crashReporter;

        /// <summary>
        /// Current session ID for this game session.
        /// </summary>
        public static string SessionId => _sessionTracker?.SessionId;

        /// <summary>
        /// Current match ID (null if not in a match).
        /// </summary>
        public static string CurrentMatchId { get; private set; }

        /// <summary>
        /// Whether the SDK is initialized and running.
        /// </summary>
        public static bool IsInitialized => _initialized;

        /// <summary>
        /// Initialize the DataHippo SDK with configuration.
        /// Call this early in your game's lifecycle (e.g., in a bootstrap scene).
        /// </summary>
        public static void Initialize(DataHippoConfig config)
        {
            if (_initialized)
            {
                Debug.LogWarning("[DataHippo] SDK already initialized");
                return;
            }

            if (config == null)
            {
                Debug.LogError("[DataHippo] Configuration is required");
                return;
            }

            if (!config.Validate(out var error))
            {
                Debug.LogError($"[DataHippo] Invalid configuration: {error}");
                return;
            }

            _config = config;

            // Create exporter for sending telemetry
            _exporter = new TelemetryExporter(config);

            // Initialize session tracker
            _sessionTracker = new SessionTracker(config, _exporter);
            _sessionTracker.StartSession();

            // Initialize collectors based on config
            if (config.enableFrameMetrics)
            {
                _frameCollector = new FrameMetricsCollector(config, _exporter);
                _frameCollector.Start();
            }

            if (config.enableMemoryMetrics)
            {
                _memoryCollector = new MemoryMetricsCollector(config, _exporter);
                _memoryCollector.Start();
            }

            if (config.enableNetworkMetrics)
            {
                _networkCollector = new NetworkMetricsCollector(config, _exporter);
                _networkCollector.Start();
            }

            if (config.enableCrashReporting)
            {
                _crashReporter = new CrashReporter(config, _exporter, _sessionTracker);
                _crashReporter.Enable();
            }

            if (config.enableSceneLoadTracking)
            {
                UnityEngine.SceneManagement.SceneManager.sceneLoaded += OnSceneLoaded;
            }

            _initialized = true;
            Debug.Log($"[DataHippo] SDK initialized for {config.gameName}");
        }

        /// <summary>
        /// Shutdown the SDK and flush remaining telemetry.
        /// </summary>
        public static void Shutdown()
        {
            if (!_initialized) return;

            _frameCollector?.Stop();
            _memoryCollector?.Stop();
            _networkCollector?.Stop();
            _crashReporter?.Disable();
            _sessionTracker?.EndSession("normal");
            _exporter?.Flush();

            UnityEngine.SceneManagement.SceneManager.sceneLoaded -= OnSceneLoaded;

            _initialized = false;
            Debug.Log("[DataHippo] SDK shutdown complete");
        }

        #region Match Tracking

        /// <summary>
        /// Start tracking a match. Call when a match/game session begins.
        /// </summary>
        public static void StartMatch(string matchId, string mode = null, string map = null, Dictionary<string, string> properties = null)
        {
            if (!_initialized)
            {
                Debug.LogWarning("[DataHippo] SDK not initialized");
                return;
            }

            CurrentMatchId = matchId;

            var attributes = new Dictionary<string, object>
            {
                ["game.match.id"] = matchId,
                ["game.session.id"] = SessionId
            };

            if (!string.IsNullOrEmpty(mode))
                attributes["game.match.mode"] = mode;
            if (!string.IsNullOrEmpty(map))
                attributes["game.match.map"] = map;

            if (properties != null)
            {
                foreach (var kvp in properties)
                    attributes[kvp.Key] = kvp.Value;
            }

            _exporter.RecordEvent("game.match.start", attributes);
        }

        /// <summary>
        /// End the current match. Call when match concludes.
        /// </summary>
        public static void EndMatch(string outcome = null, string winningTeam = null)
        {
            if (!_initialized || string.IsNullOrEmpty(CurrentMatchId))
            {
                return;
            }

            var attributes = new Dictionary<string, object>
            {
                ["game.match.id"] = CurrentMatchId,
                ["game.session.id"] = SessionId
            };

            if (!string.IsNullOrEmpty(outcome))
                attributes["game.match.outcome"] = outcome;
            if (!string.IsNullOrEmpty(winningTeam))
                attributes["game.match.winning_team"] = winningTeam;

            _exporter.RecordEvent("game.match.end", attributes);
            CurrentMatchId = null;
        }

        #endregion

        #region Player Tracking

        /// <summary>
        /// Set the current player ID for telemetry correlation.
        /// Use a pseudonymous identifier, not PII.
        /// </summary>
        public static void SetPlayerId(string playerId)
        {
            if (!_initialized) return;
            _sessionTracker?.SetPlayerId(playerId);
        }

        /// <summary>
        /// Record player joining a match.
        /// </summary>
        public static void RecordPlayerJoin(string team = null)
        {
            if (!_initialized || string.IsNullOrEmpty(CurrentMatchId)) return;

            var attributes = new Dictionary<string, object>
            {
                ["game.match.id"] = CurrentMatchId,
                ["game.session.id"] = SessionId
            };

            if (!string.IsNullOrEmpty(team))
                attributes["game.player.team"] = team;

            _exporter.RecordEvent("game.player.join", attributes);
        }

        /// <summary>
        /// Record player leaving a match.
        /// </summary>
        public static void RecordPlayerLeave(string reason = null)
        {
            if (!_initialized || string.IsNullOrEmpty(CurrentMatchId)) return;

            var attributes = new Dictionary<string, object>
            {
                ["game.match.id"] = CurrentMatchId,
                ["game.session.id"] = SessionId
            };

            if (!string.IsNullOrEmpty(reason))
                attributes["game.player.leave_reason"] = reason;

            _exporter.RecordEvent("game.player.leave", attributes);
        }

        #endregion

        #region Custom Metrics

        /// <summary>
        /// Record a custom gauge metric.
        /// </summary>
        public static void RecordGauge(string name, double value, Dictionary<string, string> attributes = null)
        {
            if (!_initialized) return;
            _exporter.RecordMetric(name, value, MetricType.Gauge, attributes);
        }

        /// <summary>
        /// Record a custom histogram metric (e.g., for durations).
        /// </summary>
        public static void RecordHistogram(string name, double value, Dictionary<string, string> attributes = null)
        {
            if (!_initialized) return;
            _exporter.RecordMetric(name, value, MetricType.Histogram, attributes);
        }

        /// <summary>
        /// Increment a counter metric.
        /// </summary>
        public static void IncrementCounter(string name, long delta = 1, Dictionary<string, string> attributes = null)
        {
            if (!_initialized) return;
            _exporter.RecordMetric(name, delta, MetricType.Counter, attributes);
        }

        #endregion

        #region Custom Spans

        /// <summary>
        /// Start a custom span for tracing an operation.
        /// Returns a disposable that ends the span when disposed.
        /// </summary>
        public static IDisposable StartSpan(string name, Dictionary<string, string> attributes = null)
        {
            if (!_initialized) return new NoOpDisposable();
            return _exporter.StartSpan(name, attributes);
        }

        #endregion

        #region Network Quality

        /// <summary>
        /// Report current network RTT (round-trip time) in seconds.
        /// Call this periodically with your game's measured RTT.
        /// </summary>
        public static void ReportNetworkRtt(double rttSeconds)
        {
            if (!_initialized) return;
            _networkCollector?.ReportRtt(rttSeconds);
        }

        /// <summary>
        /// Report packet loss ratio (0.0 to 1.0).
        /// </summary>
        public static void ReportPacketLoss(double lossRatio)
        {
            if (!_initialized) return;
            _networkCollector?.ReportPacketLoss(lossRatio);
        }

        #endregion

        private static void OnSceneLoaded(UnityEngine.SceneManagement.Scene scene, UnityEngine.SceneManagement.LoadSceneMode mode)
        {
            if (!_initialized) return;

            var attributes = new Dictionary<string, object>
            {
                ["scene.name"] = scene.name,
                ["scene.build_index"] = scene.buildIndex,
                ["load_mode"] = mode.ToString()
            };

            if (!string.IsNullOrEmpty(CurrentMatchId))
                attributes["game.match.id"] = CurrentMatchId;

            _exporter.RecordEvent("game.scene.loaded", attributes);
        }

        private class NoOpDisposable : IDisposable
        {
            public void Dispose() { }
        }
    }
}
