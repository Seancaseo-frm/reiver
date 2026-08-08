using System;
using System.Collections.Generic;
using UnityEngine;

namespace DataHippo
{
    /// <summary>
    /// Captures and reports crashes and unhandled exceptions.
    /// </summary>
    internal class CrashReporter
    {
        private readonly DataHippoConfig _config;
        private readonly TelemetryExporter _exporter;
        private readonly SessionTracker _sessionTracker;
        private bool _enabled;

        public CrashReporter(DataHippoConfig config, TelemetryExporter exporter, SessionTracker sessionTracker)
        {
            _config = config;
            _exporter = exporter;
            _sessionTracker = sessionTracker;
        }

        /// <summary>
        /// Enable crash reporting.
        /// </summary>
        public void Enable()
        {
            if (_enabled) return;
            _enabled = true;

            Application.logMessageReceived += OnLogMessageReceived;
            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;

            Debug.Log("[DataHippo] Crash reporting enabled");
        }

        /// <summary>
        /// Disable crash reporting.
        /// </summary>
        public void Disable()
        {
            if (!_enabled) return;
            _enabled = false;

            Application.logMessageReceived -= OnLogMessageReceived;
            AppDomain.CurrentDomain.UnhandledException -= OnUnhandledException;
        }

        private void OnLogMessageReceived(string condition, string stackTrace, LogType type)
        {
            // Only capture errors and exceptions
            if (type != LogType.Error && type != LogType.Exception && type != LogType.Assert)
            {
                return;
            }

            ReportError(condition, stackTrace, type.ToString());
        }

        private void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            var exception = e.ExceptionObject as Exception;
            if (exception != null)
            {
                ReportError(
                    exception.Message,
                    exception.StackTrace,
                    "UnhandledException",
                    exception.GetType().Name
                );
            }

            // If this is a fatal crash, try to end the session
            if (e.IsTerminating)
            {
                _sessionTracker?.EndSession("crash");
                _exporter?.Flush();
            }
        }

        private void ReportError(string message, string stackTrace, string logType, string exceptionType = null)
        {
            var attributes = new Dictionary<string, object>
            {
                ["game.session.id"] = _sessionTracker?.SessionId,
                ["error.message"] = TruncateString(message, 1000),
                ["error.stack_trace"] = TruncateString(stackTrace, 4000),
                ["error.type"] = logType,
                ["game.version"] = Application.version,
                ["game.platform"] = PlatformUtils.GetPlatformString(),
                ["device.model.name"] = SystemInfo.deviceModel,
                ["os.description"] = SystemInfo.operatingSystem
            };

            if (!string.IsNullOrEmpty(exceptionType))
            {
                attributes["exception.type"] = exceptionType;
            }

            if (!string.IsNullOrEmpty(DataHippoSDK.CurrentMatchId))
            {
                attributes["game.match.id"] = DataHippoSDK.CurrentMatchId;
            }

            var playerId = _sessionTracker?.GetPlayerId();
            if (!string.IsNullOrEmpty(playerId))
            {
                attributes["game.player.id"] = playerId;
            }

            // Compute a fingerprint for grouping similar errors
            attributes["error.fingerprint"] = ComputeFingerprint(message, stackTrace, exceptionType);

            _exporter.RecordEvent("game.client.error", attributes);

            // For actual exceptions/crashes, also increment counter
            if (logType == "Exception" || logType == "UnhandledException")
            {
                var counterAttrs = new Dictionary<string, string>
                {
                    ["error.type"] = exceptionType ?? "unknown"
                };
                _exporter.RecordMetric("game.client.crash.count", 1, MetricType.Counter, counterAttrs);
            }
        }

        private static string ComputeFingerprint(string message, string stackTrace, string exceptionType)
        {
            // Simple fingerprinting: use exception type + first line of stack trace
            var firstStackLine = "";
            if (!string.IsNullOrEmpty(stackTrace))
            {
                var lines = stackTrace.Split('\n');
                if (lines.Length > 0)
                {
                    firstStackLine = lines[0].Trim();
                    // Remove line numbers for grouping
                    var colonIndex = firstStackLine.LastIndexOf(':');
                    if (colonIndex > 0)
                    {
                        firstStackLine = firstStackLine.Substring(0, colonIndex);
                    }
                }
            }

            var input = $"{exceptionType ?? "error"}:{firstStackLine}";
            return ComputeHash(input);
        }

        private static string ComputeHash(string input)
        {
            using (var sha = System.Security.Cryptography.SHA256.Create())
            {
                var bytes = System.Text.Encoding.UTF8.GetBytes(input);
                var hash = sha.ComputeHash(bytes);
                return BitConverter.ToString(hash).Replace("-", "").Substring(0, 16).ToLower();
            }
        }

        private static string TruncateString(string s, int maxLength)
        {
            if (string.IsNullOrEmpty(s)) return "";
            return s.Length <= maxLength ? s : s.Substring(0, maxLength) + "...";
        }
    }
}
