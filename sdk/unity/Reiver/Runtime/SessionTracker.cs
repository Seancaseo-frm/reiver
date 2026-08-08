using System;
using System.Collections.Generic;
using UnityEngine;

namespace DataHippo
{
    /// <summary>
    /// Tracks player sessions with start/end times and quality metrics.
    /// </summary>
    internal class SessionTracker
    {
        private readonly DataHippoConfig _config;
        private readonly TelemetryExporter _exporter;
        private DateTime _sessionStartTime;
        private string _playerId;

        public string SessionId { get; private set; }

        public SessionTracker(DataHippoConfig config, TelemetryExporter exporter)
        {
            _config = config;
            _exporter = exporter;
        }

        /// <summary>
        /// Start a new session.
        /// </summary>
        public void StartSession()
        {
            SessionId = Guid.NewGuid().ToString();
            _sessionStartTime = DateTime.UtcNow;

            var attributes = new Dictionary<string, object>
            {
                ["game.session.id"] = SessionId,
                ["game.version"] = Application.version,
                ["game.platform"] = PlatformUtils.GetPlatformString(),

                // Device info
                ["device.model.name"] = SystemInfo.deviceModel,
                ["device.manufacturer"] = PlatformUtils.GetDeviceManufacturer(),
                ["os.type"] = SystemInfo.operatingSystemFamily.ToString().ToLower(),
                ["os.description"] = SystemInfo.operatingSystem,

                // Hardware info
                ["hw.gpu.vendor"] = SystemInfo.graphicsDeviceVendor,
                ["hw.gpu.model"] = SystemInfo.graphicsDeviceName,
                ["system.memory_mb"] = SystemInfo.systemMemorySize,
                ["gpu.memory_mb"] = SystemInfo.graphicsMemorySize
            };

            if (_config.sendDeviceId)
            {
                attributes["device.id"] = SystemInfo.deviceUniqueIdentifier;
            }

            _exporter.RecordEvent("game.session.start", attributes);

            Debug.Log($"[DataHippo] Session started: {SessionId}");
        }

        /// <summary>
        /// End the current session.
        /// </summary>
        public void EndSession(string reason)
        {
            if (string.IsNullOrEmpty(SessionId)) return;

            var duration = DateTime.UtcNow - _sessionStartTime;

            var attributes = new Dictionary<string, object>
            {
                ["game.session.id"] = SessionId,
                ["game.session.duration"] = duration.TotalSeconds,
                ["game.session.end_reason"] = reason
            };

            if (!string.IsNullOrEmpty(_playerId))
            {
                attributes["game.player.id"] = _playerId;
            }

            _exporter.RecordEvent("game.session.end", attributes);

            Debug.Log($"[DataHippo] Session ended: {SessionId} ({reason}), duration: {duration.TotalSeconds:F1}s");
        }

        /// <summary>
        /// Set the player ID for this session.
        /// </summary>
        public void SetPlayerId(string playerId)
        {
            _playerId = playerId;
        }

        /// <summary>
        /// Get the current player ID.
        /// </summary>
        public string GetPlayerId() => _playerId;

        /// <summary>
        /// Get session duration so far.
        /// </summary>
        public TimeSpan GetSessionDuration() => DateTime.UtcNow - _sessionStartTime;
    }
}
