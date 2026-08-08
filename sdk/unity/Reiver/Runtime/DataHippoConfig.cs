using UnityEngine;

namespace DataHippo
{
    /// <summary>
    /// Configuration for the DataHippo SDK.
    /// Create via Assets > Create > DataHippo > Configuration.
    /// </summary>
    [CreateAssetMenu(fileName = "DataHippoConfig", menuName = "DataHippo/Configuration")]
    public class DataHippoConfig : ScriptableObject
    {
        [Header("Connection")]
        [Tooltip("DataHippo API endpoint URL")]
        public string apiUrl = "https://api.datahippo.io";

        [Tooltip("Project API key from DataHippo dashboard")]
        public string projectKey = "";

        [Header("Game Info")]
        [Tooltip("Name of your game")]
        public string gameName = "";

        [Tooltip("Target platform (auto-detected if empty)")]
        public string platform = "";

        [Header("Telemetry Settings")]
        [Tooltip("Enable automatic FPS/frame time tracking")]
        public bool enableFrameMetrics = true;

        [Tooltip("Enable automatic memory usage tracking")]
        public bool enableMemoryMetrics = true;

        [Tooltip("Enable automatic network quality tracking")]
        public bool enableNetworkMetrics = true;

        [Tooltip("Enable crash and ANR reporting")]
        public bool enableCrashReporting = true;

        [Tooltip("Enable scene load time tracking")]
        public bool enableSceneLoadTracking = true;

        [Header("Sampling")]
        [Tooltip("Metrics collection interval in seconds")]
        [Range(0.1f, 10f)]
        public float metricsIntervalSeconds = 1.0f;

        [Tooltip("Batch size for sending telemetry")]
        [Range(10, 1000)]
        public int batchSize = 100;

        [Tooltip("Max queue size before dropping old data")]
        [Range(100, 10000)]
        public int maxQueueSize = 1000;

        [Header("Privacy")]
        [Tooltip("Include device ID in telemetry")]
        public bool sendDeviceId = true;

        [Tooltip("Include player ID in telemetry (requires manual setup)")]
        public bool sendPlayerId = false;

        /// <summary>
        /// Validates the configuration.
        /// </summary>
        public bool Validate(out string error)
        {
            if (string.IsNullOrEmpty(projectKey))
            {
                error = "Project key is required";
                return false;
            }

            if (string.IsNullOrEmpty(apiUrl))
            {
                error = "API URL is required";
                return false;
            }

            error = null;
            return true;
        }
    }
}
