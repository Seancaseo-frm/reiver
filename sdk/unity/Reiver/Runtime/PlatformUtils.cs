using UnityEngine;

namespace DataHippo
{
    /// <summary>
    /// Shared platform utility functions used across the SDK.
    /// Centralizes platform detection and device information.
    /// </summary>
    internal static class PlatformUtils
    {
        /// <summary>
        /// Get the current platform as a lowercase string identifier.
        /// Follows OpenTelemetry semantic conventions.
        /// </summary>
        public static string GetPlatformString()
        {
            return Application.platform switch
            {
                RuntimePlatform.Android => "android",
                RuntimePlatform.IPhonePlayer => "ios",
                RuntimePlatform.WindowsPlayer or RuntimePlatform.WindowsEditor => "windows",
                RuntimePlatform.OSXPlayer or RuntimePlatform.OSXEditor => "macos",
                RuntimePlatform.LinuxPlayer or RuntimePlatform.LinuxEditor => "linux",
                RuntimePlatform.WebGLPlayer => "web",
                RuntimePlatform.PS4 or RuntimePlatform.PS5 => "playstation",
                RuntimePlatform.XboxOne or RuntimePlatform.GameCoreXboxOne or RuntimePlatform.GameCoreXboxSeries => "xbox",
                RuntimePlatform.Switch => "switch",
                RuntimePlatform.tvOS => "tvos",
                RuntimePlatform.Stadia => "stadia",
                _ => "unknown"
            };
        }

        /// <summary>
        /// Get the CPU architecture as a string.
        /// </summary>
        public static string GetArchitecture()
        {
            return System.IntPtr.Size == 8 ? "x86_64" : "x86";
        }

        /// <summary>
        /// Get the device manufacturer from the device model.
        /// </summary>
        public static string GetDeviceManufacturer()
        {
            var model = SystemInfo.deviceModel;
            if (string.IsNullOrEmpty(model)) return "unknown";
            
            var parts = model.Split(' ');
            return parts.Length > 0 ? parts[0] : "unknown";
        }

        /// <summary>
        /// Check if the platform is a mobile device.
        /// </summary>
        public static bool IsMobilePlatform()
        {
            return Application.platform == RuntimePlatform.Android ||
                   Application.platform == RuntimePlatform.IPhonePlayer;
        }

        /// <summary>
        /// Check if the platform is a console.
        /// </summary>
        public static bool IsConsolePlatform()
        {
            return Application.platform == RuntimePlatform.PS4 ||
                   Application.platform == RuntimePlatform.PS5 ||
                   Application.platform == RuntimePlatform.XboxOne ||
                   Application.platform == RuntimePlatform.GameCoreXboxOne ||
                   Application.platform == RuntimePlatform.GameCoreXboxSeries ||
                   Application.platform == RuntimePlatform.Switch;
        }

        /// <summary>
        /// Check if the platform is a desktop OS.
        /// </summary>
        public static bool IsDesktopPlatform()
        {
            return Application.platform == RuntimePlatform.WindowsPlayer ||
                   Application.platform == RuntimePlatform.WindowsEditor ||
                   Application.platform == RuntimePlatform.OSXPlayer ||
                   Application.platform == RuntimePlatform.OSXEditor ||
                   Application.platform == RuntimePlatform.LinuxPlayer ||
                   Application.platform == RuntimePlatform.LinuxEditor;
        }
    }
}
