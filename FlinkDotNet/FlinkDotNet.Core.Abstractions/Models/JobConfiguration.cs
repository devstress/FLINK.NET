using System.Collections.Generic;
using System.Globalization;

namespace FlinkDotNet.Core.Abstractions.Models
{
    /// <summary>
    /// Represents the configuration parameters for a job.
    /// This is a placeholder and can be expanded to include specific
    /// job-level settings, user parameters, etc.
    /// Flinks equivalent is org.apache.flink.api.common.ExecutionConfig
    /// and the Configuration object passed to RichFunctions.
    /// </summary>
    /// <summary>
    /// Simple mutable job configuration used by tests.
    /// </summary>
    public class JobConfiguration
    {
        private readonly Dictionary<string, string> _settings = new();

        public void SetString(string key, string value) => _settings[key] = value;

        public string? GetString(string key, string? defaultValue = null)
            => _settings.TryGetValue(key, out var v) ? v : defaultValue;

        public void SetInt(string key, int value) =>
            _settings[key] = value.ToString(CultureInfo.InvariantCulture);

        public int GetInt(string key, int defaultValue = 0)
            => _settings.TryGetValue(key, out var v) && int.TryParse(v, out var i) ? i : defaultValue;
    }
}
