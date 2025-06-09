using System;
using FlinkDotNet.Core.Abstractions.Sinks;

namespace FlinkDotNet.TaskManager
{
    /// <summary>
    /// Minimal sink context used when invoking sink functions in tests or simplified flows.
    /// </summary>
    public class SimpleSinkContext : ISinkContext
    {
        public long CurrentProcessingTimeMillis() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}
