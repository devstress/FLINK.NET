#nullable enable
using System; // Already present, but ensuring it's there for Console.WriteLine
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Context; // For IRuntimeContext

namespace FlinkDotNet.Connectors.Sinks.Console
{
    public class ConsoleSinkFunction<TIn> : ISinkFunction<TIn>
    {
        private string _taskName = "ConsoleSink"; // Default, can be updated by Open

        public void Open(IRuntimeContext context)
        {
            _taskName = context.TaskName;
            System.Console.WriteLine($"[{_taskName}] ConsoleSinkFunction opened."); // Explicitly System.Console
        }

        public void Invoke(TIn record, ISinkContext context)
        {
            // Consider using a specific serializer if TIn is not string,
            // or rely on record.ToString() for simplicity in this basic sink.
            // For a true generic sink, one might pass a serializer to format TIn.
            System.Console.WriteLine($"[{_taskName} @ {context.CurrentProcessingTimeMillis()}ms] {record?.ToString() ?? "null"}"); // Explicitly System.Console
        }

        public void Close()
        {
            System.Console.WriteLine($"[{_taskName}] ConsoleSinkFunction closed."); // Explicitly System.Console
        }
    }

    // Implementation for ISinkContext for ConsoleSink - can be very basic or live elsewhere
    // For now, let's assume a basic context is provided by the TaskManager when executing the sink.
    // If ISinkContext needs to be more elaborate, it might be better to have a default implementation
    // in Core.Abstractions or provided by the runtime.

    // Example of a simple SinkContext that could be used by the TaskManager when calling Invoke.
    // This might not need to be in this file but shows what ISinkContext requires.
    // public class BasicSinkContext : ISinkContext
    // {
    //     public long CurrentProcessingTimeMillis() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    // }
}
#nullable disable
