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
}
#nullable disable
