using FlinkDotNet.Core.Abstractions.Serializers;
using FlinkDotNet.Core.Abstractions.Common; // For Time
using FlinkDotNet.Core.Abstractions.Windowing; // For TimeWindow, Window, Trigger

namespace FlinkDotNet.Core.Api.Windowing
{
    public class TimeWindowSerializer : ITypeSerializer<TimeWindow>
    {
        public byte[] Serialize(TimeWindow obj)
        {
            using var ms = new MemoryStream(16); // Start + End = 2 * long (8 bytes each)
            using var writer = new BinaryWriter(ms);
            writer.Write(obj.Start);
            writer.Write(obj.End);
            return ms.ToArray();
        }

        public TimeWindow Deserialize(byte[] bytes)
        {
            using var ms = new MemoryStream(bytes);
            using var reader = new BinaryReader(ms);
            long start = reader.ReadInt64();
            long end = reader.ReadInt64();
            return new TimeWindow(start, end);
        }
    }


    public class TumblingEventTimeWindows<TElement> : IWindowAssigner<TElement, TimeWindow>
    {
        public Time Size { get; }
        public Time Offset { get; }

        private TumblingEventTimeWindows(Time size, Time offset)
        {
            if (size.Milliseconds <= 0)
                throw new ArgumentOutOfRangeException(nameof(size), "Tumbling window size must be positive.");
            Size = size;
            Offset = offset;
        }

        /// <summary>
        /// Creates a new TumblingEventTimeWindows assigner with the given window size.
        /// Windows are aligned with the epoch (00:00:00 UTC on 1 January 1970).
        /// </summary>
        public static TumblingEventTimeWindows<TElement> Of(Time size) =>
            new TumblingEventTimeWindows<TElement>(size, Time.MillisecondsMethod(0));

        /// <summary>
        /// Creates a new TumblingEventTimeWindows assigner with the given window size and offset.
        /// Windows can be aligned to a specific offset from the epoch (e.g., for daily windows starting at local midnight).
        /// </summary>
        public static TumblingEventTimeWindows<TElement> Of(Time size, Time offset) =>
            new TumblingEventTimeWindows<TElement>(size, offset);

        public ICollection<TimeWindow> AssignWindows(TElement element, long timestamp, IWindowAssignerContext context)
        {
            if (timestamp > long.MinValue) // Valid timestamp for event time
            {
                long windowStart = TimeWindow.GetWindowStartWithOffset(timestamp, Offset.Milliseconds, Size.Milliseconds);
                return new List<TimeWindow> { new TimeWindow(windowStart, windowStart + Size.Milliseconds) };
            }
            // Should not happen if timestamps are correctly assigned before this assigner in event time.
            // Or, if this is used incorrectly with processing time source, this would be an issue.
            throw new ArgumentOutOfRangeException(nameof(timestamp),
                $"Invalid timestamp for event time processing: {timestamp}. Ensure timestamps are properly assigned.");
        }

        public Trigger<TElement, TimeWindow> GetDefaultTrigger(StreamExecutionEnvironment environment)
        {
            return new EventTimeTrigger<TElement, TimeWindow>();
        }

        public ITypeSerializer<TimeWindow> GetWindowSerializer() => new TimeWindowSerializer();

        public bool IsEventTime => true;

        public string ToString() => $"TumblingEventTimeWindows({Size.Milliseconds}ms, {Offset.Milliseconds}ms)";
    }
}
