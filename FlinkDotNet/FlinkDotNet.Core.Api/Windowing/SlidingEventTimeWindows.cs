using FlinkDotNet.Core.Abstractions.Common;
using FlinkDotNet.Core.Abstractions.Serializers;
using FlinkDotNet.Core.Abstractions.Windowing;

namespace FlinkDotNet.Core.Api.Windowing
{
    public class SlidingEventTimeWindows<TElement> : WindowAssigner<TElement, TimeWindow>
    {
        public Time Size { get; }
        public Time Slide { get; }
        public Time Offset { get; }

        private SlidingEventTimeWindows(Time size, Time slide, Time offset)
        {
            if (size.Milliseconds <= 0) throw new ArgumentOutOfRangeException(nameof(size));
            if (slide.Milliseconds <= 0) throw new ArgumentOutOfRangeException(nameof(slide));
            Size = size;
            Slide = slide;
            Offset = offset;
        }

        public static SlidingEventTimeWindows<TElement> Of(Time size, Time slide) =>
            new SlidingEventTimeWindows<TElement>(size, slide, Time.MillisecondsMethod(0));

        public static SlidingEventTimeWindows<TElement> Of(Time size, Time slide, Time offset) =>
            new SlidingEventTimeWindows<TElement>(size, slide, offset);

        public override ICollection<TimeWindow> AssignWindows(TElement element, long timestamp, IWindowAssignerContext context)
        {
            var windows = new List<TimeWindow>();
            long lastStart = TimeWindow.GetWindowStartWithOffset(timestamp, Offset.Milliseconds, Slide.Milliseconds);
            for (long start = lastStart; start > timestamp - Size.Milliseconds; start -= Slide.Milliseconds)
            {
                windows.Add(new TimeWindow(start, start + Size.Milliseconds));
            }
            return windows;
        }

        public override Trigger<TElement, TimeWindow> GetDefaultTrigger(StreamExecutionEnvironment environment)
            => new EventTimeTrigger<TElement, TimeWindow>();

        public override ITypeSerializer<TimeWindow> GetWindowSerializer() => new TimeWindowSerializer();

        public override bool IsEventTime => true;

        public override string ToString() => $"SlidingEventTimeWindows(size={Size.Milliseconds}ms, slide={Slide.Milliseconds}ms)";
    }
}
