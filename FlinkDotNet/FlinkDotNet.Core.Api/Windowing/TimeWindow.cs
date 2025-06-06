#nullable enable
using System;
using FlinkDotNet.Core.Api.Common; // For Time struct, if needed by static helpers, though not directly used in this version of TimeWindow properties

namespace FlinkDotNet.Core.Api.Windowing
{
    /// <summary>
    /// Represents a window defined by a start and end timestamp.
    /// The window typically includes elements with timestamps `T` where `Start <= T < End`.
    /// </summary>
    public class TimeWindow : Window
    {
        /// <summary>
        /// Gets the start timestamp of the window (inclusive).
        /// </summary>
        public long Start { get; }

        /// <summary>
        /// Gets the end timestamp of the window (exclusive).
        /// </summary>
        public long End { get; }

        public TimeWindow(long start, long end)
        {
            if (start >= end)
                throw new ArgumentException($"TimeWindow start timestamp {start} must be less than end timestamp {end}.");
            Start = start;
            End = end;
        }

        /// <summary>
        /// Returns the last millisecond that is still part of this window.
        /// This is `End - 1`.
        /// </summary>
        public override long MaxTimestamp() => End - 1;

        /// <summary>
        /// Checks if this window intersects with another TimeWindow.
        /// </summary>
        public bool Intersects(TimeWindow other)
        {
            return this.Start < other.End && this.End > other.Start;
        }

        /// <summary>
        /// Returns a new TimeWindow that covers both this window and the other window.
        /// </summary>
        public TimeWindow Cover(TimeWindow other)
        {
            return new TimeWindow(Math.Min(this.Start, other.Start), Math.Max(this.End, other.End));
        }

        public override bool Equals(object? obj)
        {
            return obj is TimeWindow window &&
                   Start == window.Start &&
                   End == window.End;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Start, End);
        }

        public override string ToString() => $"TimeWindow{{start={Start}, end={End}}}";

        /// <summary>
        /// Helper method to get the start of a window for a given timestamp, window size, and offset.
        /// This is often used by tumbling and sliding window assigners.
        /// </summary>
        /// <param name="timestamp">The timestamp to assign to a window.</param>
        /// <param name="offset">The offset to apply to the window alignment (e.g., for timezone or daily windows not starting at epoch 0). Should be less than windowSize.</param>
        /// <param name="windowSize">The size of the window (must be > 0).</param>
        /// <returns>The start timestamp of the window.</returns>
        public static long GetWindowStartWithOffset(long timestamp, long offset, long windowSize)
        {
            if (windowSize <= 0) throw new ArgumentOutOfRangeException(nameof(windowSize), "Window size must be positive.");
            // Ensure offset is positive and less than windowSize for standard modulo behavior.
            // long positiveOffset = (offset % windowSize + windowSize) % windowSize;
            // The formula from Flink's WindowAssigner.java: timestamp - (timestamp - offset + windowSize) % windowSize
            // This handles negative timestamps correctly as well if windowSize is positive.
            // Example: timestamp = -5, offset = 0, windowSize = 10. Expected start = -10.
            // -5 - (-5 - 0 + 10) % 10 = -5 - (5 % 10) = -5 - 5 = -10. Correct.
            // Example: timestamp = 7, offset = 0, windowSize = 5. Expected start = 5.
            // 7 - (7 - 0 + 5) % 5 = 7 - (12 % 5) = 7 - 2 = 5. Correct.
            return timestamp - (timestamp - offset + windowSize) % windowSize;
        }
    }
}
#nullable disable
