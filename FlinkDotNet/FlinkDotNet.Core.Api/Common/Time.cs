#nullable enable
using System;

namespace FlinkDotNet.Core.Api.Common
{
    /// <summary>
    /// Represents a time interval, typically used for window sizes, slides, or gaps.
    /// </summary>
    public readonly struct Time : IEquatable<Time>
    {
        /// <summary>
        /// Gets the time interval in milliseconds.
        /// </summary>
        public long Milliseconds { get; }

        private Time(long milliseconds)
        {
            if (milliseconds < 0)
                throw new ArgumentOutOfRangeException(nameof(milliseconds), "Time interval cannot be negative.");
            Milliseconds = milliseconds;
        }

        /// <summary>
        /// Creates a Time representing the given number of milliseconds.
        /// </summary>
        public static Time MillisecondsMethod(long milliseconds) => new Time(milliseconds);

        /// <summary>
        /// Creates a Time representing the given number of seconds.
        /// </summary>
        public static Time Seconds(long seconds) => new Time(seconds * 1000);

        /// <summary>
        /// Creates a Time representing the given number of minutes.
        /// </summary>
        public static Time Minutes(long minutes) => new Time(minutes * 60 * 1000);

        /// <summary>
        /// Creates a Time representing the given number of hours.
        /// </summary>
        public static Time Hours(long hours) => new Time(hours * 60 * 60 * 1000);

        /// <summary>
        /// Creates a Time representing the given number of days.
        /// </summary>
        public static Time Days(long days) => new Time(days * 24 * 60 * 60 * 1000);

        // IEquatable and other utility methods
        public bool Equals(Time other) => Milliseconds == other.Milliseconds;
        public override bool Equals(object? obj) => obj is Time other && Equals(other);
        public override int GetHashCode() => Milliseconds.GetHashCode();
        public static bool operator ==(Time left, Time right) => left.Equals(right);
        public static bool operator !=(Time left, Time right) => !left.Equals(right);
        public override string ToString() => $"{Milliseconds} ms";
    }
}
#nullable disable
