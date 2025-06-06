#nullable enable

namespace FlinkDotNet.JobManager.Models
{
    public class WindowConfigDto
    {
        /// <summary>
        /// Type of window assigner. Case-insensitive.
        /// Examples: "TumblingEventTime", "TumblingProcessingTime", "SlidingEventTime",
        /// "SlidingProcessingTime", "EventTimeSession", "ProcessingTimeSession", "Global".
        /// </summary>
        public string? AssignerType { get; set; }

        // Parameters for time-based windows
        public long? SizeMs { get; set; }
        public long? SlideMs { get; set; }
        public long? GapMs { get; set; }

        // Optional configurations for custom triggers/evictors
        /// <summary>
        /// Assembly-qualified type name of a custom ITrigger implementation.
        /// </summary>
        public string? TriggerType { get; set; }
        // public string? TriggerConfigJson { get; set; } // Configuration for custom trigger (if needed)

        /// <summary>
        /// Assembly-qualified type name of a custom IEvictor implementation.
        /// </summary>
        public string? EvictorType { get; set; }
        // public string? EvictorConfigJson { get; set; } // Configuration for custom evictor (if needed)

        public long? AllowedLatenessMs { get; set; }
    }
}
#nullable disable
