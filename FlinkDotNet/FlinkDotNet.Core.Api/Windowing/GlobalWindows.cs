#nullable enable
using System;
using System.Collections.Generic;
using FlinkDotNet.Core.Abstractions.Serializers;
using FlinkDotNet.Core.Api.Streaming; // For StreamExecutionEnvironment

namespace FlinkDotNet.Core.Api.Windowing
{
    /// <summary>
    /// Represents a single, global window to which all elements are assigned.
    /// Typically used with custom triggers to define processing logic.
    /// </summary>
    public class GlobalWindow : Window
    {
        private static readonly GlobalWindow _instance = new GlobalWindow();
        public static GlobalWindow Instance => _instance;

        private GlobalWindow() { } // Private constructor for singleton

        public override long MaxTimestamp() => long.MaxValue;

        public override bool Equals(object? obj) => obj is GlobalWindow;
        public override int GetHashCode() => typeof(GlobalWindow).GetHashCode();
        public override string ToString() => "GlobalWindow";
    }

    /// <summary>
    /// Serializer for the singleton GlobalWindow.
    /// </summary>
    public class GlobalWindowSerializer : ITypeSerializer<GlobalWindow>
    {
        public byte[] Serialize(GlobalWindow obj) => Array.Empty<byte>(); // No state to serialize for singleton
        public GlobalWindow Deserialize(byte[] bytes) => GlobalWindow.Instance; // Always return the singleton
    }

    // Placeholder for NeverTrigger, will be fully defined later.
    // This derives from the Trigger stub created in WindowAssigner.cs
    public class NeverTrigger<TElement, TWindow> : Trigger<TElement, TWindow> where TWindow : Window
    {
        // Actual trigger logic would go here. For now, it's a stub.
        // public override TriggerResult OnElement(TElement element, long timestamp, TWindow window, ITriggerContext ctx) => TriggerResult.CONTINUE;
        // public override TriggerResult OnEventTime(long time, TWindow window, ITriggerContext ctx) => TriggerResult.CONTINUE; // Never fires on time
        // public override TriggerResult OnProcessingTime(long time, TWindow window, ITriggerContext ctx) => TriggerResult.CONTINUE; // Never fires on time
        // public override void Clear(TWindow window, ITriggerContext ctx) { }
    }


    /// <summary>
    /// Assigns all elements to a single <see cref="GlobalWindow"/>.
    /// </summary>
    public class GlobalWindows<TElement> : WindowAssigner<TElement, GlobalWindow>
    {
        private GlobalWindows() { }

        public static GlobalWindows<TElement> Create() => new GlobalWindows<TElement>();

        public override ICollection<GlobalWindow> AssignWindows(TElement element, long timestamp, IWindowAssignerContext context)
        {
            return new List<GlobalWindow> { GlobalWindow.Instance };
        }

        public override Trigger<TElement, GlobalWindow> GetDefaultTrigger(StreamExecutionEnvironment environment)
        {
            // GlobalWindows by default never fire unless a custom trigger is specified.
            return new NeverTrigger<TElement, GlobalWindow>();
        }

        public override ITypeSerializer<GlobalWindow> GetWindowSerializer() => new GlobalWindowSerializer();

        // GlobalWindows can be used with either event time or processing time,
        // depending on the trigger. Default trigger (NeverTrigger) doesn't rely on time.
        // If an event time trigger is used, then it's event time.
        // For now, let's default to false, meaning it doesn't inherently impose event time.
        public override bool IsEventTime => false;

        public override string ToString() => "GlobalWindows";
    }
}
#nullable disable
