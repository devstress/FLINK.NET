using FlinkDotNet.Core.Abstractions.Serializers;
using FlinkDotNet.Core.Abstractions.Windowing; // For Window, GlobalWindow, Trigger

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

        public long MaxTimestamp() => long.MaxValue;

        public bool Equals(object? obj) => obj is GlobalWindow;
        public int GetHashCode() => typeof(GlobalWindow).GetHashCode();
        public string ToString() => "GlobalWindow";
    }

    /// <summary>
    /// Serializer for the singleton GlobalWindow.
    /// </summary>
    public class GlobalWindowSerializer : ITypeSerializer<GlobalWindow>
    {
        public byte[] Serialize(GlobalWindow obj) => Array.Empty<byte>(); // No state to serialize for singleton
        public GlobalWindow Deserialize(byte[] bytes) => GlobalWindow.Instance; // Always return the singleton
    }


    /// <summary>
    /// Assigns all elements to a single <see cref="GlobalWindow"/>.
    /// </summary>
    public class GlobalWindows<TElement> : IWindowAssigner<TElement, GlobalWindow>
    {
        private GlobalWindows() { }

        public static GlobalWindows<TElement> Create() => new GlobalWindows<TElement>();

        public ICollection<GlobalWindow> AssignWindows(TElement element, long timestamp, IWindowAssignerContext context)
        {
            return new List<GlobalWindow> { GlobalWindow.Instance };
        }

        public Trigger<TElement, GlobalWindow> GetDefaultTrigger(StreamExecutionEnvironment environment)
        {
            // GlobalWindows by default never fire unless a custom trigger is specified.
            return new NeverTrigger<TElement, GlobalWindow>();
        }

        public ITypeSerializer<GlobalWindow> GetWindowSerializer() => new GlobalWindowSerializer();

        // GlobalWindows can be used with either event time or processing time,
        // depending on the trigger. Default trigger (NeverTrigger) doesn't rely on time.
        // If an event time trigger is used, then it's event time.
        // For now, let's default to false, meaning it doesn't inherently impose event time.
        public bool IsEventTime => false;

        public string ToString() => "GlobalWindows";
    }
}
