#nullable enable

namespace FlinkDotNet.JobManager.Models
{
    public class OutputKeyingConfigDto
    {
        /// <summary>
        /// Base64 encoded string of the serialized IKeySelector instance.
        /// The TaskManager will deserialize this using the KeySelectorTypeName.
        /// </summary>
        public string? SerializedKeySelector { get; set; }

        /// <summary>
        /// Assembly-qualified name of the IKeySelector implementation.
        /// Used by the TaskManager to know what type to deserialize the SerializedKeySelector into.
        /// </summary>
        public string? KeySelectorTypeName { get; set; }

        /// <summary>
        /// Assembly-qualified name of the key's data type.
        /// Example: "System.String", "System.Int32", "MyProject.Types.OrderKey, MyAssembly"
        /// </summary>
        public string? KeyTypeName { get; set; }

        public OutputKeyingConfigDto(string? serializedKeySelector, string? keySelectorTypeName, string? keyTypeName)
        {
            SerializedKeySelector = serializedKeySelector;
            KeySelectorTypeName = keySelectorTypeName;
            KeyTypeName = keyTypeName;
        }

        public OutputKeyingConfigDto() {}
    }
}
#nullable disable
