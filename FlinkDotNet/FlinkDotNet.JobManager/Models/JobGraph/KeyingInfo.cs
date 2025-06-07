namespace FlinkDotNet.JobManager.Models.JobGraph
{
    /// <summary>
    /// Contains information about how the output of a specific JobEdge is keyed.
    /// This is a refined version of OutputKeyingConfig, potentially specific to an edge if needed,
    /// or can be a direct reference if KeyingInfo and OutputKeyingConfig are identical.
    /// For now, let's assume it mirrors OutputKeyingConfig for simplicity in JobVertex.OutputEdgeKeying.
    /// </summary>
    public class KeyingInfo
    {
        /// <summary>
        /// Assembly-qualified type name of the IKeySelector<TIn, TKey> implementation.
        /// </summary>
        public string KeySelectorTypeName { get; }

        /// <summary>
        /// Assembly-qualified type name of the key itself (TKey).
        /// </summary>
        public string KeyTypeAssemblyName { get; }

        // In the previous version, it had:
        // public string? SerializedKeySelector { get; set; }
        // public string? KeyTypeName { get; set; }
        // The current version aligns better with OutputKeyingConfig structure.

        public KeyingInfo(string keySelectorTypeName, string keyTypeAssemblyName)
        {
            KeySelectorTypeName = keySelectorTypeName;
            KeyTypeAssemblyName = keyTypeAssemblyName;
        }

        /// <summary>
        /// Converts this KeyingInfo to its Protobuf representation.
        /// (Assuming it maps to the same Proto.Internal.OutputKeyingConfig for now)
        /// </summary>
        /// <returns>The Protobuf OutputKeyingConfig message.</returns>
        public Proto.Internal.OutputKeyingConfig ToProto()
        {
            return new Proto.Internal.OutputKeyingConfig
            {
                KeySelectorTypeName = this.KeySelectorTypeName,
                KeyTypeAssemblyName = this.KeyTypeAssemblyName
            };
        }

        /// <summary>
        /// Creates a KeyingInfo from its Protobuf representation.
        /// (Assuming it maps from Proto.Internal.OutputKeyingConfig for now)
        /// </summary>
        /// <param name="protoConfig">The Protobuf OutputKeyingConfig message.</param>
        /// <returns>A new KeyingInfo instance.</returns>
        public static KeyingInfo FromProto(Proto.Internal.OutputKeyingConfig protoConfig)
        {
            return new KeyingInfo(protoConfig.KeySelectorTypeName, protoConfig.KeyTypeAssemblyName);
        }
    }
}
#nullable disable
