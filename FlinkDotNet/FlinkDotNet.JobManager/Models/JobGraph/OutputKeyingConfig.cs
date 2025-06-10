namespace FlinkDotNet.JobManager.Models.JobGraph
{
    /// <summary>
    /// Configuration for how the output of a JobVertex should be keyed or partitioned
    /// when being sent to a downstream JobVertex via a JobEdge.
    /// This is typically relevant for Hash shuffle mode.
    /// </summary>
    public class OutputKeyingConfig
    {
        /// <summary>
        /// Assembly-qualified type name of the IKeySelector implementation.
        /// This selector is used to extract a key from an output record.
        /// </summary>
        public string KeySelectorTypeName { get; }

        /// <summary>
        /// Assembly-qualified type name of the key itself (TKey).
        /// This is important for potentially finding appropriate serializers for the key.
        /// </summary>
        public string KeyTypeAssemblyName { get; }

        public OutputKeyingConfig(string keySelectorTypeName, string keyTypeAssemblyName)
        {
            KeySelectorTypeName = keySelectorTypeName;
            KeyTypeAssemblyName = keyTypeAssemblyName;
        }

        /// <summary>
        /// Converts this OutputKeyingConfig to its Protobuf representation.
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
        /// Creates an OutputKeyingConfig from its Protobuf representation.
        /// </summary>
        /// <param name="protoConfig">The Protobuf OutputKeyingConfig message.</param>
        /// <returns>A new OutputKeyingConfig instance.</returns>
        public static OutputKeyingConfig FromProto(Proto.Internal.OutputKeyingConfig protoConfig)
        {
            return new OutputKeyingConfig(protoConfig.KeySelectorTypeName, protoConfig.KeyTypeAssemblyName);
        }
    }
}
#nullable disable
