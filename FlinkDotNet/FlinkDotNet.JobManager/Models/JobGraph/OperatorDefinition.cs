using System.Collections.Generic;
using System.Text.Json; // For JsonSerializer
using Google.Protobuf.WellKnownTypes; // For Struct

namespace FlinkDotNet.JobManager.Models.JobGraph
{
    public class OperatorDefinition
    {
        /// <summary>
        /// Assembly-qualified class name of the user-defined function
        /// (e.g., ISourceFunction, IMapOperator, IFilterOperator, ISinkFunction, etc.).
        /// </summary>
        public string FullyQualifiedName { get; }

        /// <summary>
        /// Operator-specific configuration, stored as a JSON string.
        /// This can include things like file paths for sources, or parameters for a map function.
        /// </summary>
        public string? ConfigurationJson { get; set; }

        public OperatorDefinition(string fullyQualifiedName, string? configurationJson = null)
        {
            FullyQualifiedName = fullyQualifiedName;
            ConfigurationJson = configurationJson;
        }

        /// <summary>
        /// Converts this OperatorDefinition to its Protobuf representation.
        /// </summary>
        /// <returns>The Protobuf OperatorDefinition message.</returns>
        public Proto.Internal.OperatorDefinition ToProto()
        {
            var protoOpDef = new Proto.Internal.OperatorDefinition
            {
                FullyQualifiedName = this.FullyQualifiedName
            };

            if (!string.IsNullOrEmpty(ConfigurationJson))
            {
                // Convert JSON string to Protobuf Struct
                try
                {
                    protoOpDef.Configuration = JsonParser.Default.Parse<Struct>(ConfigurationJson);
                }
                catch (System.Exception ex)
                {
                    // Handle or log parsing error, e.g., invalid JSON
                    // For now, we might let it be null or throw
                    System.Console.WriteLine($"Error parsing OperatorDefinition.ConfigurationJson to Protobuf Struct: {ex.Message}");
                    // protoOpDef.Configuration = new Struct(); // Or leave as null
                }
            }
            // If ConfigurationJson is null or empty, protoOpDef.Configuration will remain its default (null)

            return protoOpDef;
        }

        /// <summary>
        /// Creates an OperatorDefinition from its Protobuf representation.
        /// </summary>
        /// <param name="protoOpDef">The Protobuf OperatorDefinition message.</param>
        /// <returns>A new OperatorDefinition instance.</returns>
        public static OperatorDefinition FromProto(Proto.Internal.OperatorDefinition protoOpDef)
        {
            string? configJson = null;
            if (protoOpDef.Configuration != null)
            {
                try
                {
                    configJson = JsonFormatter.Default.Format(protoOpDef.Configuration);
                }
                catch (System.Exception ex)
                {
                     System.Console.WriteLine($"Error formatting OperatorDefinition.Configuration Struct to JSON: {ex.Message}");
                }
            }
            return new OperatorDefinition(protoOpDef.FullyQualifiedName, configJson);
        }
    }
}
#nullable disable
