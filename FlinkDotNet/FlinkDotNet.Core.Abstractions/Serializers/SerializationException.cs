using System;

namespace FlinkDotNet.Core.Abstractions.Serializers
{
    // Custom SerializationException for Flink.NET (if not already defined elsewhere)
    // If it is defined elsewhere (e.g., in Core.Abstractions), this can be removed.
    // For now, adding it here for completeness of the snippet.
    public class SerializationException : Exception
    {
        public SerializationException() { }
        public SerializationException(string message) : base(message) { }
        public SerializationException(string message, Exception inner) : base(message, inner) { }
    }
}
