using System;
using System.Collections.Generic; // For IAsyncEnumerable, KeyValuePair
using System.IO; // For Stream
using System.Threading.Tasks;

namespace FlinkDotNet.Core.Abstractions.Storage
{
    public interface IStateSnapshotReader : IDisposable // Consider IAsyncDisposable
    {
        /// <summary>
        /// Gets an input stream to read raw, non-keyed state for a given state name.
        /// </summary>
        Stream GetStateInputStream(string stateName); // Example of an existing method

        /// <summary>
        /// Checks if raw, non-keyed state exists for the given name.
        /// </summary>
        bool HasState(string stateName); // Example of an existing method

        /// <summary>
        /// Checks if keyed state exists for the given state name.
        /// </summary>
        Task<bool> HasKeyedState(string stateName);

        /// <summary>
        /// Reads all serialized key-value entries for a given named keyed state.
        /// The caller is responsible for deserializing both the key and the value.
        /// </summary>
        IAsyncEnumerable<KeyValuePair<byte[], byte[]>> ReadKeyedStateEntries(string stateName);
    }
}
#nullable disable
