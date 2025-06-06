#nullable enable
using System;
using System.IO; // For Stream
using System.Threading.Tasks;

namespace FlinkDotNet.Core.Abstractions.Storage
{
    public interface IStateSnapshotWriter : IDisposable // Consider IAsyncDisposable if underlying streams are async
    {
        /// <summary>
        /// Gets an output stream to write raw, non-keyed state for a given state name.
        /// </summary>
        Stream GetStateOutputStream(string stateName); // Example of an existing method

        /// <summary>
        /// Signals the beginning of writing entries for a named keyed state.
        /// All subsequent calls to WriteKeyedEntry are associated with this stateName until EndKeyedState is called.
        /// </summary>
        Task BeginKeyedState(string stateName);

        /// <summary>
        /// Writes a single serialized key-value pair for the currently open keyed state.
        /// The caller is responsible for serializing both the key and the value.
        /// </summary>
        Task WriteKeyedEntry(byte[] key, byte[] value);

        /// <summary>
        /// Signals the end of writing entries for a named keyed state.
        /// </summary>
        Task EndKeyedState(string stateName);

        /// <summary>
        /// Commits all written data (both raw and keyed states) and returns a handle
        /// representing the entire snapshot written by this writer.
        /// </summary>
        Task<string> CommitAndGetHandleAsync(); // Example of an existing method
    }
}
#nullable disable
