using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices; // For IAsyncEnumerable
using System.Threading;
using System.Threading.Tasks;
using FlinkDotNet.Core.Abstractions.Storage; // For IStateSnapshotStore, Writer, Reader

namespace FlinkDotNet.Storage.FileSystem
{
    public class FileSystemSnapshotStore : IStateSnapshotStore
    {
        private readonly string _basePath;
        private const string RawStateExtension = ".raw_state";
        private const string KeyedStateExtension = ".keyed_state";
        private const string TempFileSuffix = ".tmp";

        // Optional: File Header for versioning
        private static readonly byte[] FileMagicNumber = { (byte)'F', (byte)'N', (byte)'K', (byte)'S' }; // FlinkNet Keyed State
        private const ushort FileFormatVersion = 1;

        public FileSystemSnapshotStore(string basePath)
        {
            _basePath = Path.GetFullPath(basePath);
            // Directory.CreateDirectory is idempotent; no need to check existence
            Directory.CreateDirectory(_basePath);
        }

        private string GetOperatorSubtaskDirectory(string jobId, long checkpointId, string operatorId, string subtaskId)
        {
            // OperatorId might contain invalid path chars if it's a user string; subtaskId usually int.toString().
            // For now, assume they are safe or JobManager provides safe IDs.
            // Sanitize operatorId and subtaskId for path safety
            string safeOperatorId = SanitizePathComponent(operatorId);
            string safeSubtaskId = SanitizePathComponent(subtaskId);
            return Path.Combine(_basePath, SanitizePathComponent(jobId), $"cp_{checkpointId}", $"{safeOperatorId}_{safeSubtaskId}");
        }

        private static string SanitizePathComponent(string component) // CA1822/S2325: Made static
        {
            return Path.GetInvalidFileNameChars().Aggregate(component, (current, c) => current.Replace(c.ToString(), "_"));
        }


        public Task<IStateSnapshotWriter> CreateWriter(string jobId, long checkpointId, string operatorId, string subtaskId)
        {
            string directory = GetOperatorSubtaskDirectory(jobId, checkpointId, operatorId, subtaskId);
            Directory.CreateDirectory(directory); // Ensure it exists
            return Task.FromResult<IStateSnapshotWriter>(new FileSystemSnapshotWriterSession(directory));
        }

        public static Task<IStateSnapshotReader> CreateReader(string snapshotHandle) // snapshotHandle is the directory path // CA1822/S2325: Made static
        {
            if (string.IsNullOrEmpty(snapshotHandle) || !Directory.Exists(snapshotHandle))
            {
                // Return a reader that indicates no state, rather than throwing,
                // to align with how operators might check for prior state.
                // Or, JobManager ensures only valid handles are passed.
                // For now, let's assume handle is valid or this indicates an issue.
                Console.WriteLine($"[FileSystemSnapshotStore] WARNING: Snapshot directory not found or handle invalid: {snapshotHandle}. Returning empty reader.");
                // Returning a "null" reader or an empty one.
                // An empty one is safer for consumers that don't null check rigorously.
                return Task.FromResult<IStateSnapshotReader>(new FileSystemSnapshotReaderSession(snapshotHandle, isEmpty: true));
            }
            return Task.FromResult<IStateSnapshotReader>(new FileSystemSnapshotReaderSession(snapshotHandle));
        }

        public async Task<SnapshotHandle> StoreSnapshot(
            string jobId,
            long checkpointId,
            string taskManagerId, // Corresponds to subtaskId for directory structure
            string operatorId,    // Corresponds to operatorId for directory, and can be part of filename for uniqueness
            byte[] snapshotData)
        {
            // Use taskManagerId as subtaskId and operatorId as operatorId for directory structure
            string directory = GetOperatorSubtaskDirectory(jobId, checkpointId, operatorId, taskManagerId);
            Directory.CreateDirectory(directory); // Ensure it exists

            // Use a unique name for the snapshot file itself, perhaps based on operatorId or a fixed name if only one raw snapshot per location
            // For now, let's use a fixed name "raw_snapshot.dat" as an example, assuming one blob per call.
            // If operatorId is unique per call for this subtask, it could be part of filename.
            // Let's use operatorId as the filename to distinguish if multiple calls are made for different "states"
            // within the same operator subtask for the same checkpoint.
            string snapshotFileName = SanitizePathComponent(operatorId) + ".snapshot.dat"; // Ensure unique and safe filename
            string filePath = Path.Combine(directory, snapshotFileName);

            await File.WriteAllBytesAsync(filePath, snapshotData);

            // The handle should be enough to retrieve this specific file.
            // Here, the handle is the full file path.
            return new SnapshotHandle(filePath);
        }

        public async Task<byte[]?> RetrieveSnapshot(SnapshotHandle handle)
        {
            if (handle == null || string.IsNullOrEmpty(handle.Value)) // CS1061 Fix: Handle -> Value
            {
                return null;
            }

            string filePath = handle.Value; // CS1061 Fix: Handle -> Value

            if (!File.Exists(filePath))
            {
                return null;
            }

            try
            {
                return await File.ReadAllBytesAsync(filePath);
            }
            catch (IOException ex)
            {
                Console.WriteLine($"[FileSystemSnapshotStore] Error reading snapshot file {filePath}: {ex.Message}");
                return null;
            }
        }

        // --- Writer Session ---
        private sealed class FileSystemSnapshotWriterSession : IStateSnapshotWriter // S3260/CA1852: Added sealed
        {
            private readonly string _operatorSubtaskDirectory;
            private readonly Dictionary<string, (FileStream Stream, BinaryWriter Writer)> _activeKeyedStateWriters = new();
            private readonly List<string> _writtenTempFiles = new List<string>();
            private string? _currentActiveKeyedStateName; // To address WriteKeyedEntry ambiguity


            public FileSystemSnapshotWriterSession(string operatorSubtaskDirectory)
            {
                _operatorSubtaskDirectory = operatorSubtaskDirectory;
            }

            private string GetFilePath(string stateName, string extension, bool isTemporary = false)
            {
                string fileName = Path.GetInvalidFileNameChars().Aggregate(stateName, (current, c) => current.Replace(c.ToString(), "_")) + extension;
                if (isTemporary)
                { // S121
                    fileName += TempFileSuffix;
                }
                return Path.Combine(_operatorSubtaskDirectory, fileName);
            }

            public Stream GetStateOutputStream(string stateName)
            {
                string tempPath = GetFilePath(stateName, RawStateExtension, isTemporary: true);
                _writtenTempFiles.Add(tempPath);
                return new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None);
            }

            public Task BeginKeyedState(string stateName)
            {
                if (_activeKeyedStateWriters.ContainsKey(stateName))
                { // S121
                    throw new InvalidOperationException($"Keyed state '{stateName}' is already open for writing.");
                }
                if (_currentActiveKeyedStateName != null)
                { // S121
                     throw new InvalidOperationException($"Another keyed state '{_currentActiveKeyedStateName}' is already active. End it before beginning a new one.");
                }

                string tempPath = GetFilePath(stateName, KeyedStateExtension, isTemporary: true);
                var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None);
                var binaryWriter = new BinaryWriter(fileStream);

                binaryWriter.Write(FileMagicNumber);
                binaryWriter.Write(FileFormatVersion);

                _activeKeyedStateWriters[stateName] = (fileStream, binaryWriter);
                _writtenTempFiles.Add(tempPath);
                _currentActiveKeyedStateName = stateName; // Set current active
                return Task.CompletedTask;
            }

            public Task WriteKeyedEntry(byte[] key, byte[] value)
            {
                if (_currentActiveKeyedStateName == null || !_activeKeyedStateWriters.TryGetValue(_currentActiveKeyedStateName, out var writerTuple)) {
                     throw new InvalidOperationException("No keyed state is currently active via BeginKeyedState, or the active state is unknown.");
                }
                var activeWriter = writerTuple.Writer;

                activeWriter.Write(key.Length);
                activeWriter.Write(key);
                activeWriter.Write(value.Length);
                activeWriter.Write(value);
                return Task.CompletedTask;
            }

            public Task EndKeyedState(string stateName)
            {
                if (_activeKeyedStateWriters.TryGetValue(stateName, out var writerTuple))
                {
                    writerTuple.Writer.Flush();
                    writerTuple.Stream.Flush();
                    writerTuple.Writer.Dispose();
                    _activeKeyedStateWriters.Remove(stateName);
                    if (_currentActiveKeyedStateName == stateName)
                    {
                        _currentActiveKeyedStateName = null; // Clear current active
                    }
                }
                else
                { // S121
                    throw new InvalidOperationException($"Keyed state '{stateName}' was not active or already ended.");
                }
                return Task.CompletedTask;
            }

            public Task<string> CommitAndGetHandleAsync()
            {
                if (_activeKeyedStateWriters.Count > 0) // CA1860
                {
                    Console.WriteLine($"[FileSystemSnapshotWriterSession] WARNING: Commit called but these keyed states were not ended: {string.Join(", ", _activeKeyedStateWriters.Keys)}. Disposing them now.");
                    foreach (var stateName in _activeKeyedStateWriters.Keys.ToList())
                    { // S121
                        EndKeyedState(stateName).GetAwaiter().GetResult();
                    }
                }

                foreach (string tempPath in _writtenTempFiles.Where(File.Exists)) // S3267: Use LINQ Where
                { // S121
                    // Inner if (File.Exists(finalPath)) still needed
                    string finalPath = tempPath.Substring(0, tempPath.Length - TempFileSuffix.Length);
                    if (File.Exists(finalPath))
                    { // S121
                        File.Delete(finalPath);
                    }
                    File.Move(tempPath, finalPath);
                }
                _writtenTempFiles.Clear();
                Console.WriteLine($"[FileSystemSnapshotWriterSession] Committed snapshot to directory: {_operatorSubtaskDirectory}");
                return Task.FromResult(_operatorSubtaskDirectory);
            }

            public async ValueTask DisposeAsync() // Implement IAsyncDisposable
            {
                if (_activeKeyedStateWriters.Count > 0) // CA1860
                {
                     Console.WriteLine($"[FileSystemSnapshotWriterSession] DisposeAsync: Ending active keyed states: {string.Join(", ", _activeKeyedStateWriters.Keys)}.");
                    foreach (var stateName in _activeKeyedStateWriters.Keys.ToList())
                    { // S121
                        await EndKeyedState(stateName);
                    }
                }
                 _activeKeyedStateWriters.Clear(); // Ensure dictionary is cleared
            }

            void IDisposable.Dispose() // Explicit IDisposable for non-async paths if needed
            {
                DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
        }

        // --- Reader Session ---
        private sealed class FileSystemSnapshotReaderSession : IStateSnapshotReader // S3260/CA1852: Added sealed
        {
            private readonly string _operatorSubtaskDirectory;
            private readonly bool _isEmpty; // If the directory didn't exist / handle was invalid

            public FileSystemSnapshotReaderSession(string operatorSubtaskDirectory, bool isEmpty = false)
            {
                _operatorSubtaskDirectory = operatorSubtaskDirectory;
                _isEmpty = isEmpty;
                if (_isEmpty && !Directory.Exists(_operatorSubtaskDirectory)) {
                     // If it's marked as empty because dir doesn't exist, it's fine.
                     // If not marked empty but dir doesn't exist, that's an issue usually caught by CreateReader.
                } else if (!_isEmpty && !Directory.Exists(_operatorSubtaskDirectory)) {
                    Console.WriteLine($"[FileSystemSnapshotReaderSession] WARNING: Directory {_operatorSubtaskDirectory} does not exist for a non-empty reader session.");
                    // This might indicate an issue if we expect state.
                }
            }

            private string GetFilePath(string stateName, string extension)
            {
                 return Path.Combine(_operatorSubtaskDirectory, Path.GetInvalidFileNameChars().Aggregate(stateName, (current, c) => current.Replace(c.ToString(), "_")) + extension);
            }

            public Stream GetStateInputStream(string stateName)
            {
                if (_isEmpty) throw new FileNotFoundException($"Snapshot is empty or directory not found. Cannot read state '{stateName}'.", GetFilePath(stateName, RawStateExtension));
                string filePath = GetFilePath(stateName, RawStateExtension);
                if (!File.Exists(filePath)) throw new FileNotFoundException($"Raw state file not found for state '{stateName}'.", filePath);
                return new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            }
            public bool HasState(string stateName) => !_isEmpty && File.Exists(GetFilePath(stateName, RawStateExtension));

            public Task<bool> HasKeyedState(string stateName) => Task.FromResult(!_isEmpty && File.Exists(GetFilePath(stateName, KeyedStateExtension)));

            public async IAsyncEnumerable<KeyValuePair<byte[], byte[]>> ReadKeyedStateEntries( // CS8403: Added async back
                string stateName)
            {
                if (_isEmpty)
                {
                    yield break;
                }

                string filePath = GetFilePath(stateName, KeyedStateExtension);
                if (!File.Exists(filePath))
                {
                    yield break;
                }

                using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
                if (fileStream.Length == 0)
                { // S121
                    yield break; // Empty file
                }

                using var reader = new BinaryReader(fileStream);

                byte[] magic = reader.ReadBytes(FileMagicNumber.Length);
                if (!magic.SequenceEqual(FileMagicNumber))
                { // S121
                    throw new IOException("Invalid magic number for keyed state file.");
                }
                ushort version = reader.ReadUInt16();
                if (version != FileFormatVersion)
                { // S121
                    throw new IOException($"Unsupported keyed state file version {version}. Expected {FileFormatVersion}.");
                }

                while (fileStream.Position < fileStream.Length)
                {
                    await Task.Yield(); // Satisfy CS1998 for async iterator method without other awaits

                    int keyLength = reader.ReadInt32();
                    if (keyLength < 0)
                    { // S121
                        throw new IOException("Invalid key length found in state file.");
                    }
                    byte[] keyBytes = reader.ReadBytes(keyLength);
                    if (keyBytes.Length != keyLength)
                    { // S121
                        throw new EndOfStreamException("Unexpected end of stream while reading key.");
                    }

                    int valueLength = reader.ReadInt32();
                    if (valueLength < 0)
                    { // S121
                        throw new IOException("Invalid value length found in state file.");
                    }
                    byte[] valueBytes = reader.ReadBytes(valueLength);
                    if (valueBytes.Length != valueLength)
                    { // S121
                        throw new EndOfStreamException("Unexpected end of stream while reading value.");
                    }

                    yield return new KeyValuePair<byte[], byte[]>(keyBytes, valueBytes);
                }
            }
            public void Dispose() { }
        }
    }
}
