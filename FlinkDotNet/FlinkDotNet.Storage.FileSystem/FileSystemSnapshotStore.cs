using System;
using System.IO;
using System.Threading.Tasks;
using FlinkDotNet.Core.Abstractions.Storage;

namespace FlinkDotNet.Storage.FileSystem
{
    public class FileSystemSnapshotStoreOptions
    {
        public string BasePath { get; set; } = Path.Combine(Path.GetTempPath(), "flinkdotnet_snapshots");
    }

    public class FileSystemSnapshotStore : IStateSnapshotStore
    {
        private readonly FileSystemSnapshotStoreOptions _options;

        public FileSystemSnapshotStore(FileSystemSnapshotStoreOptions? options = null)
        {
            _options = options ?? new FileSystemSnapshotStoreOptions();
            if (!Directory.Exists(_options.BasePath))
            {
                Directory.CreateDirectory(_options.BasePath);
            }
            Console.WriteLine($"FileSystemSnapshotStore initialized with base path: {_options.BasePath}");
        }

        private string GenerateFilePath(string jobId, long checkpointId, string taskManagerId, string operatorId)
        {
            // Sanitize inputs to create valid path segments if necessary
            var jobDir = Path.Combine(_options.BasePath, SanitizePathComponent(jobId));
            var cpDir = Path.Combine(jobDir, $"cp_{checkpointId}");
            var taskDir = Path.Combine(cpDir, SanitizePathComponent(taskManagerId));
            var filePath = Path.Combine(taskDir, $"{SanitizePathComponent(operatorId)}.dat");

            Directory.CreateDirectory(jobDir);
            Directory.CreateDirectory(cpDir);
            Directory.CreateDirectory(taskDir);

            return filePath;
        }

        private string SanitizePathComponent(string component) // Basic sanitizer
        {
            foreach (char invalidChar in Path.GetInvalidFileNameChars())
            {
                component = component.Replace(invalidChar, '_');
            }
            foreach (char invalidChar in Path.GetInvalidPathChars())
            {
                 component = component.Replace(invalidChar, '_');
            }
            return component;
        }

        public async Task<SnapshotHandle> StoreSnapshot(
            string jobId,
            long checkpointId,
            string taskManagerId,
            string operatorId,
            byte[] snapshotData)
        {
            var filePath = GenerateFilePath(jobId, checkpointId, taskManagerId, operatorId);
            try
            {
                await File.WriteAllBytesAsync(filePath, snapshotData);
                Console.WriteLine($"Snapshot stored: {filePath}");
                return new SnapshotHandle($"file://{filePath}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error storing snapshot to {filePath}: {ex.Message}");
                throw; // Rethrow to indicate failure
            }
        }

        public async Task<byte[]?> RetrieveSnapshot(SnapshotHandle handle)
        {
            if (!handle.Value.StartsWith("file://"))
            {
                throw new ArgumentException("Invalid snapshot handle for FileSystemSnapshotStore.", nameof(handle));
            }

            var filePath = handle.Value.Substring("file://".Length);
            try
            {
                if (File.Exists(filePath))
                {
                    return await File.ReadAllBytesAsync(filePath);
                }
                Console.WriteLine($"Snapshot file not found: {filePath}");
                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error retrieving snapshot from {filePath}: {ex.Message}");
                throw; // Rethrow or handle as appropriate
            }
        }
    }
}
#nullable disable
