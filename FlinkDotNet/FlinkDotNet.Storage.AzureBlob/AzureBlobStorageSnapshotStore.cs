#nullable enable
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models; // Added for BlobDownloadInfo
using FlinkDotNet.Core.Abstractions.Storage;

namespace FlinkDotNet.Storage.AzureBlob
{
    public class AzureBlobStorageSnapshotStore : IStateSnapshotStore
    {
        private readonly AzureBlobStorageSnapshotStoreOptions _options;
        private readonly BlobContainerClient _containerClient;

        public AzureBlobStorageSnapshotStore(AzureBlobStorageSnapshotStoreOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            string? effectiveConnectionString = _options.ConnectionString;

            if (string.IsNullOrWhiteSpace(effectiveConnectionString))
            {
                if (!string.IsNullOrWhiteSpace(_options.AccountName) && !string.IsNullOrWhiteSpace(_options.AccountKey))
                {
                    if (!string.IsNullOrWhiteSpace(_options.BlobServiceEndpoint))
                    {
                        effectiveConnectionString = $"DefaultEndpointsProtocol={(IsHttps(_options.BlobServiceEndpoint) ? "https" : "http")};AccountName={_options.AccountName};AccountKey={_options.AccountKey};BlobEndpoint={_options.BlobServiceEndpoint};";
                    }
                    else
                    {
                        effectiveConnectionString = $"DefaultEndpointsProtocol=https;AccountName={_options.AccountName};AccountKey={_options.AccountKey};EndpointSuffix=core.windows.net;";
                    }
                    Console.WriteLine($"[AzureBlobStorageSnapshotStore] Constructed connection string from AccountName/Key. Endpoint specified: {!string.IsNullOrWhiteSpace(_options.BlobServiceEndpoint)}");
                }
            }

            if (string.IsNullOrWhiteSpace(effectiveConnectionString))
            {
                throw new ArgumentException(
                    "Azure Blob Storage configuration is insufficient. " +
                    "Provide 'ConnectionString', or both 'AccountName' and 'AccountKey' (optionally with 'BlobServiceEndpoint').",
                    nameof(options));
            }

            if (string.IsNullOrWhiteSpace(options.ContainerName))
            {
                throw new ArgumentException("Azure Blob Storage container name must be provided.", nameof(options.ContainerName));
            }

            try
            {
                _containerClient = new BlobContainerClient(effectiveConnectionString, options.ContainerName);
                _containerClient.CreateIfNotExists();
                Console.WriteLine($"[AzureBlobStorageSnapshotStore] Initialized with Container: {options.ContainerName}. Effective CS used (i.e. was ConnectionString property initially set): {!string.IsNullOrWhiteSpace(_options.ConnectionString)}");
            }
            catch (Exception ex)
            {
                throw new ArgumentException($"[AzureBlobStorageSnapshotStore] Failed to initialize Azure Blob Container Client. Container: {options.ContainerName}. Error: {ex.Message}", ex);
            }
        }

        private bool IsHttps(string? url)
        {
            if (string.IsNullOrWhiteSpace(url)) return false;
            // Ensure url is not null before calling Trim()
            return url!.Trim().StartsWith("https://", StringComparison.OrdinalIgnoreCase);
        }

        private string GenerateBlobName(string jobId, long checkpointId, string taskManagerId, string operatorId)
        {
            var parts = new[]
            {
                _options.BasePath,
                SanitizeBlobPathComponent(jobId),
                $"cp_{checkpointId}",
                SanitizeBlobPathComponent(taskManagerId),
                $"{SanitizeBlobPathComponent(operatorId)}.dat"
            };
            return string.Join("/", parts.Where(p => !string.IsNullOrEmpty(p)));
        }

        private string SanitizeBlobPathComponent(string component)
        {
            // Replace common problematic characters. This list might need refinement based on Azure Blob naming rules
            // and typical inputs. For this iteration, replacing characters that are invalid in file/path names
            // which is often a safe bet, though Azure is more permissive.
            // Azure allows any UTF-8 character, but some might need URL encoding if used in URLs directly
            // or cause issues with tools.
            // Simple replacement for now:
            var sanitized = component.Replace('\\', '_').Replace('/', '_').Replace('?', '_').Replace('#', '_').Replace(':', '_');
            // Could also use a more restrictive approach:
            // string invalidChars = new string(Path.GetInvalidFileNameChars()) + new string(Path.GetInvalidPathChars());
            // string sanitized = component;
            // foreach (char c in invalidChars)
            // {
            //     sanitized = sanitized.Replace(c.ToString(), "_");
            // }
            return sanitized;
        }

        public async Task<SnapshotHandle> StoreSnapshot(
            string jobId,
            long checkpointId,
            string taskManagerId,
            string operatorId,
            byte[] snapshotData)
        {
            if (snapshotData == null) throw new ArgumentNullException(nameof(snapshotData));
            var blobName = GenerateBlobName(jobId, checkpointId, taskManagerId, operatorId);
            BlobClient blobClient = _containerClient.GetBlobClient(blobName);
            try
            {
                using (var stream = new MemoryStream(snapshotData, false))
                {
                    await blobClient.UploadAsync(stream, overwrite: true);
                }
                Console.WriteLine($"[AzureBlobStorageSnapshotStore] Snapshot stored: {_options.ContainerName}/{blobName}");
                return new SnapshotHandle($"azureblob://{_options.ContainerName}/{blobName}");
            }
            catch (Azure.RequestFailedException ex)
            {
                Console.WriteLine($"[AzureBlobStorageSnapshotStore] Error storing snapshot to Azure Blob {blobName}: {ex.Message}");
                throw new IOException($"Failed to store snapshot to Azure Blob Storage. Blob: {blobName}", ex);
            }
        }

        public async Task<byte[]?> RetrieveSnapshot(SnapshotHandle handle)
        {
            if (handle == null || string.IsNullOrWhiteSpace(handle.Value)) throw new ArgumentNullException(nameof(handle));
            var expectedPrefix = $"azureblob://{_options.ContainerName}/";
            if (!handle.Value.StartsWith(expectedPrefix))
            {
                throw new ArgumentException($"Invalid snapshot handle prefix. Expected '{expectedPrefix}'. Got: {handle.Value}", nameof(handle));
            }
            var blobName = handle.Value.Substring(expectedPrefix.Length);
            BlobClient blobClient = _containerClient.GetBlobClient(blobName);
            try
            {
                if (!await blobClient.ExistsAsync())
                {
                    Console.WriteLine($"[AzureBlobStorageSnapshotStore] Snapshot blob not found: {blobName}");
                    return null;
                }
                Azure.Response<BlobDownloadInfo> download = await blobClient.DownloadAsync();
                using (var memoryStream = new MemoryStream())
                {
                    await download.Value.Content.CopyToAsync(memoryStream);
                    return memoryStream.ToArray();
                }
            }
            catch (Azure.RequestFailedException ex)
            {
                Console.WriteLine($"[AzureBlobStorageSnapshotStore] Error retrieving snapshot from Azure Blob {blobName}: {ex.Message}");
                throw new IOException($"Failed to retrieve snapshot from Azure Blob Storage. Blob: {blobName}", ex);
            }
        }
    }
}
#nullable disable
