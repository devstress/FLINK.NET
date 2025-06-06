#nullable enable
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2; // For GoogleCredential
using Google.Cloud.Storage.V1;
using FlinkDotNet.Core.Abstractions.Storage; // For IStateSnapshotStore, SnapshotHandle

// Note: Google.Apis.Http might be needed for advanced HttpClient customization,
// but is not required for the basic endpoint override shown here.

namespace FlinkDotNet.Storage.GCS
{
    public class GcsSnapshotStore : IStateSnapshotStore, IDisposable
    {
        private readonly GcsSnapshotStoreOptions _options;
        private readonly StorageClient _storageClient;
        private bool _disposed = false;

        public GcsSnapshotStore(GcsSnapshotStoreOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            if (string.IsNullOrWhiteSpace(options.BucketName))
            {
                throw new ArgumentException("GCS bucket name must be provided.", nameof(options.BucketName));
            }

            StorageClientBuilder clientBuilder = new StorageClientBuilder();

            if (!string.IsNullOrWhiteSpace(options.CredentialsFilePath))
            {
                clientBuilder.GoogleCredential = GoogleCredential.FromFile(options.CredentialsFilePath);
                Console.WriteLine($"[GcsSnapshotStore] Using credentials from file: {options.CredentialsFilePath}");
            }
            else
            {
                // Relies on Application Default Credentials (ADC) if no file path is given.
                // ADC includes GOOGLE_APPLICATION_CREDENTIALS env var, gcloud auth, metadata server, etc.
                Console.WriteLine("[GcsSnapshotStore] Using Application Default Credentials (e.g., GOOGLE_APPLICATION_CREDENTIALS environment variable).");
            }

            if (!string.IsNullOrWhiteSpace(options.Endpoint))
            {
                clientBuilder.Endpoint = options.Endpoint;
                // The C# SDK client generally handles HTTP vs HTTPS endpoints correctly based on the URL.
                // No special HttpClientFactory modification is typically needed just for an HTTP endpoint
                // (like a local emulator running on http://localhost:4443).
                Console.WriteLine($"[GcsSnapshotStore] Using custom GCS endpoint: {options.Endpoint}");
            }

            // ProjectId from options is not directly set on StorageClientBuilder.
            // It's typically part of the credential or environment.
            _storageClient = clientBuilder.Build();
            Console.WriteLine($"[GcsSnapshotStore] Initialized. Bucket: {_options.BucketName}, ProjectId (from options, if any): {_options.ProjectId ?? "Not explicitly set, inferred from credentials/environment"}.");
        }

        private string GenerateGcsObjectName(string jobId, long checkpointId, string taskManagerId, string operatorId)
        {
            var parts = new[]
            {
                _options.BasePath,
                SanitizeGcsObjectNameComponent(jobId),
                $"cp_{checkpointId}",
                SanitizeGcsObjectNameComponent(taskManagerId),
                $"{SanitizeGcsObjectNameComponent(operatorId)}.dat"
            };
            // GCS object names are /-separated paths.
            return string.Join("/", parts.Where(p => !string.IsNullOrEmpty(p) && p != "/"));
        }

        private string SanitizeGcsObjectNameComponent(string component)
        {
            // GCS object naming rules: UTF-8, less than 1024 bytes, no CR/LF.
            // Avoid other problematic characters for general safety.
            // Replacing / with _ as it's a path separator in GCS for components, though GCS paths use /
            // This sanitization is for individual components to avoid them creating unwanted "subdirectories"
            // if a component itself contains '/'. The GenerateGcsObjectName method joins them with '/'.
            return component.Replace('/', '_')
                            .Replace('\\', '_')
                            .Replace('#', '_')
                            .Replace('?', '_')
                            .Replace('\r', '_') // Carriage Return
                            .Replace('\n', '_'); // Line Feed
        }

        public async Task<SnapshotHandle> StoreSnapshot(
            string jobId,
            long checkpointId,
            string taskManagerId,
            string operatorId,
            byte[] snapshotData)
        {
            if (snapshotData == null) throw new ArgumentNullException(nameof(snapshotData));

            var objectName = GenerateGcsObjectName(jobId, checkpointId, taskManagerId, operatorId);

            try
            {
                using (var stream = new MemoryStream(snapshotData, false))
                {
                    await _storageClient.UploadObjectAsync(
                        bucket: _options.BucketName,
                        objectName: objectName,
                        contentType: "application/octet-stream", // Good default for binary data
                        source: stream);
                }
                Console.WriteLine($"[GcsSnapshotStore] Snapshot stored to GCS: gs://{_options.BucketName}/{objectName}");
                return new SnapshotHandle($"gs://{_options.BucketName}/{objectName}");
            }
            catch (Google.GoogleApiException ex)
            {
                Console.WriteLine($"[GcsSnapshotStore] Error storing snapshot to GCS object gs://{_options.BucketName}/{objectName}. Error: {ex.Message}");
                throw new IOException($"Failed to store snapshot to GCS. Object: {objectName}", ex);
            }
        }

        public async Task<byte[]?> RetrieveSnapshot(SnapshotHandle handle)
        {
            if (handle == null || string.IsNullOrWhiteSpace(handle.Value)) throw new ArgumentNullException(nameof(handle));

            var expectedPrefix = $"gs://{_options.BucketName}/";
            if (!handle.Value.StartsWith(expectedPrefix))
            {
                throw new ArgumentException($"Invalid snapshot handle prefix. Expected '{expectedPrefix}'. Got: {handle.Value}", nameof(handle));
            }

            var objectName = handle.Value.Substring(expectedPrefix.Length);

            try
            {
                using (var memoryStream = new MemoryStream())
                {
                    await _storageClient.DownloadObjectAsync(
                        bucket: _options.BucketName,
                        objectName: objectName,
                        destination: memoryStream);

                    return memoryStream.ToArray();
                }
            }
            catch (Google.GoogleApiException ex) when (ex.Error?.Code == 404)
            {
                Console.WriteLine($"[GcsSnapshotStore] Snapshot GCS object not found: gs://{_options.BucketName}/{objectName}");
                return null;
            }
            catch (Google.GoogleApiException ex)
            {
                Console.WriteLine($"[GcsSnapshotStore] Error retrieving snapshot from GCS object gs://{_options.BucketName}/{objectName}. Error: {ex.Message}");
                throw new IOException($"Failed to retrieve snapshot from GCS. Object: {objectName}", ex);
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing)
            {
                _storageClient?.Dispose();
            }
            _disposed = true;
        }
    }
}
#nullable disable
