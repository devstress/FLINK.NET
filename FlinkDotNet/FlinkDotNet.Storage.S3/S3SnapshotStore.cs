#nullable enable
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Runtime; // For AWSCredentials, BasicAWSCredentials
using FlinkDotNet.Core.Abstractions.Storage; // For IStateSnapshotStore, SnapshotHandle

namespace FlinkDotNet.Storage.S3
{
    public class S3SnapshotStore : IStateSnapshotStore, IDisposable
    {
        private readonly S3SnapshotStoreOptions _options;
        private readonly AmazonS3Client _s3Client;
        private bool _disposed = false;

        public S3SnapshotStore(S3SnapshotStoreOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            if (string.IsNullOrWhiteSpace(options.BucketName))
            {
                throw new ArgumentException("S3 bucket name must be provided.", nameof(options.BucketName));
            }

            var s3Config = new AmazonS3Config
            {
                ForcePathStyle = options.PathStyleAccess
            };

            if (!string.IsNullOrWhiteSpace(options.Region))
            {
                s3Config.RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(options.Region);
            }

            if (!string.IsNullOrWhiteSpace(options.Endpoint))
            {
                s3Config.ServiceURL = options.Endpoint;
            }

            AWSCredentials? credentials = null;
            if (!string.IsNullOrWhiteSpace(options.AccessKey) && !string.IsNullOrWhiteSpace(options.SecretKey))
            {
                if(!string.IsNullOrWhiteSpace(options.SessionToken))
                {
                    credentials = new SessionAWSCredentials(options.AccessKey, options.SecretKey, options.SessionToken);
                }
                else
                {
                    credentials = new BasicAWSCredentials(options.AccessKey, options.SecretKey);
                }
            }

            if (credentials != null)
            {
                 _s3Client = new AmazonS3Client(credentials, s3Config);
            }
            else
            {
                // Use default credential chain (IAM role, environment variables, profile)
                _s3Client = new AmazonS3Client(s3Config);
            }
        }

        private string GenerateS3Key(string jobId, long checkpointId, string taskManagerId, string operatorId)
        {
            var parts = new[]
            {
                _options.BasePath,
                SanitizeS3KeyComponent(jobId),
                $"cp_{checkpointId}",
                SanitizeS3KeyComponent(taskManagerId),
                $"{SanitizeS3KeyComponent(operatorId)}.dat"
            };
            return string.Join("/", parts.Where(p => !string.IsNullOrEmpty(p)));
        }

        private string SanitizeS3KeyComponent(string component)
        {
            // S3 is quite permissive, but it's good to avoid characters that might be problematic
            // in some contexts or tools. This is a basic sanitizer.
            return component.Replace('\\', '_').Replace('?', '_').Replace('#', '_');
        }

        public async Task<SnapshotHandle> StoreSnapshot(
            string jobId,
            long checkpointId,
            string taskManagerId,
            string operatorId,
            byte[] snapshotData)
        {
            if (snapshotData == null)
            {
                throw new ArgumentNullException(nameof(snapshotData));
            }

            var s3Key = GenerateS3Key(jobId, checkpointId, taskManagerId, operatorId);

            try
            {
                using (var stream = new MemoryStream(snapshotData, false))
                {
                    var putRequest = new PutObjectRequest
                    {
                        BucketName = _options.BucketName,
                        Key = s3Key,
                        InputStream = stream
                        // Optionally: Add metadata, server-side encryption, storage class, etc.
                    };
                    await _s3Client.PutObjectAsync(putRequest);
                }
                Console.WriteLine($"Snapshot stored to S3: s3://{_options.BucketName}/{s3Key}");
                return new SnapshotHandle($"s3://{_options.BucketName}/{s3Key}");
            }
            catch (AmazonS3Exception ex)
            {
                Console.WriteLine($"Error storing snapshot to S3 key {s3Key}: {ex.Message}");
                throw new IOException($"Failed to store snapshot to S3. Key: {s3Key}", ex);
            }
        }

        public async Task<byte[]?> RetrieveSnapshot(SnapshotHandle handle)
        {
            if (handle == null || string.IsNullOrWhiteSpace(handle.Value))
            {
                throw new ArgumentNullException(nameof(handle));
            }

            var expectedPrefix = $"s3://{_options.BucketName}/";
            if (!handle.Value.StartsWith(expectedPrefix))
            {
                throw new ArgumentException($"Invalid snapshot handle prefix. Expected '{expectedPrefix}'. Got: {handle.Value}", nameof(handle));
            }

            var s3Key = handle.Value.Substring(expectedPrefix.Length);

            try
            {
                var getRequest = new GetObjectRequest
                {
                    BucketName = _options.BucketName,
                    Key = s3Key
                };

                using (GetObjectResponse response = await _s3Client.GetObjectAsync(getRequest))
                using (Stream responseStream = response.ResponseStream)
                using (var memoryStream = new MemoryStream())
                {
                    await responseStream.CopyToAsync(memoryStream);
                    return memoryStream.ToArray();
                }
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                Console.WriteLine($"Snapshot S3 key not found: {s3Key}");
                return null;
            }
            catch (AmazonS3Exception ex)
            {
                Console.WriteLine($"Error retrieving snapshot from S3 key {s3Key}: {ex.Message}");
                throw new IOException($"Failed to retrieve snapshot from S3. Key: {s3Key}", ex);
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
                _s3Client?.Dispose();
            }

            _disposed = true;
        }
    }
}
#nullable disable
