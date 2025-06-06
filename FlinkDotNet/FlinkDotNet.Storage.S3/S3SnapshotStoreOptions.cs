#nullable enable
namespace FlinkDotNet.Storage.S3
{
    /// <summary>
    /// Options for configuring the <see cref="S3SnapshotStore"/>, aligned with Apache Flink S3 configuration conventions.
    /// </summary>
    public class S3SnapshotStoreOptions
    {
        /// <summary>
        /// Gets or sets the AWS S3 bucket name where snapshots will be stored.
        /// (No direct Flink key, this is fundamental)
        /// </summary>
        public string BucketName { get; set; } = "";

        /// <summary>
        /// Gets or sets the AWS region for the S3 bucket.
        /// (Flink often relies on SDK default or environment variables like AWS_REGION)
        /// We'll keep this for explicit configuration.
        /// </summary>
        public string? Region { get; set; }

        /// <summary>
        /// Gets or sets the S3 endpoint URL. Corresponds to Flink's 's3.endpoint'.
        /// Useful for S3-compatible stores like MinIO.
        /// </summary>
        public string? Endpoint { get; set; } // Was ServiceUrl

        /// <summary>
        /// Gets or sets a flag to force path-style addressing. Corresponds to Flink's 's3.path.style.access'.
        /// Default is false (virtual-hosted style).
        /// </summary>
        public bool PathStyleAccess { get; set; } = false; // Was ForcePathStyle

        /// <summary>
        /// Gets or sets the AWS Access Key ID. Corresponds to Flink's 's3.access-key'.
        /// Optional if using IAM roles or environment variables (e.g., AWS_ACCESS_KEY_ID).
        /// </summary>
        public string? AccessKey { get; set; } // Was AccessKeyId

        /// <summary>
        /// Gets or sets the AWS Secret Access Key. Corresponds to Flink's 's3.secret-key'.
        /// Optional if using IAM roles or environment variables (e.g., AWS_SECRET_ACCESS_KEY).
        /// </summary>
        public string? SecretKey { get; set; } // Was SecretAccessKey

        /// <summary>
        /// Gets or sets the AWS Session Token. Used with temporary credentials.
        /// (No direct Flink key, but standard AWS SDK practice)
        /// </summary>
        public string? SessionToken { get; set; }

        /// <summary>
        /// Optional. Gets or sets a base path or prefix to use within the bucket for all snapshots.
        /// (Flink typically includes job ID in paths, this is an additional prefix if desired)
        /// </summary>
        public string BasePath { get; set; } = "";
    }
}
#nullable disable
