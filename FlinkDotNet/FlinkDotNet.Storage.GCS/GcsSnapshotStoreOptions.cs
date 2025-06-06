#nullable enable
namespace FlinkDotNet.Storage.GCS
{
    /// <summary>
    /// Options for configuring the <see cref="GcsSnapshotStore"/>.
    /// </summary>
    public class GcsSnapshotStoreOptions
    {
        /// <summary>
        /// Gets or sets the Google Cloud Storage bucket name where snapshots will be stored.
        /// </summary>
        public string BucketName { get; set; } = "";

        /// <summary>
        /// Optional. Gets or sets the Google Cloud Project ID.
        /// If not set, the Project ID might be inferred from the credentials or environment.
        /// </summary>
        public string? ProjectId { get; set; }

        /// <summary>
        /// Optional. Gets or sets the path to the Google Cloud JSON credentials file.
        /// If not set, the store relies on the GOOGLE_APPLICATION_CREDENTIALS environment variable
        /// or other default credential discovery mechanisms of the Google Cloud SDK.
        /// </summary>
        public string? CredentialsFilePath { get; set; }

        /// <summary>
        /// Optional. Gets or sets a custom GCS endpoint.
        /// Useful for testing with GCS emulators (e.g., "http://localhost:4443").
        /// If null or empty, defaults to the standard GCS production endpoint.
        /// </summary>
        public string? Endpoint { get; set; }

        /// <summary>
        /// Optional. Gets or sets a base path or prefix to use within the bucket for all snapshots.
        /// For example, "flink_cluster_apps/my_job_snapshots".
        /// If empty, snapshots will be stored at the root of the bucket.
        /// </summary>
        public string BasePath { get; set; } = "";
    }
}
#nullable disable
