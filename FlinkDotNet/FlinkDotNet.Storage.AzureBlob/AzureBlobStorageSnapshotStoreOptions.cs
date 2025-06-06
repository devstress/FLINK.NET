#nullable enable
namespace FlinkDotNet.Storage.AzureBlob
{
    /// <summary>
    /// Options for configuring the <see cref="AzureBlobStorageSnapshotStore"/>.
    /// </summary>
    public class AzureBlobStorageSnapshotStoreOptions
    {
        /// <summary>
        /// Gets or sets the Azure Blob Storage connection string.
        /// This is the preferred method of configuration and typically includes account name, key, and endpoint.
        /// Example for Azurite: "UseDevelopmentStorage=true" or "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;".
        /// </summary>
        public string? ConnectionString { get; set; }

        /// <summary>
        /// Optional. Gets or sets the Azure Storage account name.
        /// Can be used as an alternative to ConnectionString if AccountKey is also provided.
        /// Inspired by Flink's configuration style (though Flink's key is more complex).
        /// </summary>
        public string? AccountName { get; set; }

        /// <summary>
        /// Optional. Gets or sets the Azure Storage account key.
        /// Can be used as an alternative to ConnectionString if AccountName is also provided.
        /// Inspired by Flink's configuration style.
        /// </summary>
        public string? AccountKey { get; set; }

        /// <summary>
        /// Optional. Gets or sets the Blob Service Endpoint.
        /// Useful when constructing a connection string from AccountName and AccountKey, especially for emulators or specific cloud environments.
        /// If not provided when AccountName/Key are used, it might default to standard Azure endpoints.
        /// Example for Azurite: "http://127.0.0.1:10000/devstoreaccount1"
        /// </summary>
        public string? BlobServiceEndpoint { get; set; }

        /// <summary>
        /// Gets or sets the name of the blob container where snapshots will be stored.
        /// The container will be created if it doesn't exist.
        /// </summary>
        public string ContainerName { get; set; } = "flinkdotnet-snapshots";

        /// <summary>
        /// Optional. Gets or sets a base path or prefix to use within the container for all snapshots.
        /// </summary>
        public string BasePath { get; set; } = "";
    }
}
#nullable disable
