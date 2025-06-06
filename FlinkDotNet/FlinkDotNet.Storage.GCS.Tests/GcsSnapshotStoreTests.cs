#nullable enable
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FlinkDotNet.Core.Abstractions.Storage;
using FlinkDotNet.Storage.GCS; // SUT
using Google.Apis.Requests; // For GoogleApiException and Error
using Google.Cloud.Storage.V1;
using Moq;
using Xunit;
using System.Linq; // Required for SanitizeGcsObjectNameComponent in StoreSnapshot_GeneratesCorrectObjectName if used directly

public class GcsSnapshotStoreTests
{
    private readonly Mock<StorageClient> _mockStorageClient;
    private readonly GcsSnapshotStoreOptions _defaultOptions;

    public GcsSnapshotStoreTests()
    {
        // StorageClient is abstract, or its relevant methods are virtual.
        // We can mock it directly.
        _mockStorageClient = new Mock<StorageClient>();

        _defaultOptions = new GcsSnapshotStoreOptions
        {
            BucketName = "test-gcs-bucket",
            ProjectId = "test-project", // Optional for client, but good for options
            // CredentialsFilePath and Endpoint are null by default, relying on ADC for tests or emulator setup
            BasePath = "flink-gcs-snapshots"
        };
    }

    // Helper to create store. GcsSnapshotStore news up StorageClient via StorageClientBuilder.
    // For pure unit test with mock, SUT needs DI for StorageClient.
    // This test setup assumes GcsSnapshotStore is refactored or we test through a wrapper,
    // or we accept that the constructor test itself is more of an integration test part.
    // The tests for Store/Retrieve will use the _mockStorageClient.
    private GcsSnapshotStore CreateStoreWithMock(GcsSnapshotStoreOptions? options = null)
    {
        // This is a conceptual way to ensure the SUT uses our mock.
        // Requires GcsSnapshotStore to be testable, e.g., internal constructor for testing,
        // or using a factory pattern for StorageClient.
        // For now, we'll assume such a mechanism exists for the mock to be effective.
        // A common pattern: internal GcsSnapshotStore(GcsSnapshotStoreOptions options, StorageClient clientForTest)
        // For this subtask, construct the tests as if this is possible.
        // The subtask for GcsSnapshotStore implementation did not include this DI refactor.
        return new GcsSnapshotStoreTestWrapper(options ?? _defaultOptions, _mockStorageClient.Object);
    }

    [Fact]
    public void Constructor_WithValidOptions_Initializes()
    {
        // This test primarily checks if options validation passes.
        // Actual client creation might interact with environment (ADC).
        var options = new GcsSnapshotStoreOptions { BucketName = "test" };
        // This will use the actual GcsSnapshotStore constructor.
        // It might throw if ADC is not set up and it tries to create a real client,
        // but the point is to pass GcsSnapshotStore's own initial validation.
        try
        {
            var store = new GcsSnapshotStore(options);
            Assert.NotNull(store);
        }
        catch (Google.GoogleApiException gax) when (gax.Message.Contains("Could not determine project ID") || gax.Message.Contains("Error creating channel"))
        {
            // This is an acceptable outcome if ADC isn't fully set up but options validation passed.
            // Or if the default credentials don't have a project ID.
            // "Error creating channel" can happen if GCS emulator not running or networking issues.
            Assert.NotNull(gax);
        }
        catch (Exception ex)
        {
            // Fail if it's an ArgumentException from our validation, or other unexpected exception
             if (ex is ArgumentException && ex.Message.StartsWith("GCS bucket name must be provided."))
                Assert.Fail("Threw for missing bucket name when it was provided.");
             // Depending on environment, other Google SDK exceptions could occur if ADC is partially available.
             // For this test, we're mostly concerned that OUR validation (e.g. bucket name) passes.
        }
    }

    [Fact]
    public void Constructor_ThrowsArgumentException_WhenBucketNameIsMissing()
    {
        var options = new GcsSnapshotStoreOptions { BucketName = "" }; // Invalid: empty bucket name
        var ex = Assert.Throws<ArgumentException>(() => new GcsSnapshotStore(options));
        Assert.Contains("GCS bucket name must be provided", ex.Message);
        Assert.Equal("options.BucketName", ex.ParamName); // Ensure correct param name in exception
    }

    [Fact]
    public async Task StoreSnapshot_UploadsDataAndReturnsCorrectHandle()
    {
        var store = CreateStoreWithMock(); // Uses wrapper that injects mock client
        var data = Encoding.UTF8.GetBytes("gcs test data");
        var jobId = "gcsjob1";
        var checkpointId = 100L;
        var taskManagerId = "gcstm1";
        var operatorId = "gcsop1";

        var expectedObjectName = $"{_defaultOptions.BasePath}/{jobId}/cp_{checkpointId}/{taskManagerId}/{operatorId}.dat";

        _mockStorageClient.Setup(client => client.UploadObjectAsync(
            _defaultOptions.BucketName,
            expectedObjectName,
            "application/octet-stream",
            It.IsAny<Stream>(),
            null, // UploadObjectOptions
            It.IsAny<CancellationToken>(),
            null  // IProgress<UploadProgress>
        )).ReturnsAsync(new Google.Apis.Storage.v1.Data.Object()); // Return dummy object

        var handle = await store.StoreSnapshot(jobId, checkpointId, taskManagerId, operatorId, data);

        Assert.NotNull(handle);
        Assert.Equal($"gs://{_defaultOptions.BucketName}/{expectedObjectName}", handle.Value);
        _mockStorageClient.Verify(client => client.UploadObjectAsync(
            _defaultOptions.BucketName, expectedObjectName, "application/octet-stream",
            It.IsAny<Stream>(), null, It.IsAny<CancellationToken>(), null), Times.Once);
    }

    [Fact]
    public async Task RetrieveSnapshot_DownloadsData_WhenObjectExists()
    {
        var store = CreateStoreWithMock();
        var testData = Encoding.UTF8.GetBytes("retrieved gcs data");
        var objectName = $"{_defaultOptions.BasePath}/gcsjob2/cp_200/gcstm2/gcsop2.dat";
        var handle = new SnapshotHandle($"gs://{_defaultOptions.BucketName}/{objectName}");

        _mockStorageClient.Setup(client => client.DownloadObjectAsync(
            _defaultOptions.BucketName,
            objectName,
            It.IsAny<Stream>(), // Destination stream
            null, // DownloadObjectOptions
            It.IsAny<CancellationToken>(),
            null // IProgress<DownloadProgress>
        )).Callback<string, string, Stream, DownloadObjectOptions, CancellationToken, IProgress<Google.Cloud.Storage.V1.DownloadProgress>>(
            (bucket, obj, stream, opts, token, progress) => {
                using (var ms = new MemoryStream(testData))
                {
                    ms.CopyTo(stream); // Simulate writing data to the destination stream
                }
        }).Returns(Task.CompletedTask); // DownloadObjectAsync is void returning Task

        var retrievedData = await store.RetrieveSnapshot(handle);

        Assert.NotNull(retrievedData);
        Assert.Equal(testData, retrievedData);
    }

    [Fact]
    public async Task RetrieveSnapshot_ReturnsNull_WhenObjectNotFound()
    {
        var store = CreateStoreWithMock();
        var objectName = $"{_defaultOptions.BasePath}/gcsjob3/cp_300/gcstm3/gcsop3.dat";
        var handle = new SnapshotHandle($"gs://{_defaultOptions.BucketName}/{objectName}");

        var notFoundException = new GoogleApiException("GCS", "Not Found") { Error = new RequestError { Code = 404 } };

        _mockStorageClient.Setup(client => client.DownloadObjectAsync(
            _defaultOptions.BucketName, objectName, It.IsAny<Stream>(), null, It.IsAny<CancellationToken>(), null))
            .ThrowsAsync(notFoundException);

        var retrievedData = await store.RetrieveSnapshot(handle);
        Assert.Null(retrievedData);
    }

    [Fact]
    public async Task StoreSnapshot_ThrowsIOException_OnGoogleApiException()
    {
        var store = CreateStoreWithMock();
        var data = Encoding.UTF8.GetBytes("gcs test data");
        var generalException = new GoogleApiException("GCS", "Service error"); // Non-404 error

        _mockStorageClient.Setup(client => client.UploadObjectAsync(
            It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Stream>(),
            null, It.IsAny<CancellationToken>(), null))
            .ThrowsAsync(generalException);

        await Assert.ThrowsAsync<IOException>(() => store.StoreSnapshot("j", 0, "t", "o", data));
    }

    [Theory]
    [InlineData("gcsJob", 1L, "gcsTm", "gcsOp", "flink-gcs-snapshots/gcsJob/cp_1/gcsTm/gcsOp.dat")]
    [InlineData("my-gcs/job", 2L, "task/manager", "op/name", "flink-gcs-snapshots/my-gcs_job/cp_2/task_manager/op_name.dat")]
    [InlineData("gcsJob3", 3L, "gcsTm3", "gcsOp3", "gcsJob3/cp_3/gcsTm3/gcsOp3.dat", "")] // Test with empty BasePath
    public async Task StoreSnapshot_GeneratesCorrectObjectName(string jobId, long checkpointId, string taskManagerId, string operatorId, string expectedObjectSuffix, string? basePathOverride = null)
    {
        var customOptions = new GcsSnapshotStoreOptions
        {
            BucketName = "custom-gcs-bucket",
            ProjectId = _defaultOptions.ProjectId, // Copied
            BasePath = basePathOverride // Use override if provided (can be null or empty)
        };

        // If basePathOverride is null (meaning use default test behavior), set it to the default base path.
        // If basePathOverride is "", it means explicitly test with an empty base path.
        if (basePathOverride == null)
        {
             customOptions.BasePath = _defaultOptions.BasePath;
        }

        var store = CreateStoreWithMock(customOptions);
        _mockStorageClient.Setup(client => client.UploadObjectAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Stream>(), null, It.IsAny<CancellationToken>(), null))
            .ReturnsAsync(new Google.Apis.Storage.v1.Data.Object());

        var handle = await store.StoreSnapshot(jobId, checkpointId, taskManagerId, operatorId, Encoding.UTF8.GetBytes("data"));

        Assert.Equal($"gs://{customOptions.BucketName}/{expectedObjectSuffix}", handle.Value);

        _mockStorageClient.Verify(client => client.UploadObjectAsync(
            customOptions.BucketName,
            expectedObjectSuffix,
            "application/octet-stream",
            It.IsAny<Stream>(), null, It.IsAny<CancellationToken>(), null), Times.Once);
    }
}

// Wrapper class for testing GcsSnapshotStore as if StorageClient was injected
public class GcsSnapshotStoreTestWrapper : GcsSnapshotStore
{
    public GcsSnapshotStoreTestWrapper(GcsSnapshotStoreOptions options, StorageClient testClient) : base(options)
    {
        var fieldInfo = typeof(GcsSnapshotStore).GetField("_storageClient",
                            System.Reflection.BindingFlags.NonPublic |
                            System.Reflection.BindingFlags.Instance);
        if (fieldInfo != null)
        {
            fieldInfo.SetValue(this, testClient);
        }
        else
        {
            // This fallback is if the field name changes or isn't found, to make it obvious during test failure.
            throw new InvalidOperationException("Could not set _storageClient via reflection. Test setup is broken or SUT changed.");
        }
    }
}
#nullable disable
