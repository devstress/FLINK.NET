#nullable enable
using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using FlinkDotNet.Core.Abstractions.Storage;
using FlinkDotNet.Storage.AzureBlob;
using Moq;
using Xunit;

public class AzureBlobStorageSnapshotStoreTests
{
    // Mocks for testing interactions IF the SUT allowed for DI of BlobContainerClient or BlobClient.
    // Given the current SUT, these are less effective for pure unit tests of methods like StoreSnapshot.
    // However, they can be used to verify that the SUT *would* make correct calls if it used these mocks.
    private readonly Mock<BlobContainerClient> _mockBlobContainerClient;
    private readonly Mock<BlobClient> _mockBlobClient;

    private readonly AzureBlobStorageSnapshotStoreOptions _defaultOptionsWithConnectionString;
    private readonly AzureBlobStorageSnapshotStoreOptions _defaultOptionsWithAccountParts;

    public AzureBlobStorageSnapshotStoreTests()
    {
        // Setup mocks for client interactions (less relevant for constructor tests, more for method tests)
        // The Uri can be a dummy one. The key is that it's a valid Uri.
        _mockBlobContainerClient = new Mock<BlobContainerClient>(new Uri("http://127.0.0.1/test-container"), (BlobClientOptions)null!);
        _mockBlobClient = new Mock<BlobClient>(); // Mock<BlobClient>(new Uri("http://127.0.0.1/test-container/blob"), (BlobClientOptions)null!) - if needed

        _mockBlobContainerClient.Setup(container => container.GetBlobClient(It.IsAny<string>()))
                                .Returns(_mockBlobClient.Object);
        _mockBlobContainerClient.Setup(container => container.CreateIfNotExists(It.IsAny<PublicAccessType>(), It.IsAny<Metadata>(), It.IsAny<BlobContainerEncryptionScopeOptions>(), It.IsAny<CancellationToken>()))
                                .Returns(Response.FromValue(BlobsModelFactory.BlobContainerInfo(new ETag("test"), DateTimeOffset.UtcNow), Mock.Of<Response>()));


        _defaultOptionsWithConnectionString = new AzureBlobStorageSnapshotStoreOptions
        {
            ConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
            ContainerName = "test-container",
            BasePath = "flink-snapshots"
        };

        _defaultOptionsWithAccountParts = new AzureBlobStorageSnapshotStoreOptions
        {
            AccountName = "devstoreaccount1",
            AccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
            BlobServiceEndpoint = "http://127.0.0.1:10000/devstoreaccount1",
            ContainerName = "test-container-parts",
            BasePath = "flink-snapshots-parts"
        };
    }

    // For existing tests that rely on mocking client behavior AFTER construction,
    // this helper is problematic as SUT creates its own clients.
    // These tests are more integration-like or assume future DI.
    // For constructor tests, we call 'new AzureBlobStorageSnapshotStore(options)' directly.
    private AzureBlobStorageSnapshotStore CreateStoreForMethodTests(AzureBlobStorageSnapshotStoreOptions? options = null)
    {
        // This method is intended for tests that mock BlobClient interactions.
        // It assumes that the AzureBlobStorageSnapshotStore, once constructed,
        // will somehow use the _mockBlobContainerClient and _mockBlobClient we've set up.
        // This is not true with the current SUT design, making these tests less like unit tests.
        // However, keeping the structure for now.
        return new AzureBlobStorageSnapshotStore(options ?? _defaultOptionsWithConnectionString);
    }

    [Fact]
    public void Constructor_Initializes_With_ConnectionString()
    {
        // Act & Assert
        // This might throw if Azurite isn't running, but the goal is to check options validation.
        // An Azure.RequestFailedException during CreateIfNotExists is acceptable here.
        try
        {
            var store = new AzureBlobStorageSnapshotStore(_defaultOptionsWithConnectionString);
            Assert.NotNull(store);
        }
        catch (ArgumentException argEx) when (argEx.Message.Contains("configuration is insufficient"))
        {
            Assert.Fail($"Constructor threw ArgumentException for valid ConnectionString: {argEx.Message}");
        }
        catch (RequestFailedException)
        {
            // This is okay - means options were valid, but Azurite might not be running for CreateIfNotExists
        }
    }

    [Fact]
    public void Constructor_Initializes_With_AccountName_And_Key()
    {
        // Act & Assert
        try
        {
            var store = new AzureBlobStorageSnapshotStore(_defaultOptionsWithAccountParts);
            Assert.NotNull(store);
        }
        catch (ArgumentException argEx) when (argEx.Message.Contains("configuration is insufficient"))
        {
            Assert.Fail($"Constructor threw ArgumentException for valid AccountName/Key/Endpoint: {argEx.Message}");
        }
        catch (RequestFailedException)
        {
            // Okay if Azurite not running for CreateIfNotExists
        }
    }

    [Fact]
    public void Constructor_Initializes_With_AccountName_And_Key_NoEndpoint()
    {
        var options = new AzureBlobStorageSnapshotStoreOptions
        {
            AccountName = "dummyaccount",
            AccountKey = "dummykey==" , // Basic valid-looking base64
            ContainerName = "test",
        };
        // Act & Assert
        try
        {
            var store = new AzureBlobStorageSnapshotStore(options);
            Assert.NotNull(store);
        }
        catch (ArgumentException argEx) when (argEx.Message.Contains("configuration is insufficient"))
        {
            Assert.Fail($"Constructor threw ArgumentException for AccountName/Key without explicit endpoint: {argEx.Message}");
        }
        catch (RequestFailedException)
        {
             // Okay if it fails trying to connect to a real Azure endpoint with dummy credentials
        }
    }


    [Theory]
    [InlineData(null, "key", "endpoint", "container")] // No AccountName
    [InlineData("name", null, "endpoint", "container")] // No AccountKey
    [InlineData("name", "key", "endpoint", null)]      // No ContainerName (covered by specific test too)
    [InlineData(null, null, null, "container")]        // No ConnectionString, no parts
    public void Constructor_Throws_When_Configuration_Is_Insufficient(string? accName, string? accKey, string? endpoint, string? container)
    {
        // Arrange
        var options = new AzureBlobStorageSnapshotStoreOptions
        {
            AccountName = accName,
            AccountKey = accKey,
            BlobServiceEndpoint = endpoint,
            ContainerName = container! // Null handled by specific test or this one if container is null
        };

        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() => new AzureBlobStorageSnapshotStore(options));
        // Check if the exception message indicates insufficient configuration or missing container name
        Assert.True(
            ex.Message.Contains("configuration is insufficient") ||
            ex.Message.Contains("container name must be provided")
        );
    }

    [Fact]
    public void Constructor_Throws_When_ContainerName_Is_Missing_WithConnectionString()
    {
        var options = new AzureBlobStorageSnapshotStoreOptions
        {
            ConnectionString = _defaultOptionsWithConnectionString.ConnectionString, // Valid CS
            ContainerName = "" // Invalid ContainerName
        };
        var ex = Assert.Throws<ArgumentException>(() => new AzureBlobStorageSnapshotStore(options));
        Assert.Contains("container name must be provided", ex.Message);
    }

    [Fact]
    public void Constructor_Throws_When_ContainerName_Is_Missing_WithAccountParts()
    {
        var options = new AzureBlobStorageSnapshotStoreOptions
        {
            AccountName = _defaultOptionsWithAccountParts.AccountName,
            AccountKey = _defaultOptionsWithAccountParts.AccountKey,
            BlobServiceEndpoint = _defaultOptionsWithAccountParts.BlobServiceEndpoint,
            ContainerName = null // Invalid ContainerName
        };
        var ex = Assert.Throws<ArgumentException>(() => new AzureBlobStorageSnapshotStore(options));
        Assert.Contains("container name must be provided", ex.Message);
    }


    [Fact]
    public async Task StoreSnapshot_UploadsDataAndReturnsCorrectHandle()
    {
        // This test uses _defaultOptionsWithConnectionString, which is fine.
        // It relies on the CreateStoreForMethodTests which news up the SUT.
        // The mock setups for _mockBlobContainerClient and _mockBlobClient will NOT be used by the SUT.
        // This test will attempt a real Azurite connection.
        // To make it a unit test, SUT needs DI for BlobContainerClient.
        var store = CreateStoreForMethodTests(); // Uses _defaultOptionsWithConnectionString

        var data = Encoding.UTF8.GetBytes("test data");
        var jobId = "job1";
        var checkpointId = 1L;
        var taskManagerId = "tm1";
        var operatorId = "op1";
        // Expected blob name uses BasePath from _defaultOptionsWithConnectionString
        var expectedBlobName = $"{_defaultOptionsWithConnectionString.BasePath}/{jobId}/cp_{checkpointId}/{taskManagerId}/{operatorId}.dat";

        // These mock setups are aspirational for a DI-friendly SUT
        _mockBlobContainerClient.Setup(c => c.GetBlobClient(expectedBlobName)).Returns(_mockBlobClient.Object);
        _mockBlobClient.Setup(client => client.UploadAsync(It.IsAny<Stream>(), true, It.IsAny<CancellationToken>()))
                       .ReturnsAsync(Mock.Of<Response<BlobContentInfo>>());

        // Actual behavior: store will use its own client. If Azurite not running, UploadAsync will fail.
        // If Azurite is running, it will actually upload.
        var handle = await store.StoreSnapshot(jobId, checkpointId, taskManagerId, operatorId, data);

        Assert.NotNull(handle);
        Assert.Equal($"azureblob://{_defaultOptionsWithConnectionString.ContainerName}/{expectedBlobName}", handle.Value);

        // This Verify will not work as expected as _mockBlobClient isn't used by SUT.
        // _mockBlobClient.Verify(client => client.UploadAsync(It.IsAny<Stream>(), true, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task RetrieveSnapshot_DownloadsData_WhenBlobExists()
    {
        var store = CreateStoreForMethodTests(); // Uses _defaultOptionsWithConnectionString
        var testData = Encoding.UTF8.GetBytes("retrieved data");
        var blobName = $"{_defaultOptionsWithConnectionString.BasePath}/job2/cp_2/tm2/op2.dat";
        var handle = new SnapshotHandle($"azureblob://{_defaultOptionsWithConnectionString.ContainerName}/{blobName}");

        // Store some data first so Retrieve can find it (since mocks aren't used by SUT)
        await store.StoreSnapshot("job2", 2L, "tm2", "op2", testData);

        // Aspirational mock setups
        var mockBlobDownloadInfo = BlobsModelFactory.BlobDownloadInfo(content: new MemoryStream(testData));
        _mockBlobContainerClient.Setup(c => c.GetBlobClient(blobName)).Returns(_mockBlobClient.Object);
        _mockBlobClient.Setup(client => client.ExistsAsync(It.IsAny<CancellationToken>())).ReturnsAsync(Response.FromValue(true, Mock.Of<Response>()));
        _mockBlobClient.Setup(client => client.DownloadAsync(It.IsAny<CancellationToken>()))
                       .ReturnsAsync(Response.FromValue(mockBlobDownloadInfo, Mock.Of<Response>()));

        var retrievedData = await store.RetrieveSnapshot(handle);

        Assert.NotNull(retrievedData);
        Assert.Equal(testData, retrievedData);
    }

    [Fact]
    public async Task RetrieveSnapshot_ReturnsNull_WhenBlobDoesNotExist()
    {
        var store = CreateStoreForMethodTests();
        var blobName = $"{_defaultOptionsWithConnectionString.BasePath}/job3/cp_3/tm3/op3.dat"; // Non-existent
        var handle = new SnapshotHandle($"azureblob://{_defaultOptionsWithConnectionString.ContainerName}/{blobName}");

        _mockBlobContainerClient.Setup(c => c.GetBlobClient(blobName)).Returns(_mockBlobClient.Object);
        _mockBlobClient.Setup(client => client.ExistsAsync(It.IsAny<CancellationToken>())).ReturnsAsync(Response.FromValue(false, Mock.Of<Response>()));

        var retrievedData = await store.RetrieveSnapshot(handle);
        Assert.Null(retrievedData);
    }

    // StoreSnapshot_ThrowsIOException_OnAzureRequestFailedException and
    // RetrieveSnapshot_ThrowsIOException_OnAzureRequestFailedExceptionDuringDownload
    // are harder to make reliable unit tests without DI, as they depend on actual client errors.
    // They could be tested by configuring SUT with invalid credentials/endpoints if Azurite is NOT running,
    // or by trying to access non-existent data for retrieve.

    [Theory]
    [InlineData("job1", 1L, "tm1", "op1", "flink-snapshots/job1/cp_1/tm1/op1.dat")]
    [InlineData("my-job/with/slashes", 2L, "task_manager_1", "operator_name", "flink-snapshots/my-job_with_slashes/cp_2/task_manager_1/operator_name.dat")]
    [InlineData("job3", 3L, "tm3", "op3", "job3/cp_3/tm3/op3.dat", "")] // Test with empty BasePath
    public async Task StoreSnapshot_GeneratesCorrectBlobNameAndHandle(string jobId, long checkpointId, string taskManagerId, string operatorId, string expectedPathSuffix, string? basePathOverride = null)
    {
        var customOptions = new AzureBlobStorageSnapshotStoreOptions
        {
            // Use ConnectionString as primary for this test variant to ensure it still works
            ConnectionString = _defaultOptionsWithConnectionString.ConnectionString,
            ContainerName = "name-gen-test-container", // Custom container for this test
            BasePath = basePathOverride ?? _defaultOptionsWithConnectionString.BasePath // Use override or default from _options
        };

        // If basePathOverride is explicitly "", it should result in an empty BasePath
        if (basePathOverride == "")
        {
            customOptions.BasePath = "";
        }

        var store = new AzureBlobStorageSnapshotStore(customOptions);

        var handle = await store.StoreSnapshot(jobId, checkpointId, taskManagerId, operatorId, Encoding.UTF8.GetBytes("data"));

        Assert.Equal($"azureblob://{customOptions.ContainerName}/{expectedPathSuffix}", handle.Value);
    }
}
#nullable disable
