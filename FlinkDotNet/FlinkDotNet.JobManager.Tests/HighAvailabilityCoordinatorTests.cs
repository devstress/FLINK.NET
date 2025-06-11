using FlinkDotNet.JobManager.Services.HighAvailability;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace FlinkDotNet.JobManager.Tests
{
    public class HighAvailabilityCoordinatorTests
    {
        [Fact]
        public async Task AcquireAndResign()
        {
            var logger = NullLogger<HighAvailabilityCoordinator>.Instance;
            var config = new HighAvailabilityConfiguration
            {
                HeartbeatInterval = TimeSpan.FromMilliseconds(100),
                LeaderElectionInterval = TimeSpan.FromMilliseconds(200)
            };
            
            using var ha = new HighAvailabilityCoordinator(logger, config);
            
            // Give time for the heartbeat to register the instance
            await Task.Delay(150);
            
            Assert.False(ha.IsLeader);
            Assert.True(await ha.TryAcquireLeadershipAsync());
            Assert.True(ha.IsLeader);
            await ha.ResignLeadershipAsync();
            Assert.False(ha.IsLeader);
        }

        [Fact]
        public async Task RegisterAndDeregisterInstances()
        {
            var logger = NullLogger<HighAvailabilityCoordinator>.Instance;
            using var ha = new HighAvailabilityCoordinator(logger);

            // Give time for the heartbeat to register the instance
            await Task.Delay(100);
            var initialCount = ha.InstanceCount;

            var instance = new JobManagerInstance
            {
                InstanceId = "test-instance",
                IsHealthy = true,
                Region = "us-east-1"
            };

            Assert.True(await ha.RegisterInstanceAsync(instance));
            Assert.Equal(initialCount + 1, ha.InstanceCount);

            Assert.True(await ha.DeregisterInstanceAsync("test-instance"));
            Assert.Equal(initialCount, ha.InstanceCount);
        }

        [Fact]
        public async Task GetClusterStatus()
        {
            var logger = NullLogger<HighAvailabilityCoordinator>.Instance;
            using var ha = new HighAvailabilityCoordinator(logger);

            // Give time for the heartbeat to register the instance
            await Task.Delay(100);

            var status = ha.GetClusterStatus();
            Assert.NotNull(status);
            Assert.True(status.TotalInstances >= 1); // The coordinator registers itself
            Assert.True(status.HealthyInstances >= 1);
        }
    }

    public class CrossRegionFailoverCoordinatorTests
    {
        [Fact]
        public async Task TriggerFailover()
        {
            var logger = NullLogger<CrossRegionFailoverCoordinator>.Instance;
            var config = new CrossRegionConfiguration
            {
                PrimaryRegion = "us-east-1",
                KnownRegions = new List<string> { "us-east-1", "us-west-2" },
                HealthCheckInterval = TimeSpan.FromMilliseconds(100)
            };

            using var coordinator = new CrossRegionFailoverCoordinator(logger, config);

            Assert.Equal("us-east-1", coordinator.PrimaryRegion);
            Assert.False(coordinator.IsFailoverActive);

            var success = await coordinator.TriggerFailoverAsync("us-west-2");
            Assert.True(success);
            Assert.Equal("us-west-2", coordinator.PrimaryRegion);
            Assert.True(coordinator.IsFailoverActive);
        }

        [Fact]
        public async Task RegisterRegion()
        {
            var logger = NullLogger<CrossRegionFailoverCoordinator>.Instance;
            var config = new CrossRegionConfiguration
            {
                KnownRegions = new List<string> { "us-east-1" }
            };

            using var coordinator = new CrossRegionFailoverCoordinator(logger, config);

            var endpoint = new RegionEndpoint
            {
                Host = "eu-west-1.example.com",
                Port = 8080
            };

            var success = await coordinator.RegisterRegionAsync("eu-west-1", endpoint);
            Assert.True(success);

            var status = coordinator.GetClusterStatus();
            Assert.Equal(2, status.TotalRegions);
        }

        [Fact]
        public async Task FailBack()
        {
            var logger = NullLogger<CrossRegionFailoverCoordinator>.Instance;
            var config = new CrossRegionConfiguration
            {
                PrimaryRegion = "us-east-1",
                KnownRegions = new List<string> { "us-east-1", "us-west-2" }
            };

            using var coordinator = new CrossRegionFailoverCoordinator(logger, config);

            // Trigger failover
            await coordinator.TriggerFailoverAsync("us-west-2");
            Assert.Equal("us-west-2", coordinator.PrimaryRegion);

            // Fail back
            var success = await coordinator.FailBackAsync("us-east-1");
            Assert.True(success);
            Assert.Equal("us-east-1", coordinator.PrimaryRegion);
            Assert.False(coordinator.IsFailoverActive);
        }
    }
}
