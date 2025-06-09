using FlinkDotNet.JobManager.Services.HighAvailability;
using Xunit;

namespace FlinkDotNet.JobManager.Tests
{
    public class HighAvailabilityCoordinatorTests
    {
        [Fact]
        public async Task AcquireAndResign()
        {
            var ha = new HighAvailabilityCoordinator();
            Assert.False(ha.IsLeader);
            Assert.True(await ha.TryAcquireLeadershipAsync());
            Assert.True(ha.IsLeader);
            ha.ResignLeadership();
            Assert.False(ha.IsLeader);
        }
    }
}
