using FlinkDotNet.JobManager.Services.HighAvailability;
using Xunit;

namespace FlinkDotNet.JobManager.Tests
{
    public class HighAvailabilityCoordinatorTests
    {
        [Fact]
        public void AcquireAndResign()
        {
            var ha = new HighAvailabilityCoordinator();
            Assert.False(ha.IsLeader);
            Assert.True(ha.TryAcquireLeadershipAsync().Result);
            Assert.True(ha.IsLeader);
            ha.ResignLeadership();
            Assert.False(ha.IsLeader);
        }
    }
}
