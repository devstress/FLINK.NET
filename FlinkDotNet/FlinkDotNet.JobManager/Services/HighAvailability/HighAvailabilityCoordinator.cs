using System.Threading;
using System.Threading.Tasks;

namespace FlinkDotNet.JobManager.Services.HighAvailability
{
    /// <summary>
    /// Minimal leader election coordinator for high availability tests.
    /// </summary>
    public class HighAvailabilityCoordinator
    {
        private int _isLeader;

        public bool IsLeader => _isLeader == 1;

        public Task<bool> TryAcquireLeadershipAsync()
        {
            var result = Interlocked.CompareExchange(ref _isLeader, 1, 0) == 0;
            return Task.FromResult(result);
        }

        public void ResignLeadership()
        {
            Interlocked.Exchange(ref _isLeader, 0);
        }
    }
}
