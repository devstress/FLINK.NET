using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace FlinkDotNet.Core.Networking
{
    /// <summary>
    /// Credit-based backpressure system for controlling data flow between tasks
    /// </summary>
    public interface ICreditBasedFlowController
    {
        /// <summary>
        /// Requests credits to send data to the specified output channel
        /// </summary>
        Task<bool> RequestCredits(string outputChannelId, int requestedCredits, CancellationToken cancellationToken = default);

        /// <summary>
        /// Grants credits to the specified input channel
        /// </summary>
        void GrantCredits(string inputChannelId, int credits);

        /// <summary>
        /// Notifies that credits have been consumed
        /// </summary>
        void ConsumeCredits(string channelId, int consumedCredits);

        /// <summary>
        /// Gets the current available credits for a channel
        /// </summary>
        int GetAvailableCredits(string channelId);
    }

    /// <summary>
    /// Credit-based flow controller implementation
    /// </summary>
    public class CreditBasedFlowController : ICreditBasedFlowController
    {
        private readonly ConcurrentDictionary<string, ChannelCredits> _channelCredits;
        private readonly int _maxCreditsPerChannel;
        private readonly int _lowWaterMark;
        private readonly int _highWaterMark;

        public CreditBasedFlowController(int maxCreditsPerChannel = 1000, int lowWaterMark = 200, int highWaterMark = 800)
        {
            _channelCredits = new ConcurrentDictionary<string, ChannelCredits>();
            _maxCreditsPerChannel = maxCreditsPerChannel;
            _lowWaterMark = lowWaterMark;
            _highWaterMark = highWaterMark;
        }

        public async Task<bool> RequestCredits(string outputChannelId, int requestedCredits, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(outputChannelId))
                throw new ArgumentException("Channel ID cannot be null or empty", nameof(outputChannelId));

            var channelCredits = _channelCredits.GetOrAdd(outputChannelId, _ => new ChannelCredits(_maxCreditsPerChannel));
            
            return await channelCredits.RequestCredits(requestedCredits, cancellationToken);
        }

        public void GrantCredits(string inputChannelId, int credits)
        {
            if (string.IsNullOrEmpty(inputChannelId))
                throw new ArgumentException("Channel ID cannot be null or empty", nameof(inputChannelId));

            var channelCredits = _channelCredits.GetOrAdd(inputChannelId, _ => new ChannelCredits(_maxCreditsPerChannel));
            channelCredits.GrantCredits(credits);
        }

        public void ConsumeCredits(string channelId, int consumedCredits)
        {
            if (_channelCredits.TryGetValue(channelId, out var channelCredits))
            {
                channelCredits.ConsumeCredits(consumedCredits);
            }
        }

        public int GetAvailableCredits(string channelId)
        {
            return _channelCredits.TryGetValue(channelId, out var channelCredits) 
                ? channelCredits.AvailableCredits 
                : 0;
        }
    }

    /// <summary>
    /// Manages credits for a single channel
    /// </summary>
    internal class ChannelCredits
    {
        private readonly object _lock = new object();
        private readonly SemaphoreSlim _creditSemaphore;
        private readonly int _maxCredits;
        private int _availableCredits;
        private readonly Queue<TaskCompletionSource<bool>> _pendingRequests = new();

        public ChannelCredits(int maxCredits)
        {
            _maxCredits = maxCredits;
            _availableCredits = maxCredits;
            _creditSemaphore = new SemaphoreSlim(maxCredits, maxCredits);
        }

        public int AvailableCredits
        {
            get
            {
                lock (_lock)
                {
                    return _availableCredits;
                }
            }
        }

        public async Task<bool> RequestCredits(int requestedCredits, CancellationToken cancellationToken)
        {
            if (requestedCredits <= 0)
                return true;

            lock (_lock)
            {
                if (_availableCredits >= requestedCredits)
                {
                    _availableCredits -= requestedCredits;
                    return true;
                }
            }

            // Wait for credits to become available
            var tcs = new TaskCompletionSource<bool>();
            
            lock (_lock)
            {
                _pendingRequests.Enqueue(tcs);
            }

            try
            {
                using (cancellationToken.Register(() => tcs.TrySetCanceled()))
                {
                    return await tcs.Task;
                }
            }
            catch (OperationCanceledException)
            {
                return false;
            }
        }

        public void GrantCredits(int credits)
        {
            if (credits <= 0)
                return;

            lock (_lock)
            {
                _availableCredits = Math.Min(_availableCredits + credits, _maxCredits);
                
                // Process pending requests
                while (_pendingRequests.Count > 0 && _availableCredits > 0)
                {
                    var request = _pendingRequests.Dequeue();
                    _availableCredits--;
                    request.TrySetResult(true);
                }
            }
        }

        public void ConsumeCredits(int consumedCredits)
        {
            if (consumedCredits <= 0)
                return;

            lock (_lock)
            {
                _availableCredits = Math.Max(0, _availableCredits - consumedCredits);
            }
        }
    }

    /// <summary>
    /// Backpressure monitor that tracks system pressure and adjusts flow control
    /// </summary>
    public class BackpressureMonitor
    {
        private readonly ICreditBasedFlowController _flowController;
        private readonly ConcurrentDictionary<string, ChannelMetrics> _channelMetrics;
        private readonly Timer _monitoringTimer;
        private readonly TimeSpan _monitoringInterval;

        public BackpressureMonitor(ICreditBasedFlowController flowController, TimeSpan? monitoringInterval = null)
        {
            _flowController = flowController ?? throw new ArgumentNullException(nameof(flowController));
            _channelMetrics = new ConcurrentDictionary<string, ChannelMetrics>();
            _monitoringInterval = monitoringInterval ?? TimeSpan.FromSeconds(1);
            
            _monitoringTimer = new Timer(MonitorBackpressure, null, _monitoringInterval, _monitoringInterval);
        }

        public void RecordDataSent(string channelId, int bytes)
        {
            var metrics = _channelMetrics.GetOrAdd(channelId, _ => new ChannelMetrics());
            Interlocked.Add(ref metrics.BytesSent, bytes);
            Interlocked.Increment(ref metrics.MessagesSent);
        }

        public void RecordDataReceived(string channelId, int bytes)
        {
            var metrics = _channelMetrics.GetOrAdd(channelId, _ => new ChannelMetrics());
            Interlocked.Add(ref metrics.BytesReceived, bytes);
            Interlocked.Increment(ref metrics.MessagesReceived);
        }

        public double GetBackpressureRatio(string channelId)
        {
            if (!_channelMetrics.TryGetValue(channelId, out var metrics))
                return 0.0;

            var totalCapacity = _flowController.GetAvailableCredits(channelId);
            if (totalCapacity == 0)
                return 1.0; // Fully backpressured

            var usedCredits = Math.Max(0, 1000 - totalCapacity); // Assuming max 1000 credits
            return (double)usedCredits / 1000.0;
        }

        private void MonitorBackpressure(object? state)
        {
            foreach (var kvp in _channelMetrics)
            {
                var channelId = kvp.Key;
                var metrics = kvp.Value;
                
                var backpressureRatio = GetBackpressureRatio(channelId);
                
                // Adjust credits based on backpressure
                if (backpressureRatio > 0.8) // High backpressure
                {
                    // Reduce credits granted to slow down the sender
                    _flowController.GrantCredits(channelId, 10);
                }
                else if (backpressureRatio < 0.2) // Low backpressure
                {
                    // Increase credits to allow faster sending
                    _flowController.GrantCredits(channelId, 100);
                }
                else
                {
                    // Normal backpressure, grant moderate credits
                    _flowController.GrantCredits(channelId, 50);
                }
            }
        }

        public void Dispose()
        {
            _monitoringTimer?.Dispose();
        }
    }

    /// <summary>
    /// Metrics for a single channel
    /// </summary>
    internal class ChannelMetrics
    {
        public long BytesSent;
        public long BytesReceived;
        public long MessagesSent;
        public long MessagesReceived;
        public DateTime LastActivity = DateTime.UtcNow;
    }
}