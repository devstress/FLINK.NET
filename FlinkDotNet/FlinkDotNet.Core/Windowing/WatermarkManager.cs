using System.Collections.Concurrent;
using FlinkDotNet.Core.Abstractions.Windowing;
using Microsoft.Extensions.Logging;

namespace FlinkDotNet.Core.Windowing
{
    /// <summary>
    /// Advanced watermark manager that handles alignment across multiple input streams
    /// </summary>
    public class WatermarkManager
    {
        private readonly ILogger<WatermarkManager> _logger;
        private readonly ConcurrentDictionary<string, long> _inputWatermarks;
        private readonly object _lock = new object();
        private long _currentWatermark = long.MinValue;
        private readonly List<IWatermarkListener> _listeners;

        public WatermarkManager(ILogger<WatermarkManager> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _inputWatermarks = new ConcurrentDictionary<string, long>();
            _listeners = new List<IWatermarkListener>();
        }

        /// <summary>
        /// Gets the current aligned watermark across all inputs
        /// </summary>
        public long CurrentWatermark => _currentWatermark;

        /// <summary>
        /// Registers a watermark listener
        /// </summary>
        public void AddListener(IWatermarkListener listener)
        {
            if (listener == null)
                throw new ArgumentNullException(nameof(listener));

            lock (_lock)
            {
                _listeners.Add(listener);
            }
        }

        /// <summary>
        /// Updates the watermark for a specific input stream
        /// </summary>
        public void UpdateWatermark(string inputId, long watermark)
        {
            if (string.IsNullOrEmpty(inputId))
                throw new ArgumentException("Input ID cannot be null or empty", nameof(inputId));

            _inputWatermarks.AddOrUpdate(inputId, watermark, (_, _) => watermark);
            
            lock (_lock)
            {
                // Calculate the minimum watermark across all inputs (watermark alignment)
                var minWatermark = _inputWatermarks.Values.DefaultIfEmpty(long.MinValue).Min();
                
                if (minWatermark > _currentWatermark)
                {
                    var oldWatermark = _currentWatermark;
                    _currentWatermark = minWatermark;
                    
                    _logger.LogDebug("Watermark advanced from {OldWatermark} to {NewWatermark}", oldWatermark, _currentWatermark);
                    
                    // Notify all listeners of watermark advancement
                    NotifyListeners(new Watermark(_currentWatermark));
                }
            }
        }

        /// <summary>
        /// Removes an input stream (e.g., when it finishes)
        /// </summary>
        public void RemoveInput(string inputId)
        {
            _inputWatermarks.TryRemove(inputId, out _);
            _logger.LogDebug("Removed input stream: {InputId}", inputId);
        }

        private void NotifyListeners(Watermark watermark)
        {
            foreach (var listener in _listeners)
            {
                try
                {
                    listener.OnWatermark(watermark);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error notifying watermark listener");
                }
            }
        }
    }

    /// <summary>
    /// Interface for components that need to be notified of watermark changes
    /// </summary>
    public interface IWatermarkListener
    {
        void OnWatermark(Watermark watermark);
    }

    /// <summary>
    /// Enhanced watermark generator with support for punctuated watermarks
    /// </summary>
    public class PunctuatedWatermarkGenerator<T> : IWatermarkGenerator<T>
    {
        private readonly Func<T, long> _timestampExtractor;
        private readonly Func<T, bool> _shouldEmitWatermark;
        private readonly long _outOfOrderness;
        private long _currentWatermark = long.MinValue;

        public PunctuatedWatermarkGenerator(
            Func<T, long> timestampExtractor,
            Func<T, bool> shouldEmitWatermark,
            long outOfOrderness = 0)
        {
            _timestampExtractor = timestampExtractor ?? throw new ArgumentNullException(nameof(timestampExtractor));
            _shouldEmitWatermark = shouldEmitWatermark ?? throw new ArgumentNullException(nameof(shouldEmitWatermark));
            _outOfOrderness = outOfOrderness;
        }

        public long CurrentWatermark => _currentWatermark;

        public void OnEvent(T element, long timestamp)
        {
            if (_shouldEmitWatermark(element))
            {
                var elementTimestamp = _timestampExtractor(element);
                var potential = elementTimestamp - _outOfOrderness;
                
                if (potential > _currentWatermark)
                {
                    _currentWatermark = potential;
                }
            }
        }
    }

    /// <summary>
    /// Periodic watermark generator that emits watermarks at regular intervals
    /// </summary>
    public class PeriodicWatermarkGenerator<T> : IWatermarkGenerator<T>, IDisposable
    {
        private readonly Func<T, long> _timestampExtractor;
        private readonly long _outOfOrderness;
        private readonly TimeSpan _interval;
        private readonly Timer _timer;
        private long _currentWatermark = long.MinValue;
        private long _maxTimestamp = long.MinValue;
        private bool _disposed;

        public event Action<long>? WatermarkEmitted;

        public PeriodicWatermarkGenerator(
            Func<T, long> timestampExtractor,
            long outOfOrderness = 0,
            TimeSpan? interval = null)
        {
            _timestampExtractor = timestampExtractor ?? throw new ArgumentNullException(nameof(timestampExtractor));
            _outOfOrderness = outOfOrderness;
            _interval = interval ?? TimeSpan.FromSeconds(1);
            
            _timer = new Timer(EmitWatermark, null, _interval, _interval);
        }

        public long CurrentWatermark => _currentWatermark;

        public void OnEvent(T element, long timestamp)
        {
            var elementTimestamp = _timestampExtractor(element);
            if (elementTimestamp > _maxTimestamp)
            {
                _maxTimestamp = elementTimestamp;
            }
        }

        private void EmitWatermark(object? state)
        {
            if (_disposed) return;

            var potential = _maxTimestamp - _outOfOrderness;
            if (potential > _currentWatermark)
            {
                _currentWatermark = potential;
                WatermarkEmitted?.Invoke(_currentWatermark);
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _timer?.Dispose();
                _disposed = true;
            }
        }
    }

    /// <summary>
    /// Watermark strategy that combines timestamp assignment and watermark generation
    /// </summary>
    public class WatermarkStrategy<T>
    {
        private readonly Func<T, long> _timestampAssigner;
        private readonly Func<IWatermarkGenerator<T>> _watermarkGeneratorFactory;

        public WatermarkStrategy(Func<T, long> timestampAssigner, Func<IWatermarkGenerator<T>> watermarkGeneratorFactory)
        {
            _timestampAssigner = timestampAssigner ?? throw new ArgumentNullException(nameof(timestampAssigner));
            _watermarkGeneratorFactory = watermarkGeneratorFactory ?? throw new ArgumentNullException(nameof(watermarkGeneratorFactory));
        }

        public long ExtractTimestamp(T element) => _timestampAssigner(element);
        public IWatermarkGenerator<T> CreateWatermarkGenerator() => _watermarkGeneratorFactory();

        /// <summary>
        /// Creates a watermark strategy for monotonically increasing timestamps
        /// </summary>
        public static WatermarkStrategy<T> ForMonotonousTimestamps(Func<T, long> timestampAssigner)
        {
            return new WatermarkStrategy<T>(
                timestampAssigner,
                () => new MonotonicWatermarkGenerator<T>()
            );
        }

        /// <summary>
        /// Creates a watermark strategy for bounded out-of-orderness
        /// </summary>
        public static WatermarkStrategy<T> ForBoundedOutOfOrderness(
            Func<T, long> timestampAssigner, 
            TimeSpan maxOutOfOrderness)
        {
            return new WatermarkStrategy<T>(
                timestampAssigner,
                () => new MonotonicWatermarkGenerator<T>(maxOutOfOrderness.Ticks / TimeSpan.TicksPerMillisecond)
            );
        }

        /// <summary>
        /// Creates a watermark strategy for punctuated watermarks
        /// </summary>
        public static WatermarkStrategy<T> ForPunctuatedWatermarks(
            Func<T, long> timestampAssigner,
            Func<T, bool> shouldEmitWatermark,
            long outOfOrderness = 0)
        {
            return new WatermarkStrategy<T>(
                timestampAssigner,
                () => new PunctuatedWatermarkGenerator<T>(timestampAssigner, shouldEmitWatermark, outOfOrderness)
            );
        }

        /// <summary>
        /// Creates a watermark strategy for periodic watermarks
        /// </summary>
        public static WatermarkStrategy<T> ForPeriodicWatermarks(
            Func<T, long> timestampAssigner,
            long outOfOrderness = 0,
            TimeSpan? interval = null)
        {
            return new WatermarkStrategy<T>(
                timestampAssigner,
                () => new PeriodicWatermarkGenerator<T>(timestampAssigner, outOfOrderness, interval)
            );
        }
    }

    /// <summary>
    /// Event time window assigner that uses watermarks to determine window completeness
    /// </summary>
    public class EventTimeWindowAssigner<T> : IWatermarkListener
    {
        private readonly TimeSpan _windowSize;
        private readonly TimeSpan _windowSlide;
        private readonly ILogger<EventTimeWindowAssigner<T>> _logger;
        private readonly ConcurrentDictionary<long, List<T>> _windowBuffers;
        private readonly object _lock = new object();

        public event Action<long, List<T>>? WindowComplete;

        public EventTimeWindowAssigner(TimeSpan windowSize, TimeSpan? windowSlide = null, ILogger<EventTimeWindowAssigner<T>>? logger = null)
        {
            _windowSize = windowSize;
            _windowSlide = windowSlide ?? windowSize; // Tumbling by default
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _windowBuffers = new ConcurrentDictionary<long, List<T>>();
        }

        public void AssignToWindow(T element, long timestamp)
        {
            var windowStart = GetWindowStart(timestamp);
            
            _windowBuffers.AddOrUpdate(windowStart, 
                new List<T> { element },
                (_, existing) => { existing.Add(element); return existing; });
        }

        public void OnWatermark(Watermark watermark)
        {
            lock (_lock)
            {
                var completedWindows = _windowBuffers.Keys
                    .Where(windowStart => windowStart + _windowSize.Ticks / TimeSpan.TicksPerMillisecond <= watermark.Timestamp)
                    .ToList();

                foreach (var windowStart in completedWindows)
                {
                    if (_windowBuffers.TryRemove(windowStart, out var elements))
                    {
                        _logger.LogDebug("Window completed: {WindowStart} with {Count} elements", windowStart, elements.Count);
                        WindowComplete?.Invoke(windowStart, elements);
                    }
                }
            }
        }

        private long GetWindowStart(long timestamp)
        {
            var slideMs = _windowSlide.Ticks / TimeSpan.TicksPerMillisecond;
            return timestamp - (timestamp % slideMs);
        }
    }
}