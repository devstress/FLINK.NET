namespace FlinkDotNet.Core.Abstractions.Windowing
{
    /// <summary>
    /// Interface for objects that derive watermarks from stream events.
    /// </summary>
    public interface IWatermarkGenerator<in T>
    {
        long CurrentWatermark { get; }
        void OnEvent(T element, long timestamp);
    }

    /// <summary>
    /// Simple watermark generator that emits monotonically increasing watermarks.
    /// </summary>
    public class MonotonicWatermarkGenerator<T> : IWatermarkGenerator<T>
    {
        private long _currentWatermark = long.MinValue;
        private readonly long _outOfOrderness;

        public MonotonicWatermarkGenerator(long outOfOrderness = 0)
        {
            _outOfOrderness = outOfOrderness;
        }

        public long CurrentWatermark => _currentWatermark;

        public void OnEvent(T element, long timestamp)
        {
            var potential = timestamp - _outOfOrderness;
            if (potential > _currentWatermark)
            {
                _currentWatermark = potential;
            }
        }
    }
}
