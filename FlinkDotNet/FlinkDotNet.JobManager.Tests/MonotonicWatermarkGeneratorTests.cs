using FlinkDotNet.Core.Abstractions.Windowing;
using Xunit;

namespace FlinkDotNet.JobManager.Tests
{
    public class MonotonicWatermarkGeneratorTests
    {
        [Fact]
        public void Watermark_IncreasesWithEvents()
        {
            var gen = new MonotonicWatermarkGenerator<int>();
            gen.OnEvent(1, 100);
            Assert.Equal(100, gen.CurrentWatermark);
            gen.OnEvent(2, 105);
            Assert.Equal(105, gen.CurrentWatermark);
        }

        [Fact]
        public void Watermark_RespectsOutOfOrderness()
        {
            var gen = new MonotonicWatermarkGenerator<int>(5);
            gen.OnEvent(1, 100);
            Assert.Equal(95, gen.CurrentWatermark);
            gen.OnEvent(2, 96);
            Assert.Equal(95, gen.CurrentWatermark);
        }
    }
}
