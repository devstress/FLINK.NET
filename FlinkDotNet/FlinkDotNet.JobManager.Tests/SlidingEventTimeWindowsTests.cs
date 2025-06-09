using System.Collections.Generic;
using FlinkDotNet.Core.Api.Windowing;
using FlinkDotNet.Core.Abstractions.Common;
using FlinkDotNet.Core.Abstractions.Windowing;
using Xunit;

namespace FlinkDotNet.JobManager.Tests
{
    public class SlidingEventTimeWindowsTests
    {
        [Fact]
        public void AssignWindows_ReturnsExpectedWindows()
        {
            var assigner = SlidingEventTimeWindows<int>.Of(Time.MillisecondsMethod(10), Time.MillisecondsMethod(5));
            var ctx = new DefaultWindowAssignerContext();
            var windows = assigner.AssignWindows(1, 12, ctx);

            var expected = new List<TimeWindow>
            {
                new TimeWindow(10, 20),
                new TimeWindow(5, 15)
            };

            Assert.Equal(expected, windows);
        }
    }
}
