#nullable enable
using Xunit;
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Models;
using FlinkDotNet.Core.Abstractions.Models.State;
using FlinkDotNet.Core.Abstractions.Runtime;
using FlinkDotNet.Core.Abstractions.States;
using System; // For NotImplementedException, ArgumentNullException

namespace FlinkDotNet.JobManager.Tests.Core.Abstractions.Runtime // Adjusted namespace
{
    public class BasicRuntimeContextTests
    {
        [Fact]
        public void Constructor_InitializesPropertiesCorrectly()
        {
            var jobConfig = new JobConfiguration();
            var context = new BasicRuntimeContext(
                jobName: "TestJob",
                taskName: "TestTask",
                numberOfParallelSubtasks: 2,
                indexOfThisSubtask: 1,
                jobConfiguration: jobConfig);

            Assert.Equal("TestJob", context.JobName);
            Assert.Equal("TestTask", context.TaskName);
            Assert.Equal(2, context.NumberOfParallelSubtasks);
            Assert.Equal(1, context.IndexOfThisSubtask);
            Assert.Same(jobConfig, context.JobConfiguration);
        }

        [Fact]
        public void GetValueState_ReturnsNonNullValueState()
        {
            var context = new BasicRuntimeContext();
            var descriptor = new ValueStateDescriptor<int>("testState", defaultValue: 5);
            var state = context.GetValueState(descriptor);

            Assert.NotNull(state);
            Assert.IsAssignableFrom<IValueState<int>>(state);
            Assert.Equal(5, state.Value()); // Check default value is used
        }

        [Fact]
        public void GetValueState_SameDescriptor_ReturnsSameInstance()
        {
            var context = new BasicRuntimeContext();
            var descriptor = new ValueStateDescriptor<string>("myState");

            var state1 = context.GetValueState(descriptor);
            var state2 = context.GetValueState(descriptor);

            Assert.Same(state1, state2);
        }

        [Fact]
        public void GetValueState_DifferentDescriptors_ReturnsDifferentInstances()
        {
            var context = new BasicRuntimeContext();
            var descriptor1 = new ValueStateDescriptor<int>("stateOne");
            var descriptor2 = new ValueStateDescriptor<int>("stateTwo");

            var state1 = context.GetValueState(descriptor1);
            var state2 = context.GetValueState(descriptor2);

            Assert.NotSame(state1, state2);
        }

        [Fact]
        public void GetValueState_DescriptorIsNull_ThrowsArgumentNullException()
        {
            var context = new BasicRuntimeContext();
            Assert.Throws<ArgumentNullException>(() => context.GetValueState<int>(null!));
        }

        [Fact]
        public void GetListState_ThrowsNotImplementedException()
        {
            var context = new BasicRuntimeContext();
            var descriptor = new ListStateDescriptor<int>("testListState");
            Assert.Throws<NotImplementedException>(() => context.GetListState(descriptor));
        }

        [Fact]
        public void GetMapState_ThrowsNotImplementedException()
        {
            var context = new BasicRuntimeContext();
            var descriptor = new MapStateDescriptor<string, int>("testMapState");
            Assert.Throws<NotImplementedException>(() => context.GetMapState(descriptor));
        }
    }
}
#nullable disable
