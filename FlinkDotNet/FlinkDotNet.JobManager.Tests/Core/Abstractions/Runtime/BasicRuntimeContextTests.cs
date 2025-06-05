#nullable enable
using Xunit;
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Models;
using FlinkDotNet.Core.Abstractions.Models.State;
using FlinkDotNet.Core.Abstractions.Runtime;
using FlinkDotNet.Core.Abstractions.States;
using System; // For NotImplementedException, ArgumentNullException
using System.Collections.Generic; // For List in tests
using System.Linq; // For Linq extensions on IEnumerable in tests


namespace FlinkDotNet.JobManager.Tests.Core.Abstractions.Runtime
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
        public void Constructor_DefaultValues_AreSet()
        {
            var context = new BasicRuntimeContext();
            Assert.Equal("DefaultJob", context.JobName);
            Assert.Equal("DefaultTask", context.TaskName);
            Assert.Equal(1, context.NumberOfParallelSubtasks);
            Assert.Equal(0, context.IndexOfThisSubtask);
            Assert.NotNull(context.JobConfiguration); // Default JobConfiguration is created
        }

        // --- Tests for GetValueState ---
        [Fact]
        public void GetValueState_ReturnsNonNullValueState()
        {
            var context = new BasicRuntimeContext();
            var descriptor = new ValueStateDescriptor<int>("testState", defaultValue: 5);
            var state = context.GetValueState(descriptor);

            Assert.NotNull(state);
            Assert.IsAssignableFrom<IValueState<int>>(state);
            Assert.Equal(5, state.Value());
        }

        [Fact]
        public void GetValueState_SameDescriptor_ReturnsSameInstance()
        {
            var context = new BasicRuntimeContext();
            var descriptor = new ValueStateDescriptor<string>("myValueState");

            var state1 = context.GetValueState(descriptor);
            var state2 = context.GetValueState(descriptor);

            Assert.Same(state1, state2);
        }

        [Fact]
        public void GetValueState_DifferentDescriptors_ReturnsDifferentInstances()
        {
            var context = new BasicRuntimeContext();
            var descriptor1 = new ValueStateDescriptor<int>("valueStateOne");
            var descriptor2 = new ValueStateDescriptor<int>("valueStateTwo");

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

        // --- Tests for GetListState ---
        [Fact]
        public void GetListState_ReturnsNonNullListState()
        {
            var context = new BasicRuntimeContext();
            var descriptor = new ListStateDescriptor<int>("testListState");
            var state = context.GetListState(descriptor);

            Assert.NotNull(state);
            Assert.IsAssignableFrom<IListState<int>>(state);
            Assert.Empty(state.Get());
        }

        [Fact]
        public void GetListState_SameDescriptor_ReturnsSameInstance()
        {
            var context = new BasicRuntimeContext();
            var descriptor = new ListStateDescriptor<string>("myListState");

            var state1 = context.GetListState(descriptor);
            var state2 = context.GetListState(descriptor);

            Assert.Same(state1, state2);
        }

        [Fact]
        public void GetListState_DifferentDescriptors_ReturnsDifferentInstances()
        {
            var context = new BasicRuntimeContext();
            var descriptor1 = new ListStateDescriptor<int>("listStateOne");
            var descriptor2 = new ListStateDescriptor<int>("listStateTwo");

            var state1 = context.GetListState(descriptor1);
            var state2 = context.GetListState(descriptor2);

            Assert.NotSame(state1, state2);
        }

        [Fact]
        public void GetListState_DescriptorIsNull_ThrowsArgumentNullException()
        {
            var context = new BasicRuntimeContext();
            Assert.Throws<ArgumentNullException>(() => context.GetListState<int>(null!));
        }

        // --- Tests for GetMapState ---
        [Fact]
        public void GetMapState_ReturnsNonNullMapState()
        {
            var context = new BasicRuntimeContext();
            var descriptor = new MapStateDescriptor<string, int>("testMapState");
            var state = context.GetMapState(descriptor);

            Assert.NotNull(state);
            Assert.IsAssignableFrom<IMapState<string, int>>(state);
            Assert.True(state.IsEmpty());
        }

        [Fact]
        public void GetMapState_SameDescriptor_ReturnsSameInstance()
        {
            var context = new BasicRuntimeContext();
            var descriptor = new MapStateDescriptor<string, double>("myMapState");

            var state1 = context.GetMapState(descriptor);
            var state2 = context.GetMapState(descriptor);

            Assert.Same(state1, state2);
        }

        [Fact]
        public void GetMapState_DifferentDescriptors_ReturnsDifferentInstances()
        {
            var context = new BasicRuntimeContext();
            var descriptor1 = new MapStateDescriptor<int, string>("mapStateOne");
            var descriptor2 = new MapStateDescriptor<int, string>("mapStateTwo");

            var state1 = context.GetMapState(descriptor1);
            var state2 = context.GetMapState(descriptor2);

            Assert.NotSame(state1, state2);
        }

        [Fact]
        public void GetMapState_DescriptorIsNull_ThrowsArgumentNullException()
        {
            var context = new BasicRuntimeContext();
            Assert.Throws<ArgumentNullException>(() => context.GetMapState<string, int>(null!));
        }
    }
}
#nullable disable
