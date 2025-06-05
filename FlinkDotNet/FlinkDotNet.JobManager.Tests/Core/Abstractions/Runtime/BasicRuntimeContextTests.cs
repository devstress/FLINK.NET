#nullable enable
using Xunit;
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Models;
using FlinkDotNet.Core.Abstractions.Models.State;
using FlinkDotNet.Core.Abstractions.Runtime;
using FlinkDotNet.Core.Abstractions.Serializers; // Added for serializers
using FlinkDotNet.Core.Abstractions.States;
using System; // For NotImplementedException, ArgumentNullException
using System.Collections.Generic; // For List in tests
using System.Linq; // For Linq extensions on IEnumerable in tests


namespace FlinkDotNet.JobManager.Tests.Core.Abstractions.Runtime
{
    public class BasicRuntimeContextTests
    {
        private readonly StringSerializer _stringSerializer = new StringSerializer();
        private readonly IntSerializer _intSerializer = new IntSerializer();
        private readonly PocoSerializer<double> _doubleSerializer = new PocoSerializer<double>();
        private const string DummyKey = "dummy_key_for_non_keyed_tests";

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
            context.SetCurrentKey(DummyKey); // Key must be set
            var descriptor = new ValueStateDescriptor<int>("testState", _intSerializer, defaultValue: 5);
            var state = context.GetValueState(descriptor);

            Assert.NotNull(state);
            Assert.IsAssignableFrom<IValueState<int>>(state);
            Assert.Equal(5, state.Value());
        }

        [Fact]
        public void GetValueState_SameDescriptor_ReturnsSameInstance()
        {
            var context = new BasicRuntimeContext();
            context.SetCurrentKey(DummyKey); // Key must be set
            var descriptor = new ValueStateDescriptor<string>("myValueState", _stringSerializer);

            var state1 = context.GetValueState(descriptor);
            var state2 = context.GetValueState(descriptor);

            Assert.Same(state1, state2);
        }

        [Fact]
        public void GetValueState_DifferentDescriptors_ReturnsDifferentInstances()
        {
            var context = new BasicRuntimeContext();
            context.SetCurrentKey(DummyKey); // Key must be set
            var descriptor1 = new ValueStateDescriptor<int>("valueStateOne", _intSerializer);
            var descriptor2 = new ValueStateDescriptor<int>("valueStateTwo", _intSerializer);

            var state1 = context.GetValueState(descriptor1);
            var state2 = context.GetValueState(descriptor2);

            Assert.NotSame(state1, state2);
        }

        [Fact]
        public void GetValueState_DescriptorIsNull_ThrowsArgumentNullException()
        {
            var context = new BasicRuntimeContext();
            context.SetCurrentKey(DummyKey); // Key must be set, though it will throw before using it
            Assert.Throws<ArgumentNullException>(() => context.GetValueState<int>(null!));
        }

        // --- Tests for GetListState ---
        [Fact]
        public void GetListState_ReturnsNonNullListState()
        {
            var context = new BasicRuntimeContext();
            context.SetCurrentKey(DummyKey); // Key must be set
            var descriptor = new ListStateDescriptor<int>("testListState", _intSerializer);
            var state = context.GetListState(descriptor);

            Assert.NotNull(state);
            Assert.IsAssignableFrom<IListState<int>>(state);
            Assert.Empty(state.Get());
        }

        [Fact]
        public void GetListState_SameDescriptor_ReturnsSameInstance()
        {
            var context = new BasicRuntimeContext();
            context.SetCurrentKey(DummyKey); // Key must be set
            var descriptor = new ListStateDescriptor<string>("myListState", _stringSerializer);

            var state1 = context.GetListState(descriptor);
            var state2 = context.GetListState(descriptor);

            Assert.Same(state1, state2);
        }

        [Fact]
        public void GetListState_DifferentDescriptors_ReturnsDifferentInstances()
        {
            var context = new BasicRuntimeContext();
            context.SetCurrentKey(DummyKey); // Key must be set
            var descriptor1 = new ListStateDescriptor<int>("listStateOne", _intSerializer);
            var descriptor2 = new ListStateDescriptor<int>("listStateTwo", _intSerializer);

            var state1 = context.GetListState(descriptor1);
            var state2 = context.GetListState(descriptor2);

            Assert.NotSame(state1, state2);
        }

        [Fact]
        public void GetListState_DescriptorIsNull_ThrowsArgumentNullException()
        {
            var context = new BasicRuntimeContext();
            context.SetCurrentKey(DummyKey); // Key must be set
            Assert.Throws<ArgumentNullException>(() => context.GetListState<int>(null!));
        }

        // --- Tests for GetMapState ---
        [Fact]
        public void GetMapState_ReturnsNonNullMapState()
        {
            var context = new BasicRuntimeContext();
            context.SetCurrentKey(DummyKey); // Key must be set
            var descriptor = new MapStateDescriptor<string, int>("testMapState", _stringSerializer, _intSerializer);
            var state = context.GetMapState(descriptor);

            Assert.NotNull(state);
            Assert.IsAssignableFrom<IMapState<string, int>>(state);
            Assert.True(state.IsEmpty());
        }

        [Fact]
        public void GetMapState_SameDescriptor_ReturnsSameInstance()
        {
            var context = new BasicRuntimeContext();
            context.SetCurrentKey(DummyKey); // Key must be set
            var descriptor = new MapStateDescriptor<string, double>("myMapState", _stringSerializer, _doubleSerializer);

            var state1 = context.GetMapState(descriptor);
            var state2 = context.GetMapState(descriptor);

            Assert.Same(state1, state2);
        }

        [Fact]
        public void GetMapState_DifferentDescriptors_ReturnsDifferentInstances()
        {
            var context = new BasicRuntimeContext();
            context.SetCurrentKey(DummyKey); // Key must be set
            var descriptor1 = new MapStateDescriptor<int, string>("mapStateOne", _intSerializer, _stringSerializer);
            var descriptor2 = new MapStateDescriptor<int, string>("mapStateTwo", _intSerializer, _stringSerializer);

            var state1 = context.GetMapState(descriptor1);
            var state2 = context.GetMapState(descriptor2);

            Assert.NotSame(state1, state2);
        }

        [Fact]
        public void GetMapState_DescriptorIsNull_ThrowsArgumentNullException()
        {
            var context = new BasicRuntimeContext();
            context.SetCurrentKey(DummyKey); // Key must be set
            Assert.Throws<ArgumentNullException>(() => context.GetMapState<string, int>(null!));
        }
    }
}
#nullable disable
