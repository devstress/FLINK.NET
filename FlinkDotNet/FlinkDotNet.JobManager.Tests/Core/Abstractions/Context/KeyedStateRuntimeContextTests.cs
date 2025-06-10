using FlinkDotNet.Core.Abstractions.Models; // For JobConfiguration
using FlinkDotNet.Core.Abstractions.Models.State;
using FlinkDotNet.Core.Abstractions.Runtime;
using FlinkDotNet.Core.Abstractions.Serializers; // For ITypeSerializer and basic serializers

namespace FlinkDotNet.JobManager.Tests.Core.Abstractions.Context
{
    public class KeyedStateRuntimeContextTests
    {
        private readonly ITypeSerializer<string> _stringSerializer = new StringSerializer();
        private readonly ITypeSerializer<int> _intSerializer = new IntSerializer();
        private readonly ITypeSerializer<long> _longSerializer = new LongSerializer();

        private BasicRuntimeContext CreateContext() => new BasicRuntimeContext(
            jobName: "TestJob",
            taskName: "TestTask",
            numberOfParallelSubtasks: 1,
            indexOfThisSubtask: 0,
            jobConfiguration: new JobConfiguration());

        [Fact]
        public void GetValueState_RequiresKeySet()
        {
            var context = CreateContext();
            var descriptor = new ValueStateDescriptor<string>("testState", _stringSerializer);
            Assert.Throws<InvalidOperationException>(() => context.GetValueState(descriptor));
        }

        [Fact]
        public void GetValueState_ScopedPerKey()
        {
            var context = CreateContext();
            var descriptor = new ValueStateDescriptor<string>("testState", _stringSerializer, "default");

            context.SetCurrentKey("key1");
            var state1 = context.GetValueState(descriptor);
            state1.Update("value1");

            context.SetCurrentKey("key2");
            var state2 = context.GetValueState(descriptor);
            Assert.Equal("default", state2.Value()); // Should be default for new key
            state2.Update("value2");

            Assert.Equal("value2", state2.Value());

            context.SetCurrentKey("key1");
            var state1Again = context.GetValueState(descriptor);
            Assert.Equal("value1", state1Again.Value()); // Should retain value for key1
        }

        [Fact]
        public void GetValueState_ReturnsSameInstanceForKeyAndDescriptor()
        {
            var context = CreateContext();
            var descriptor = new ValueStateDescriptor<string>("testState", _stringSerializer);
            context.SetCurrentKey("key1");
            var state1 = context.GetValueState(descriptor);
            var state2 = context.GetValueState(descriptor);
            Assert.Same(state1, state2);
        }

        [Fact]
        public void GetListState_RequiresKeySet()
        {
            var context = CreateContext();
            var descriptor = new ListStateDescriptor<string>("testListState", _stringSerializer);
            Assert.Throws<InvalidOperationException>(() => context.GetListState(descriptor));
        }

        [Fact]
        public void GetListState_ScopedPerKey()
        {
            var context = CreateContext();
            var descriptor = new ListStateDescriptor<string>("testListState", _stringSerializer);

            context.SetCurrentKey("keyA");
            var listStateA = context.GetListState(descriptor);
            listStateA.Add("itemA1");

            context.SetCurrentKey("keyB");
            var listStateB = context.GetListState(descriptor);
            Assert.Empty(listStateB.Get());
            listStateB.Add("itemB1");
            listStateB.Add("itemB2");

            context.SetCurrentKey("keyA");
            var listStateAAgain = context.GetListState(descriptor);
            Assert.Collection(listStateAAgain.Get(), item => Assert.Equal("itemA1", item));

            context.SetCurrentKey("keyB");
            var listStateBAgain = context.GetListState(descriptor);
            Assert.Collection(listStateBAgain.Get(),
                item => Assert.Equal("itemB1", item),
                item => Assert.Equal("itemB2", item));
        }

        [Fact]
        public void GetMapState_RequiresKeySet()
        {
            var context = CreateContext();
            var descriptor = new MapStateDescriptor<long, string>("testMapState", _longSerializer, _stringSerializer);
            Assert.Throws<InvalidOperationException>(() => context.GetMapState(descriptor));
        }

        [Fact]
        public void GetMapState_ScopedPerKey()
        {
            var context = CreateContext();
            var descriptor = new MapStateDescriptor<long, string>("testMapState", _longSerializer, _stringSerializer);

            context.SetCurrentKey("map_key_1");
            var mapState1 = context.GetMapState(descriptor);
            mapState1.Put(1L, "value1_1");

            context.SetCurrentKey("map_key_2");
            var mapState2 = context.GetMapState(descriptor);
            Assert.False(mapState2.Contains(1L));
            mapState2.Put(2L, "value2_1");

            context.SetCurrentKey("map_key_1");
            var mapState1Again = context.GetMapState(descriptor);
            Assert.True(mapState1Again.Contains(1L));
            Assert.Equal("value1_1", mapState1Again.Get(1L));
            Assert.False(mapState1Again.Contains(2L));
        }

        [Fact]
        public void GetState_ThrowsIfDifferentStateTypeRequestedForSameNameAndKey()
        {
            var context = CreateContext();
            var valueDesc = new ValueStateDescriptor<string>("multiTypeState", _stringSerializer);
            var listDesc = new ListStateDescriptor<string>("multiTypeState", _stringSerializer);

            context.SetCurrentKey("someKey");
            context.GetValueState(valueDesc); // Initialize as ValueState

            // Attempting to get it as ListState for the same key should fail
            Assert.Throws<InvalidOperationException>(() => context.GetListState(listDesc));
        }
    }
}
#nullable disable
