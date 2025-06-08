using Xunit;
using FlinkDotNet.Core.Abstractions.States;
using FlinkDotNet.Core.Abstractions.Serializers; // Added for serializers

namespace FlinkDotNet.JobManager.Tests.Core.Abstractions.States // Adjusted namespace for clarity
{
    public class InMemoryValueStateTests
    {
        private readonly IntSerializer _intSerializer = new IntSerializer();
        private readonly StringSerializer _stringSerializer = new StringSerializer();
        private readonly JsonPocoSerializer<object?> _objectSerializer = new JsonPocoSerializer<object?>();
        private readonly JsonPocoSerializer<MyStruct> _myStructSerializer = new JsonPocoSerializer<MyStruct>();

        [Fact]
        public void Value_Initial_ReturnsInitialValue() // Renamed to reflect new constructor
        {
            var state = new InMemoryValueState<int>(10, _intSerializer);
            Assert.Equal(10, state.Value());

            var stateStr = new InMemoryValueState<string?>("default", _stringSerializer);
            Assert.Equal("default", stateStr.Value());

            var stateNullableStr = new InMemoryValueState<string?>(null, _stringSerializer);
            Assert.Null(stateNullableStr.Value());
        }

        [Fact]
        public void Value_AfterUpdate_ReturnsUpdatedValue()
        {
            var state = new InMemoryValueState<int>(0, _intSerializer);
            state.Update(42);
            Assert.Equal(42, state.Value());

            var stateStr = new InMemoryValueState<string?>("initial", _stringSerializer);
            stateStr.Update("updated");
            Assert.Equal("updated", stateStr.Value());

            stateStr.Update(null); // Update to null
            Assert.Null(stateStr.Value());
        }

        [Fact]
        public void Clear_ResetsToDefaultOfType() // Name reflects behavior change (no longer descriptor's default)
        {
            var state = new InMemoryValueState<int>(5, _intSerializer); // Initial value is 5
            state.Update(100); // Set a value
            Assert.Equal(100, state.Value()); // Verify its set

            state.Clear();
            Assert.Equal(default(int), state.Value()); // Should return default(int), which is 0

            // Test with reference type
            var obj = new object();
            var stateObj = new InMemoryValueState<object?>(obj, _objectSerializer); // Initial value is obj
            stateObj.Update(obj); // Not strictly necessary if constructor sets it
            Assert.Same(obj, stateObj.Value());

            stateObj.Clear();
            Assert.Null(stateObj.Value()); // Should return default(object?), which is null
        }

        [Fact]
        public void Update_WithDefaultValue_IsConsideredSet() // Test name might be less relevant, but behavior is key
        {
            var state = new InMemoryValueState<int>(10, _intSerializer); // Initial value is 10
            state.Update(0); // Update with the default for int
            Assert.Equal(0, state.Value());
        }

        [Fact]
        public void Value_Initial_StructType_ReturnsInitialStructValue() // Renamed
        {
            var state = new InMemoryValueState<MyStruct>(new MyStruct { X = 1 }, _myStructSerializer);
            Assert.Equal(1, state.Value().X);

            var stateNoDefault = new InMemoryValueState<MyStruct>(default(MyStruct), _myStructSerializer); // Initial value is default(MyStruct)
             Assert.Equal(0, stateNoDefault.Value().X); // Default for int is 0
        }

        private struct MyStruct { public int X { get; set; } }
    }
}
#nullable disable
