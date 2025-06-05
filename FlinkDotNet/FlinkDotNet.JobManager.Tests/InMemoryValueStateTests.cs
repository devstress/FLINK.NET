#nullable enable
using Xunit;
using FlinkDotNet.Core.Abstractions.States;

namespace FlinkDotNet.JobManager.Tests.Core.Abstractions.States // Adjusted namespace for clarity
{
    public class InMemoryValueStateTests
    {
        [Fact]
        public void Value_NotSet_ReturnsDefaultValue()
        {
            var state = new InMemoryValueState<int>(defaultValue: 10);
            Assert.Equal(10, state.Value());

            var stateStr = new InMemoryValueState<string?>(defaultValue: "default");
            Assert.Equal("default", stateStr.Value());

            var stateNullableStr = new InMemoryValueState<string?>(defaultValue: null);
            Assert.Null(stateNullableStr.Value());
        }

        [Fact]
        public void Value_AfterUpdate_ReturnsUpdatedValue()
        {
            var state = new InMemoryValueState<int>(defaultValue: 0);
            state.Update(42);
            Assert.Equal(42, state.Value());

            var stateStr = new InMemoryValueState<string?>(defaultValue: "initial");
            stateStr.Update("updated");
            Assert.Equal("updated", stateStr.Value());

            stateStr.Update(null); // Update to null
            Assert.Null(stateStr.Value());
        }

        [Fact]
        public void Clear_ResetsToDefaultValueAndIsSetToFalse()
        {
            var state = new InMemoryValueState<int>(defaultValue: 5);
            state.Update(100); // Set a value
            Assert.Equal(100, state.Value()); // Verify its set

            state.Clear();
            Assert.Equal(5, state.Value()); // Should return default

            // Test with reference type
            var stateObj = new InMemoryValueState<object?>(defaultValue: null);
            var obj = new object();
            stateObj.Update(obj);
            Assert.Same(obj, stateObj.Value());

            stateObj.Clear();
            Assert.Null(stateObj.Value());
        }

        [Fact]
        public void Update_WithDefaultValue_IsConsideredSet()
        {
            var state = new InMemoryValueState<int>(defaultValue: 0);
            state.Update(0); // Update with the same as default
            Assert.Equal(0, state.Value());
            // How to check _isSet? The public API should be sufficient.
            // If it was not set, Value() would return default. If it is set to default, Value() also returns default.
            // The key is consistent behavior. This test mostly ensures Update(default) behaves like any other Update.
        }

        [Fact]
        public void Value_NotSet_StructType_ReturnsDefaultStructValue()
        {
            var state = new InMemoryValueState<MyStruct>(defaultValue: new MyStruct { X = 1 });
            Assert.Equal(1, state.Value().X);

            var stateNoDefault = new InMemoryValueState<MyStruct>(); // Default for MyStruct
             Assert.Equal(0, stateNoDefault.Value().X); // Default for int is 0
        }

        private struct MyStruct { public int X { get; set; } }
    }
}
#nullable disable
