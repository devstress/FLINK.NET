using System.Collections.Generic;
using System.Linq;
using Xunit;
using FlinkDotNet.Core.Abstractions.States;
using FlinkDotNet.Core.Abstractions.Serializers; // Added for serializers

namespace FlinkDotNet.JobManager.Tests.Core.Abstractions.States
{
    public class InMemoryMapStateTests
    {
        private readonly StringSerializer _stringSerializer = new StringSerializer();
        private readonly IntSerializer _intSerializer = new IntSerializer();

        [Fact]
        public void NewState_IsEmpty_ReturnsTrue()
        {
            var state = new InMemoryMapState<string, int>(_stringSerializer, _intSerializer);
            Assert.True(state.IsEmpty());
            Assert.Empty(state.Keys());
            Assert.Empty(state.Values());
            Assert.Empty(state.Entries());
        }

        [Fact]
        public void Put_SingleEntry_GetReturnsValue_ContainsReturnsTrue_IsNotEmpty()
        {
            var state = new InMemoryMapState<string, string>(_stringSerializer, _stringSerializer);
            state.Put("key1", "value1");

            Assert.Equal("value1", state.Get("key1"));
            Assert.True(state.Contains("key1"));
            Assert.False(state.IsEmpty());
        }

        [Fact]
        public void Get_NonExistentKey_ReturnsDefaultValue()
        {
            var state = new InMemoryMapState<string, int>(_stringSerializer, _intSerializer); // int default is 0
            Assert.Equal(0, state.Get("nonexistent"));

            var stateStr = new InMemoryMapState<int, string?>(_intSerializer, _stringSerializer); // string? default is null
            Assert.Null(stateStr.Get(123));
        }

        [Fact]
        public void Put_UpdateExistingKey_GetReturnsNewValue()
        {
            var state = new InMemoryMapState<string, int>(_stringSerializer, _intSerializer);
            state.Put("key1", 10);
            state.Put("key1", 20); // Update
            Assert.Equal(20, state.Get("key1"));
        }

        [Fact]
        public void PutAll_AddsMultipleEntries_OverwritesExisting()
        {
            var state = new InMemoryMapState<string, int>(_stringSerializer, _intSerializer);
            state.Put("key1", 1); // Existing, will be overwritten
            state.Put("key2", 2); // Existing, will remain

            var newEntries = new Dictionary<string, int>
            {
                { "key1", 100 }, // Overwrite
                { "key3", 300 }  // New
            };
            state.PutAll(newEntries);

            Assert.Equal(100, state.Get("key1"));
            Assert.Equal(2, state.Get("key2"));
            Assert.Equal(300, state.Get("key3"));
            Assert.Equal(3, state.Entries().Count());
        }

        [Fact]
        public void PutAll_NullDictionary_DoesNotThrow_StateUnchanged()
        {
            var state = new InMemoryMapState<string, int>(_stringSerializer, _intSerializer);
            state.Put("key1", 1);
            state.PutAll(null!);
            Assert.Equal(1, state.Get("key1"));
            Assert.Single(state.Entries());
        }


        [Fact]
        public void Contains_NonExistentKey_ReturnsFalse()
        {
            var state = new InMemoryMapState<string, int>(_stringSerializer, _intSerializer);
            Assert.False(state.Contains("key1"));
        }

        [Fact]
        public void Remove_ExistingKey_RemovesEntry_GetReturnsDefault_ContainsReturnsFalse()
        {
            var state = new InMemoryMapState<string, int>(_stringSerializer, _intSerializer);
            state.Put("key1", 10);
            state.Remove("key1");

            Assert.Equal(0, state.Get("key1")); // int default
            Assert.False(state.Contains("key1"));
            Assert.True(state.IsEmpty());
        }

        [Fact]
        public void Remove_NonExistentKey_DoesNothing()
        {
            var state = new InMemoryMapState<string, int>(_stringSerializer, _intSerializer);
            state.Put("key1", 10);
            state.Remove("key2"); // Non-existent
            Assert.Equal(1, state.Entries().Count());
            Assert.True(state.Contains("key1"));
        }

        [Fact]
        public void Keys_ReturnsAllKeys()
        {
            var state = new InMemoryMapState<string, int>(_stringSerializer, _intSerializer);
            state.Put("a", 1);
            state.Put("b", 2);
            state.Put("c", 3);
            Assert.Equal(new[] { "a", "b", "c" }.OrderBy(k => k), state.Keys().OrderBy(k => k));
        }

        [Fact]
        public void Values_ReturnsAllValues()
        {
            var state = new InMemoryMapState<string, int>(_stringSerializer, _intSerializer);
            state.Put("a", 10);
            state.Put("b", 20);
            state.Put("c", 10); // Duplicate value
            Assert.Equal(new[] { 10, 20, 10 }.OrderBy(v => v), state.Values().OrderBy(v => v));
        }

        [Fact]
        public void Entries_ReturnsAllKeyValuePairs()
        {
            var state = new InMemoryMapState<string, int>(_stringSerializer, _intSerializer);
            state.Put("x", 100);
            state.Put("y", 200);
            var entries = state.Entries().ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

            Assert.Equal(2, entries.Count);
            Assert.Equal(100, entries["x"]);
            Assert.Equal(200, entries["y"]);
        }

        [Fact]
        public void Entries_ModifyingReturnedEnumerable_DoesNotAffectInternalState()
        {
            var state = new InMemoryMapState<string, string>(_stringSerializer, _stringSerializer);
            state.Put("key1", "value1");

            var entries = state.Entries();
            // Attempt to modify a materialized copy of the enumerable
            var entriesList = entries.ToList();
            // entriesList.Add(new KeyValuePair<string, string>("key2", "value2")); // This would modify only the copy if allowed

            Assert.Single(state.Entries()); // Original state should be unchanged
            Assert.Equal("value1", state.Get("key1"));
        }


        [Fact]
        public void Clear_RemovesAllEntries_IsEmptyReturnsTrue()
        {
            var state = new InMemoryMapState<string, int>(_stringSerializer, _intSerializer);
            state.Put("one", 1);
            state.Put("two", 2);
            state.Clear();

            Assert.True(state.IsEmpty());
            Assert.Empty(state.Keys());
        }
    }
}
#nullable disable
