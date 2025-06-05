#nullable enable
using System.Collections.Generic;
using System.Linq;
using Xunit;
using FlinkDotNet.Core.Abstractions.States;

namespace FlinkDotNet.JobManager.Tests.Core.Abstractions.States
{
    public class InMemoryListStateTests
    {
        [Fact]
        public void Get_NewState_ReturnsEmptyEnumerable()
        {
            var state = new InMemoryListState<int>();
            Assert.Empty(state.Get());
        }

        [Fact]
        public void Add_SingleItem_GetReturnsItem()
        {
            var state = new InMemoryListState<string>();
            state.Add("hello");
            Assert.Equal(new[] { "hello" }, state.Get());
        }

        [Fact]
        public void Add_MultipleItems_GetReturnsAllItemsInOrder()
        {
            var state = new InMemoryListState<int>();
            state.Add(1);
            state.Add(2);
            state.Add(3);
            Assert.Equal(new[] { 1, 2, 3 }, state.Get());
        }

        [Fact]
        public void AddAll_NullEnumerable_DoesNotThrowAndListRemainsSame()
        {
            var state = new InMemoryListState<string>();
            state.Add("existing");
            state.AddAll(null!); // Pass null
            Assert.Equal(new[] { "existing" }, state.Get()); // Should not change
        }

        [Fact]
        public void AddAll_EmptyEnumerable_DoesNotChangeList()
        {
            var state = new InMemoryListState<int>();
            state.Add(1);
            state.AddAll(new List<int>()); // Empty list
            Assert.Equal(new[] { 1 }, state.Get());
        }

        [Fact]
        public void AddAll_ItemsToExistingList_AppendsItems()
        {
            var state = new InMemoryListState<string>();
            state.Add("one");
            state.AddAll(new[] { "two", "three" });
            Assert.Equal(new[] { "one", "two", "three" }, state.Get());
        }

        [Fact]
        public void Update_NullEnumerable_ClearsList()
        {
            var state = new InMemoryListState<int>();
            state.Add(1);
            state.Add(2);
            state.Update(null!); // Update with null
            Assert.Empty(state.Get());
        }

        [Fact]
        public void Update_EmptyEnumerable_ClearsList()
        {
            var state = new InMemoryListState<int>();
            state.Add(1);
            state.Add(2);
            state.Update(new List<int>()); // Update with empty
            Assert.Empty(state.Get());
        }

        [Fact]
        public void Update_WithNewItems_ReplacesExistingList()
        {
            var state = new InMemoryListState<string>();
            state.Add("initial1");
            state.Add("initial2");
            state.Update(new[] { "new1", "new2", "new3" });
            Assert.Equal(new[] { "new1", "new2", "new3" }, state.Get());
        }

        [Fact]
        public void Clear_RemovesAllItems()
        {
            var state = new InMemoryListState<int>();
            state.Add(10);
            state.Add(20);
            state.Clear();
            Assert.Empty(state.Get());
        }

        [Fact]
        public void Clear_EmptyList_RemainsEmpty()
        {
            var state = new InMemoryListState<double>();
            state.Clear();
            Assert.Empty(state.Get());
        }

        [Fact]
        public void Get_ReturnsCopyOfInternalList()
        {
            var state = new InMemoryListState<string>();
            state.Add("a");
            var list1 = (List<string>)state.Get(); // Cast to List to modify
            list1.Add("b"); // Modify the returned list

            var list2 = state.Get(); // Get again
            Assert.Equal(new[] { "a" }, list2); // Internal state should be unchanged
        }
    }
}
#nullable disable
