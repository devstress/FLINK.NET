# Working with State in Flink.NET

Flink.NET allows operators to maintain state, enabling complex stream processing applications such as event-driven applications, stateful analytics, and more. This guide focuses on **Keyed State**, which is the most common type of state used in Flink.NET, particularly for parallel processing.

## Keyed State

Keyed State is partitioned or sharded by a key that is extracted from the input data. After a `DataStream` is keyed using `KeyBy()`, all subsequent stateful operations on that stream will be scoped to the current key. This means each parallel instance of an operator will manage state for a distinct subset of keys.

To work with state, you typically use "Rich" versions of operator interfaces (e.g., `IRichMapOperator`, `IRichFilterOperator`), as these provide access to the `IRuntimeContext`, which is the gateway to state management.

See `[Using IRuntimeContext](./Developing-RuntimeContext.md)` for how to access the context.

## Available State Primitives

Flink.NET provides several types of keyed state, available through `FlinkDotNet.Core.Abstractions.States`:

*   **`IValueState<T>`**: Holds a single value of type `T`.
*   **`IListState<T>`**: Holds a list of elements of type `T`.
*   **`IMapState<TK, TV>`**: Holds a map (dictionary) of key-value pairs, where keys are of type `TK` and values are of type `TV`.

## Using State in Rich Functions

### 1. Define a State Descriptor

Before you can access state, you need to describe it using a `StateDescriptor`. The descriptor defines:
*   A unique **name** for the state (within the operator).
*   The **type(s)** of data the state will hold.
*   The **serializer(s)** for those types. Flink.NET needs this to persist and restore state during checkpointing.
*   Optionally, a default value (for `IValueState`).

State descriptors are typically created once, often when the operator is initialized.

**Example Descriptors:**

```csharp
using FlinkDotNet.Core.Abstractions.Models.State;
using FlinkDotNet.Core.Abstractions.Serializers; // For BasicSerializers

// For IValueState<long>
var countStateDescriptor = new ValueStateDescriptor<long>(
    name: "myCounter",
    serializer: new LongSerializer(), // Or your custom serializer for long
    defaultValue: 0L
);

// For IListState<string>
var eventListStateDescriptor = new ListStateDescriptor<string>(
    name: "userEvents",
    elementSerializer: new StringSerializer() // Serializer for elements in the list
);

// For IMapState<string, MyPoco>
// Assume MyPoco is a MemoryPack-annotated POCO
// and MemoryPackSerializer<MyPoco> is available or registered.
var userProfileStateDescriptor = new MapStateDescriptor<string, MyPoco>(
    name: "userProfiles",
    keySerializer: new StringSerializer(),
    valueSerializer: new MemoryPackSerializer<MyPoco>() // Or your custom ITypeSerializer<MyPoco>
);
```
Refer to `[Defining Data Types](./Developing-Data-Types.md)` for more on serializers.

### 2. Retrieve State in `Open()`

In the `Open(IRuntimeContext context)` method of your Rich UDF, use the `IRuntimeContext` and your `StateDescriptor` to get a handle to the state object:

```csharp
using FlinkDotNet.Core.Abstractions.Operators;
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.States;
using FlinkDotNet.Core.Abstractions.Models.State;
using FlinkDotNet.Core.Abstractions.Serializers; // For BasicSerializers

public class MyStatefulOperator : IRichMapOperator<string, string> // Example
{
    private IValueState<long> _visitCountState;
    private IListState<string> _recentItemsState;

    public void Open(IRuntimeContext context)
    {
        var countDescriptor = new ValueStateDescriptor<long>("visitCounter", new LongSerializer(), 0L);
        _visitCountState = context.GetValueState(countDescriptor);

        var listDescriptor = new ListStateDescriptor<string>("recentItems", new StringSerializer());
        _recentItemsState = context.GetListState(listDescriptor);
    }

    public string Map(string inputEvent)
    {
        // ... use state ...
        long currentCount = _visitCountState.Value();
        currentCount++;
        _visitCountState.Update(currentCount);

        _recentItemsState.Add(inputEvent.Substring(0, Math.Min(inputEvent.Length, 10)));
        // Keep only last 5 items (example)
        var items = new List<string>(_recentItemsState.Get());
        if (items.Count > 5)
        {
            _recentItemsState.Update(items.Skip(items.Count - 5));
        }

        return $"Event: {inputEvent}, Count for key: {currentCount}, Recent: {string.Join(",", _recentItemsState.Get())}";
    }

    public void Close() { /* Optional cleanup */ }
}
```

### 3. Using State Primitives

#### `IValueState<T>`

*   **`T Value()`**: Retrieves the current value. Returns the default value from the descriptor if nothing was set for the current key.
*   **`void Update(T value)`**: Sets or updates the value for the current key. If `value` is `null` (for reference types or `Nullable<T>`), Flink's typical behavior is to clear the state (equivalent to `Clear()`).
*   **`void Clear()`**: Removes the value for the current key, resetting it to the default.

#### `IListState<T>`

*   **`IEnumerable<T> Get()`**: Returns all elements currently in the list for the current key. Returns an empty enumerable if the list is empty or not set.
*   **`void Add(T value)`**: Adds a single value to the list for the current key.
*   **`void AddAll(IEnumerable<T> values)`**: Adds all given values to the list.
*   **`void Update(IEnumerable<T> values)`**: Replaces all current elements in the list with the given values for the current key. If `values` is null or empty, this effectively clears the list.
*   **`void Clear()`**: Deletes all elements from the list for the current key.

#### `IMapState<TK, TV>`

*   **`TV Get(TK key)`**: Retrieves the value associated with the given map key (within the current stream key's state). Returns `default(TV)` if the map key is not present.
*   **`void Put(TK key, TV value)`**: Associates the given value with the given map key.
*   **`void PutAll(IDictionary<TK, TV> map)`**: Copies all mappings from the specified dictionary to this map state.
*   **`bool Contains(TK key)`**: Checks if the map state contains a mapping for the specified map key.
*   **`void Remove(TK key)`**: Removes the mapping for the specified map key.
*   **`IEnumerable<TK> Keys()`**: Retrieves an enumerable of all map keys in the state.
*   **`IEnumerable<TV> Values()`**: Retrieves an enumerable of all map values in the state.
*   **`IEnumerable<KeyValuePair<TK, TV>> Entries()`**: Retrieves an enumerable of all key-value pairs (entries) in the map state.
*   **`bool IsEmpty()`**: Checks if the map state contains no key-value mappings.
*   **`void Clear()`**: Removes all mappings from the map state.

## State Serialization

As seen in the `StateDescriptor` examples, you must provide serializers for your state types. This is critical because Flink.NET needs to convert your state objects into bytes for checkpointing and potential persistence.
*   Use appropriate built-in serializers (e.g., `StringSerializer`, `LongSerializer`) for basic types.
*   For POCOs, ensure they are compatible with `MemoryPackSerializer` or provide a custom `ITypeSerializer`.
*   Refer to `[Defining Data Types](./Developing-Data-Types.md)` and `[Core Concepts: Serialization Overview](./Core-Concepts-Serialization.md)` for details.

## State Fault Tolerance (Checkpointing)

The state you manage in Flink.NET operators is made fault-tolerant through **checkpointing**. Periodically, Flink.NET will take a snapshot of your operator's state and store it in a configured durable location (e.g., a file system). If a failure occurs, the job can be restarted, and operators can restore their state from the last successful checkpoint, ensuring data consistency (often exactly-once semantics, depending on sources and sinks).

See `[Core Concepts: Checkpointing Overview](./Core-Concepts-Checkpointing-Overview.md)` for more information.

## State Time-To-Live (TTL) - Planned

Flink.NET plans to support Time-To-Live (TTL) for state. This feature will allow you to configure state entries to expire and be automatically cleaned up after a certain duration of inactivity or since their creation/last update. This helps manage state size and is useful for session-like data. (Details TBD)

---

Working with state is a powerful feature of Flink.NET. By understanding these primitives and how to use them within your Rich UDFs, you can build sophisticated, fault-tolerant streaming applications.
