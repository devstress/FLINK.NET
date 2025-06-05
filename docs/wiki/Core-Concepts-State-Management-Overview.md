# Core Concepts: State Management Overview

Stateful stream processing is a cornerstone of Flink.NET, allowing applications to maintain information across events for complex computations. "State" is any data an operator remembers from past events to process current or future ones. Examples include counters, sums, windowed event collections, or machine learning models being updated.

*(Apache Flink Ref: [Working with State](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/))*

## Keyed State

Flink.NET primarily focuses on **Keyed State**. This type of state is partitioned and managed in the scope of a specific key, which is extracted from the input data (typically after a `keyBy()` operation in the dataflow). All state operations for a given key are handled by the same parallel operator instance, enabling consistent state access and updates even in a distributed environment.

*(Apache Flink Ref: [Keyed State](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/#keyed-state))*

### Types of Keyed State in Flink.NET

Flink.NET provides several primitive types of keyed state, defined as interfaces in `FlinkDotNet.Core.Abstractions.States`:

*   **`IValueState<T>`:** Holds a single value of type `T`.
    *   `T Value()`: Retrieves the current value.
    *   `void Update(T value)`: Sets or overwrites the value.
    *   `void Clear()`: Removes the value.
*   **`IListState<T>`:** Holds a list of elements of type `T`.
    *   `IEnumerable<T> Get()`: Retrieves all elements.
    *   `void Add(T value)`: Adds an element.
    *   `void AddAll(IEnumerable<T> values)`: Adds multiple elements.
    *   `void Update(IEnumerable<T> values)`: Replaces the entire list.
    *   `void Clear()`: Removes all elements.
*   **`IMapState<TK, TV>`:** Holds a map of key-value pairs (`TK` for user key, `TV` for user value).
    *   `TV Get(TK key)`: Retrieves value for a map key.
    *   `void Put(TK key, TV value)`: Adds/updates a map entry.
    *   `void Remove(TK key)`: Removes an entry.
    *   And other standard map operations like `Contains`, `Keys`, `Values`, `Entries`, `IsEmpty`.

*(Apache Flink Ref: [Available State Primitives](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/#available-state-primitives))*

### Accessing and Describing State

State is accessed within "Rich" operators (e.g., `IRichMapOperator`, `IRichFilterOperator`) that implement `IOperatorLifecycle`. The `Open(IRuntimeContext context)` method provides an `IRuntimeContext`, which is then used to get handles to state objects.

To get a state handle, an operator must first define a **`StateDescriptor`** (e.g., `ValueStateDescriptor<T>`, `ListStateDescriptor<T>`, `MapStateDescriptor<TK,TV>`). The descriptor contains:
*   A unique **name** for the state (within the operator).
*   The **type** of the state.
*   (Later) Information about **serializers** for the state''s data type.
*   (For `ValueStateDescriptor`) An optional **default value**.

```csharp
// Conceptual example within a Rich operator:
public class MyStatefulCounter : IRichMapOperator<string, string>
{
    private IValueState<long> _countState;

    public void Open(IRuntimeContext context)
    {
        var descriptor = new ValueStateDescriptor<long>("myCounterState", defaultValue: 0L);
        _countState = context.GetValueState(descriptor);
    }

    public string Map(string eventData)
    {
        long currentCount = _countState.Value();
        currentCount++;
        _countState.Update(currentCount);
        return $"Event: {eventData}, Current Count for Key: {currentCount}";
    }

    public void Close() { /* Cleanup state if needed */ }
}
```

### State Backends

A **State Backend** determines how and where state is stored. Flink.NET is designed to support different state backends:

1.  **In-Memory State Backend (Default for local testing):**
    *   *Live State:* Kept on the .NET process memory of the TaskManagers.
    *   *Snapshots for Checkpoints:* Serialized and written to a durable object store (e.g., MinIO, S3, Azure Blob Storage).
    *   *Pros:* Very fast.
    *   *Cons:* State size limited by TaskManager memory.

2.  **Durable Keyed State Backend (e.g., RocksDB-like, or Redis with durable snapshots - Planned for production):**
    *   *Live State:* Managed in an embedded key-value store (like RocksDB on local disk) or a fast distributed store (like Redis). This allows for state much larger than available memory.
    *   *Snapshots for Checkpoints:* Asynchronously snapshotted to a durable object store.
    *   *Pros:* Supports very large state, good performance for many access patterns.
    *   *Cons:* Potentially higher latency than pure in-memory for individual accesses if disk I/O is involved for live state (less so for Redis).

The choice of state backend is a configuration parameter and is crucial for scalability and performance. All state backends in Flink.NET will integrate with the checkpointing mechanism to provide fault tolerance.

*(Apache Flink Ref: [State Backends](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/))*

Proper state management is essential for building advanced, fault-tolerant streaming applications with Flink.NET, enabling features like windowing, complex event processing, and applications that learn and adapt over time.
