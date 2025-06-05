# Core Concepts: State Management Overview

### Table of Contents
- [What is State?](#what-is-state)
- [Keyed State](#keyed-state)
  - [Types of Keyed State in Flink.NET](#types-of-keyed-state-in-flinknet)
  - [Accessing and Describing State](#accessing-and-describing-state)
- [State Backends](#state-backends)

Stateful stream processing is a cornerstone of Flink.NET, allowing applications to maintain information across events for complex computations.

*(Apache Flink Ref: [Working with State](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/))*

## What is State?
In stream processing, "state" is any data an operator remembers from past events to process current ones.

## Keyed State
Flink.NET primarily focuses on **Keyed State**, partitioned by a key extracted from input data. All state for a given key is managed by the same operator instance.

*(Apache Flink Ref: [Keyed State](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/#keyed-state))*

### Types of Keyed State in Flink.NET
Core state interfaces (`FlinkDotNet.Core.Abstractions.States`):
*   **`IValueState<T>`:** Holds a single value.
*   **`IListState<T>`:** Holds a list of elements.
*   **`IMapState<TK, TV>`:** Holds key-value pairs.

*(Apache Flink Ref: [State Primitives](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/#available-state-primitives))*

### Accessing and Describing State
State is accessed in "Rich" operators via `IRuntimeContext` using a `StateDescriptor` (e.g., `ValueStateDescriptor<T>`). The descriptor defines the state''s name, type, and (later) serializers and default values.

```csharp
// Conceptual example in a Rich operator:
public class MyRichMap : IRichMapOperator<string, string> {
    private IValueState<long> _myCounter;
    public void Open(IRuntimeContext context) {
        var descriptor = new ValueStateDescriptor<long>("myCounterState", 0L);
        _myCounter = context.GetValueState(descriptor);
    }
    // ... Map method ...
}
```

### State Backends
A **State Backend** determines how state is stored. Flink.NET plans to support:
1.  **In-Memory State Backend:** Live state in TaskManager memory, snapshots to durable object store. Fast, but memory-limited.
2.  **Durable Keyed State Backend (Planned):** Live state in an embedded store (like RocksDB) or fast distributed store (like Redis), with snapshots to durable object store. Supports large state.

*(Apache Flink Ref: [State Backends](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/))*

Proper state management is essential for advanced, fault-tolerant streaming applications.
