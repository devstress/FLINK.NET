# Using IRuntimeContext in Flink.NET Operators

The `IRuntimeContext` interface in Flink.NET (`FlinkDotNet.Core.Abstractions.Context.IRuntimeContext`) provides access to essential information about the operator's execution environment. It is available within "Rich" User-Defined Functions (UDFs) – those that implement an `IRich*Operator` interface (which inherits from `IOperatorLifecycle`).

## Accessing `IRuntimeContext`

You gain access to the `IRuntimeContext` in the `Open(IRuntimeContext context)` method of your Rich UDF. This method is part of the `IOperatorLifecycle` interface. It's good practice to store the context in a private field if you need to access it in other methods of your operator (like `Map`, `Filter`, or `Close`).

```csharp
using FlinkDotNet.Core.Abstractions.Operators;
using FlinkDotNet.Core.Abstractions.Context;
using System; // For Console.WriteLine

public class MyRichExampleOperator : IRichMapOperator<string, string>
{
    private IRuntimeContext _runtimeContext;
    private int _subtaskIndex;

    public void Open(IRuntimeContext context)
    {
        _runtimeContext = context; // Store the context
        _subtaskIndex = context.IndexOfThisSubtask; // Example: Store specific info

        Console.WriteLine($"[{_runtimeContext.TaskName}] Opened. Subtask Index: {_subtaskIndex}");
    }

    public string Map(string value)
    {
        // You can now use _runtimeContext or _subtaskIndex here
        return $"Task: {_runtimeContext.TaskName}, Value: {value.ToUpper()}";
    }

    public void Close()
    {
        Console.WriteLine($"[{_runtimeContext.TaskName}] Closed.");
    }
}
```

## Information Provided by `IRuntimeContext`

The `IRuntimeContext` interface offers several properties to get information about the current task and job:

*   **`string JobName { get; }`**: Gets the name of the job the operator is part of.
*   **`string TaskName { get; }`**: Gets the name of the task (this specific operator instance, often including subtask index).
*   **`int NumberOfParallelSubtasks { get; }`**: Gets the total number of parallel instances (parallelism) for this operator.
*   **`int IndexOfThisSubtask { get; }`**: Gets the 0-based index of this specific parallel instance of the operator.
*   **`JobConfiguration JobConfiguration { get; }`**: Gets the global job configuration. This can be used to pass job-wide parameters to your operators. (Currently, `JobConfiguration` is a basic placeholder but can be expanded).

## Accessing Keyed State

One of the most important roles of `IRuntimeContext` is to provide access to **keyed state**. This is state that is partitioned by a key, typically after a `KeyBy()` operation on a `DataStream`.

The context provides the following methods for state access:

*   **`IValueState<T> GetValueState<T>(ValueStateDescriptor<T> stateDescriptor)`**: Retrieves a handle to a value state, which stores a single value of type `T`.
*   **`IListState<T> GetListState<T>(ListStateDescriptor<T> stateDescriptor)`**: Retrieves a handle to a list state, which stores a list of elements of type `T`.
*   **`IMapState<TK, TV> GetMapState<TK, TV>(MapStateDescriptor<TK, TV> stateDescriptor)`**: Retrieves a handle to a map state, which stores key-value pairs.

To use these methods, you first define a `StateDescriptor` (e.g., `ValueStateDescriptor`) which specifies the name of the state, its type, and its serializer.

```csharp
// Inside a Rich operator that has access to IRuntimeContext _runtimeContext;

// Example for ValueState:
// Assume _countState is a field: private IValueState<long> _countState;
var countDescriptor = new ValueStateDescriptor<long>(
    "myCounterState",
    new LongSerializer(), // Provide the appropriate serializer
    defaultValue: 0L
);
_countState = _runtimeContext.GetValueState(countDescriptor);

// Now you can use _countState.Value() and _countState.Update().
```

For more detailed information on using state, refer to the `[Working with State](./Developing-State.md)` documentation.

## Managing the Current Key for State

Keyed state is always associated with the "current key" being processed by the operator instance. The Flink.NET runtime (specifically the `TaskExecutor`) is responsible for setting this current key before invoking your operator's processing method for an element from a keyed stream.

`IRuntimeContext` provides methods related to this key management, though they are primarily for internal use by the framework or for advanced custom operator implementations:

*   **`object? GetCurrentKey()`**: Gets the current key that the context (and thus any state retrieved from it) is scoped to. Returns `null` if the context is not currently keyed (e.g., in a non-keyed stream operator).
*   **`void SetCurrentKey(object? key)`**: **INTERNAL USE:** This method is called by the Flink.NET runtime (e.g., `TaskExecutor`) to set the current key before processing an element in a keyed stream. Application developers typically do not call this method directly.

Understanding `IRuntimeContext` is essential for writing Rich functions that can leverage Flink.NET's stateful processing capabilities and gain insights into their operational environment.

---
Previous: [Developing Operators](./Developing-Operators.md)
[Home](https://github.com/devstress/FLINK.NET/blob/main/docs/wiki/Wiki-Structure-Outline.md)
Next: [Developing State](./Developing-State.md)
