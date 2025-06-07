# Working with Operators (User-Defined Functions) in Flink.NET

Operators are the fundamental building blocks for transforming data in Flink.NET. You implement User-Defined Functions (UDFs) that define the core logic of these operators. Flink.NET provides various interfaces for common data transformations.

## Overview

Most operators in Flink.NET follow a pattern where you implement a specific interface corresponding to the desired operation (e.g., `IMapOperator` for map, `IFilterOperator` for filter). These interfaces typically define a single method that processes individual data elements.

For operators that require more advanced functionality, such as accessing runtime information, managing state, or performing setup/teardown tasks, Flink.NET provides "Rich" versions of these interfaces (e.g., `IRichMapOperator`). These rich interfaces inherit from their basic counterparts and also from `IOperatorLifecycle`.

## Core Operator Interfaces

Here are some of the primary operator interfaces available in `FlinkDotNet.Core.Abstractions.Operators`:

*   **`IMapOperator<TIn, TOut>`**:
    *   Transforms one input element into one output element.
    *   Defines `TOut Map(TIn element);`

*   **`IFilterOperator<T>`**:
    *   Decides whether to keep or discard an element.
    *   Defines `bool Filter(T element);` (returns `true` to keep the element).

*   **`IFlatMapOperator<TIn, TOut>`**:
    *   Transforms one input element into zero, one, or more output elements.
    *   Defines `void FlatMap(TIn element, ICollector<TOut> collector);`
    *   Uses an `ICollector<TOut>` to emit output elements.

*   **`IReduceOperator<T>`**:
    *   Combines two elements of the same type into a single element of that type. Typically used on keyed streams.
    *   Defines `T Reduce(T currentAccumulator, T newValue);`

*   **`IAggregateOperator<TIn, TAgg, TOut>`**:
    *   A more general aggregation interface than `IReduceOperator`.
    *   Involves creating an accumulator (`TAgg CreateAccumulator()`), adding elements to it (`TAgg Add(TAgg accumulator, TIn value)`), merging accumulators (`TAgg Merge(TAgg a, TAgg b)`), and deriving a final result (`TOut GetResult(TAgg accumulator)`).

*   **`IJoinFunction<TLeft, TRight, TOut>`**:
    *   Combines two elements from different input streams (left and right) into a single output element.
    *   Defines `TOut Join(TLeft left, TRight right);`

*   **`IWindowOperator<TIn, TOut>`**:
    *   This is currently a placeholder interface for window-specific operations. Detailed windowing APIs and functions (like `IProcessWindowFunction`) are planned for the future. See `[Windowing API (Future)](./Developing-Windowing-Api.md)`.

## Rich Functions and `IOperatorLifecycle`

For operators that need initialization, cleanup, or access to runtime information (like state or task details), you should implement the "Rich" version of the operator interface. These interfaces extend the basic operator interface and also implement `IOperatorLifecycle`.

**`IOperatorLifecycle`** defines two methods:

*   `void Open(IRuntimeContext context)`: Called once before any data processing methods (like `Map` or `Filter`). Use this for setup, such as initializing connections or retrieving state.
*   `void Close()`: Called once after all records have been processed or when the operator is shutting down. Use this for cleanup tasks.

The `IRuntimeContext` passed to `Open` provides access to:
*   Job and task information (name, parallelism, subtask index).
*   `JobConfiguration`.
*   Methods to access keyed state (e.g., `GetValueState`, `GetListState`). See `[Using IRuntimeContext](./Developing-RuntimeContext.md)` and `[Working with State](./Developing-State.md)`.

**Example Rich Operator Interfaces:**
*   `IRichMapOperator<TIn, TOut>`
*   `IRichFilterOperator<T>`
*   `IRichFlatMapOperator<TIn, TOut>`
*   And so on for other operator types.

## Examples

### Simple Map Operator

```csharp
using FlinkDotNet.Core.Abstractions.Operators;

public class MySimpleMapper : IMapOperator<int, string>
{
    public string Map(int value)
    {
        return $"Value: {value * 2}";
    }
}

// Usage in DataStream API:
// DataStream<int> inputStream = ... ;
// DataStream<string> outputStream = inputStream.Map(new MySimpleMapper(), "my-mapper");
```

### Rich Filter Operator

```csharp
using FlinkDotNet.Core.Abstractions.Operators;
using FlinkDotNet.Core.Abstractions.Context;
using System; // For Console.WriteLine

public class MyRichFilter : IRichFilterOperator<string>
{
    private string _taskName;
    private int _elementsProcessed = 0;

    public void Open(IRuntimeContext context)
    {
        _taskName = context.TaskName;
        Console.WriteLine($"[{_taskName}] MyRichFilter opened.");
        // Example: Initialize state or a connection here
    }

    public bool Filter(string value)
    {
        _elementsProcessed++;
        if (_elementsProcessed % 100 == 0)
        {
            Console.WriteLine($"[{_taskName}] Processed {_elementsProcessed} elements.");
        }
        return value.Contains("Flink");
    }

    public void Close()
    {
        Console.WriteLine($"[{_taskName}] MyRichFilter closed. Processed a total of {_elementsProcessed} elements.");
    }
}

// Usage in DataStream API:
// DataStream<string> textStream = ... ;
// DataStream<string> filteredStream = textStream.Filter(new MyRichFilter(), "my-rich-filter");
```

## Implementing UDFs

When implementing your UDFs:
*   Ensure your UDF class has a public parameterless constructor if Flink.NET needs to instantiate it by type.
*   Make your UDFs serializable if they contain state that needs to be checkpointed by Flink.NET itself (though state is typically managed via Flink.NET's state abstractions).
*   Focus on the logic within the core processing methods (`Map`, `Filter`, etc.).
*   Use Rich functions when you need to interact with the runtime environment or manage state.

This page provides a starting point for understanding and implementing operators in Flink.NET. Refer to specific interface definitions and further examples in the repository as you build your applications.

---
Previous: [Developing Data Types](./Developing-Data-Types.md)
[Home](https://github.com/devstress/FLINK.NET/blob/main/docs/wiki/Wiki-Structure-Outline.md)
Next: [Developing RuntimeContext](./Developing-RuntimeContext.md)
