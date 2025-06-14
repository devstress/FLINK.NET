# Operator Chaining in FlinkDotNet

## Introduction

Operator chaining is a significant performance optimization in FlinkDotNet, inspired by similar mechanisms in FlinkDotnet. It involves fusing multiple data stream operators together into a single execution task. When operators are chained, records are passed between them via direct method calls within the same thread, bypassing the usual serialization/deserialization steps and network communication (or even inter-thread queueing) that would occur if they were separate tasks.

## Benefits

Enabling operator chaining offers several advantages:

*   **Reduced Overhead:** Eliminates the overhead of record serialization/deserialization between chained operators.
*   **Lower Latency:** Direct method calls are much faster than passing data through network stacks or even IPC mechanisms.
*   **Better Resource Utilization:** Fewer tasks and threads can lead to more efficient use of CPU and memory resources.

## Controlling Chaining

FlinkDotNet provides both global and operator-specific controls for chaining.

### Global Configuration

By default, operator chaining is **enabled** in `FlinkDotNet`. The default strategy for most operators is to chain whenever possible (`ChainingStrategy.ALWAYS`).

You can control global chaining behavior via the `StreamExecutionEnvironment`:

*   **Disabling Chaining Globally:**
    ```csharp
    StreamExecutionEnvironment env = StreamExecutionEnvironment.GetExecutionEnvironment();
    env.DisableOperatorChaining(); // Disables chaining for all operators in the job
    ```
    When disabled, all operators will run as separate tasks.

*   **Setting a Default Chaining Strategy:**
    While individual operators often default to `ChainingStrategy.ALWAYS`, you can influence the environment's default choice (though typically operator-level settings or API calls like `StartNewChain()` are more common for fine-tuning).
    ```csharp
    // Example: Setting a different default (less common to change this globally)
    // env.SetDefaultChainingStrategy(ChainingStrategy.HEAD);
    ```

### Operator-Level Control

You can fine-tune chaining for specific operators using methods on the `DataStream` API. These methods modify the `ChainingStrategy` of the underlying transformation.

The `FlinkDotNet.Core.Abstractions.Operators.ChainingStrategy` enum defines the preferences:

*   `ALWAYS`: The operator will chain to its predecessor and allow successors to chain to it, if all other conditions (like `ShuffleMode.FORWARD`, same parallelism) are met. This is the most common default for operators.
*   `NEVER`: The operator will not be chained to its predecessor, nor will its successors chain to it. This effectively breaks a chain at this operator.
*   `HEAD`: The operator must be the head of a new chain. It will not chain to its predecessor. Successors can chain to it if their strategy is `ALWAYS`.

**API Methods on `DataStream`:**

*   **`StartNewChain()`**: Forces the current operator to be the head of a new chain. It will not be chained with its predecessor operator.
    ```csharp
    DataStream<Output> result = inputStream
        .Map(new MyMapFunction1()) // Might chain with inputStream's operator
        .StartNewChain()          // MyMapFunction1 will be head of its chain (or standalone if no successors chain)
        .Map(new MyMapFunction2()); // MyMapFunction2 can chain to MyMapFunction1
    ```

*   **`DisableChaining()`**: Prevents the current operator from being chained with its predecessor or successor.
    ```csharp
    DataStream<Output> result = inputStream
        .Map(new MyMapFunction1())
        .DisableChaining()        // MyMapFunction1 will be standalone
        .Map(new MyMapFunction2()); // MyMapFunction2 will be standalone (or head of a new chain)
    ```

## How it Works (Conceptual)

During job compilation, the `StreamExecutionEnvironment` analyzes the logical plan (the sequence of transformations). When generating the `JobGraph` (the physical execution plan):

1.  It checks the global `IsChainingEnabled()` flag.
2.  For each potential link between two operators, it considers:
    *   The `ShuffleMode` of the connection (must be `FORWARD`).
    *   The parallelism of the two operators (must be the same).
    *   The `ChainingStrategy` of both the upstream and downstream operators.
        *   If either is `NEVER`, they won't chain.
        *   If the downstream is `HEAD`, they won't chain (downstream starts a new chain).
3.  If all conditions are met, the operators are fused into a single `JobVertex` in the `JobGraph`. The first operator in the sequence becomes the "head" of the `JobVertex`, and subsequent fused operators are "chained" within it.
4.  At runtime, data records between these chained operators are passed as Java/C# object references via a component called `ChainedCollector`, avoiding serialization and network/IPC overhead.

This optimization is applied automatically where possible, but the API methods provide control for specific scenarios where default behavior might not be desired.
```

---
Previous: [Core Processing Features](./Core-Processing-Features.md)
[Home](https://github.com/devstress/FLINK.NET/blob/main/docs/wiki/Wiki-Structure-Outline.md)
Next: [Credit-Based Flow Control](./Credit-Based-Flow-Control.md)
