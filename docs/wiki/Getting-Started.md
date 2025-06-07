# Getting Started with Flink.NET

This guide provides a basic walkthrough of how to set up a Flink.NET project and write a simple streaming application.

## Prerequisites

*   .NET SDK (refer to the main README for the recommended version, typically .NET 8 or later).
*   An understanding of basic C# and stream processing concepts.

## 1. Project Setup

1.  **Create a new .NET Console Application:**
    ```bash
    dotnet new console -o MyFlinkApp
    cd MyFlinkApp
    ```

2.  **Add Flink.NET NuGet Packages:**
    You'll need to reference the core Flink.NET libraries. As package names might evolve, refer to the main project's README or `Directory.Packages.props` for the exact names. Conceptually, you would add packages like:
    ```bash
    # Placeholder package names - replace with actual ones
    dotnet add package FlinkDotNet.Core.Api
    dotnet add package FlinkDotNet.Core.Abstractions
    # Add specific connectors if needed, e.g.:
    # dotnet add package FlinkDotNet.Connectors.Sinks.Console
    ```

## 2. Writing a Simple Flink.NET Application

Let's create a simple application that reads a sequence of numbers, converts them to strings with a prefix, and prints them to the console.

```csharp
// MyFlinkApp/Program.cs

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FlinkDotNet.Core.Api;
using FlinkDotNet.Core.Api.Streaming;
using FlinkDotNet.Core.Abstractions.Sources;
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Operators;
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Serializers;
// Assuming ConsoleSinkFunction is in a namespace like this:
using FlinkDotNet.Connectors.Sinks.Console;

// Simple Source Function
public class NumberSource : ISourceFunction<long>
{
    private volatile bool _isRunning = true;
    private readonly long _count;

    public NumberSource(long count = 10)
    {
        _count = count;
    }

    public void Run(ISourceContext<long> ctx)
    {
        for (long i = 0; i < _count && _isRunning; i++)
        {
            ctx.Collect(i);
            Thread.Sleep(100); // Simulate some delay
        }
    }

    public void Cancel()
    {
        _isRunning = false;
    }
}

// Simple Map Operator
public class PrefixMapOperator : IMapOperator<long, string>
{
    public string Map(long value)
    {
        return $"Number: {value}";
    }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Starting Flink.NET Getting Started Example...");

        // 1. Set up the StreamExecutionEnvironment
        var env = StreamExecutionEnvironment.GetExecutionEnvironment();

        // Register serializers if needed (e.g., for custom types or if defaults are not sufficient)
        // For basic types like long and string, Flink.NET provides built-in serializers.
        // env.SerializerRegistry.RegisterSerializer(typeof(long), typeof(LongSerializer));
        // env.SerializerRegistry.RegisterSerializer(typeof(string), typeof(StringSerializer));

        // 2. Create a DataStream from a source
        var source = new NumberSource(15);
        DataStream<long> numberStream = env.AddSource(source, "number-source");

        // 3. Apply transformations
        var mapOperator = new PrefixMapOperator();
        DataStream<string> stringStream = numberStream.Map(mapOperator, "prefix-mapper");

        // 4. Add a sink to print results to the console
        var consoleSink = new ConsoleSinkFunction<string>();
        stringStream.AddSink(consoleSink, "console-sink");

        // 5. Build the JobGraph
        // The JobGraph is a serializable representation of your job's dataflow.
        var jobGraph = env.CreateJobGraph("MySimpleFlinkJob");
        Console.WriteLine($"JobGraph '{jobGraph.JobName}' created with {jobGraph.Vertices.Count} vertices.");

        // 6. Execute the job
        // In a real deployment, this JobGraph would be submitted to a Flink.NET JobManager.
        // The FlinkJobSimulator project shows an example of gRPC submission:
        // FlinkDotNetAspire/FlinkJobSimulator/Program.cs

        // The StreamExecutionEnvironment.ExecuteAsync() method is currently a placeholder
        // for potential local/embedded execution.
        Console.WriteLine("Job execution would typically involve submitting the JobGraph to a JobManager.");
        Console.WriteLine("For a local test without a full cluster, you might use a local executor (if available) or simulate parts of it.");
        // await env.ExecuteAsync("MySimpleFlinkJob"); // This is currently a placeholder

        Console.WriteLine("To run this job in a distributed manner, you would package this application and submit its JobGraph to a Flink.NET JobManager.");
        Console.WriteLine("Please see the FlinkJobSimulator project for an example of how a JobGraph is built and submitted via gRPC.");
        await Task.CompletedTask; // Keep console open for async Main
    }
}

```

## 3. Understanding the Code

*   **`StreamExecutionEnvironment`**: The entry point for creating Flink.NET jobs. It's used to register sources, define transformations, and build the `JobGraph`.
*   **`ISourceFunction`**: Defines how data is ingested into the stream. Our `NumberSource` emits a sequence of numbers.
*   **`IMapOperator`**: Defines a transformation that takes one element and produces one element. `PrefixMapOperator` converts numbers to formatted strings.
*   **`ISinkFunction`**: Defines where the data goes after processing. `ConsoleSinkFunction` (assumed to be provided by a connector library) prints records to the console.
*   **`DataStream<T>`**: Represents a stream of elements of type `T`. You apply operations like `Map`, `Filter`, `KeyBy`, `AddSink`, etc., to these streams.
*   **`JobGraph`**: A graph representation of the dataflow, including sources, operators, sinks, and their connections. This is what gets executed by the Flink.NET runtime.

## 4. Running the Application (Conceptual)

Currently, Flink.NET's primary execution model involves submitting the `JobGraph` to a separate **JobManager** process, which then coordinates execution with **TaskManagers**.

1.  **Build the JobGraph**: The example code shows how `env.CreateJobGraph()` generates this.
2.  **Submit to JobManager**:
    *   You would typically have a client utility or use gRPC directly to serialize the `JobGraph` (e.g., to Protobuf) and send it to the JobManager's submission endpoint.
    *   The `FlinkDotNetAspire/FlinkJobSimulator/Program.cs` project in the Flink.NET repository provides a working example of this gRPC submission process.
3.  **Local Execution (Future/Conceptual)**:
    *   The `StreamExecutionEnvironment.ExecuteAsync()` method is a placeholder for a potential future local execution mode that could run a job within the same process, perhaps with an embedded JobManager/TaskManager for simple tests. This is not fully implemented for general use yet.

## Next Steps

*   Explore the `FlinkDotNetAspire` solution to see how a JobManager, TaskManager, and a job-submitting client (`FlinkJobSimulator`) are run together.
*   Dive deeper into the [Core Concepts](./Core-Concepts-Overview.md) of Flink.NET.
*   Learn more about [Developing Operators](./Developing-Operators.md) and [Working with State](./Developing-State.md).

---
Next: [Business Requirements](./Business-Requirements.md)
