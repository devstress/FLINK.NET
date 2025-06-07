# Getting Started with Flink.NET

This guide will walk you through the process of setting up your development environment and writing a simple Flink.NET application.

## Prerequisites

*   **.NET SDK:** Ensure you have the .NET SDK (version X.X.X or higher) installed. You can download it from [here](https://dotnet.microsoft.com/download).
*   **IDE (Optional but Recommended):** An IDE like Visual Studio, JetBrains Rider, or VS Code can greatly improve your development experience.
*   **Apache Flink (for local execution):** To run Flink.NET applications locally, you'll need a local Apache Flink cluster.
    *   Follow the [Apache Flink Local Installation Guide](https://nightlies.apache.org/flink/flink-docs-stable/docs/try-flink/local_installation/).
    *   Ensure your Flink cluster is running before executing Flink.NET jobs.

## 1. Create a New .NET Project

Start by creating a new .NET console application. You can do this using the .NET CLI or your preferred IDE.

```bash
dotnet new console -n MyFlinkApp
cd MyFlinkApp
```

## 2. Add Flink.NET NuGet Packages

Add the necessary Flink.NET NuGet packages to your project. The core package required to get started is `FlinkDotNet.Core.Abstractions`. Others will be needed for specific operators or connectors.

```bash
dotnet add package FlinkDotNet.Core.Abstractions
# Add other packages as needed, e.g., FlinkDotNet.Core
```

*(Note: Specific package names might change. Refer to the main [Readme.md](../../Readme.md) or the official NuGet feed for the latest package information.)*

## 3. Write Your First Flink.NET Application

Let's create a simple application that reads a list of numbers, filters out the even ones, and prints the odd numbers to the console.

```csharp
// Program.cs
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FlinkDotNet.Core.Abstractions; // Or the relevant 'using' for StreamExecutionEnvironment
using FlinkDotNet.Core.Api; // Or the relevant 'using' for DataStream

public class Program
{
    public static async Task Main(string[] args)
    {
        // 1. Set up the execution environment
        // Note: The exact way to get the environment might vary based on the library version.
        // This is a conceptual example.
        var environment = StreamExecutionEnvironment.GetExecutionEnvironment();

        // 2. Create a data stream from a collection
        var numbers = environment.FromCollection(new List<int> { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });

        // 3. Define a transformation: Filter for odd numbers
        var oddNumbers = numbers.Filter(new OddNumberFilter());

        // 4. Define a sink: Print to console
        oddNumbers.Print(); // Or a more specific sink operation

        // 5. Execute the job
        // The actual execution call might differ.
        // This might involve specifying a job name or other configurations.
        await environment.ExecuteAsync("My First Flink.NET Job");
    }
}

// Define a simple filter operator
public class OddNumberFilter : IFilterFunction<int>
{
    public bool Filter(int value)
    {
        return value % 2 != 0;
    }
}
```

**Explanation:**

*   **`StreamExecutionEnvironment.GetExecutionEnvironment()`**: This initializes the Flink execution environment.
*   **`environment.FromCollection(...)`**: This creates a `DataStream<int>` from an in-memory collection. In real applications, you would use source connectors (e.g., Kafka, File).
*   **`numbers.Filter(new OddNumberFilter())`**: This applies a filter transformation. The `OddNumberFilter` class implements the `IFilterFunction<int>` interface, defining the logic to keep only odd numbers.
*   **`oddNumbers.Print()`**: This is a simple sink that prints each element of the stream to the console.
*   **`environment.ExecuteAsync(...)`**: This triggers the execution of the Flink job.

*(Disclaimer: The exact API calls and class names like `StreamExecutionEnvironment`, `FromCollection`, `Print`, `ExecuteAsync`, and `IFilterFunction` are illustrative. Refer to the specific Flink.NET library version you are using for the correct API usage and necessary `using` statements. The purpose of this example is to show the general structure.)*

## 4. Configure the Flink Runner (Conceptual)

To execute this job, Flink.NET needs to know how to communicate with your Flink cluster (or run in an embedded mode if supported). This configuration might involve:

*   Setting environment variables.
*   A configuration file (e.g., `appsettings.json`).
*   Programmatic configuration.

**Example (Conceptual `appsettings.json`):**

```json
{
  "Flink": {
    "JobManagerRestAddress": "http://localhost:8081"
    // Other relevant configurations
  }
}
```

*(Refer to Flink.NET's specific documentation for details on how to configure the job submission and connection to Flink.)*

## 5. Run Your Application

Once your code is ready and the Flink connection is configured:

1.  **Ensure your Apache Flink cluster is running.** You can typically start it by navigating to your Flink installation's `bin` directory and running `./start-cluster.sh` (on Linux/macOS) or `start-cluster.bat` (on Windows).
2.  **Run your .NET application:**

    ```bash
    dotnet run
    ```

You should see the odd numbers (1, 3, 5, 7, 9) printed in the console output from your Flink job. You can also monitor the job through the Flink Web UI (usually at `http://localhost:8081`).

## Next Steps

*   Explore different [[Operators|Developing-Operators]] available in Flink.NET.
*   Learn about [[Connectors|Connectors-Overview]] to read from and write to external systems.
*   Understand [[State Management|Core-Concepts-State-Management-Overview]] for building stateful applications.

**Apache Flink References:**

*   [Flink DataStream API Programming Guide](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/overview/)
*   [Fundamental Concepts (Flink Architecture)](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink_architecture/)
