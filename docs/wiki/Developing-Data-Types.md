# Defining Data Types in Flink.NET

When developing Flink.NET applications, the data types you define for your records (events, messages) are crucial. Flink.NET needs to be able to serialize and deserialize these types efficiently for network transfer, state management, and communication with connectors.

## POCOs (Plain Old C# Objects)

The most common way to define your data types is by using Plain Old C# Objects (POCOs).

### Requirements for POCOs

For Flink.NET to handle your POCOs effectively, especially with the default high-performance `MemoryPack` serializer, you should follow these conventions:

1.  **Public Class:** The class should be public.
2.  **`[MemoryPackable]` Attribute:** Annotate your class with the `[MemoryPackable]` attribute from the `MemoryPack` library.
3.  **`partial` Keyword:** Make your class `partial`. This allows the `MemoryPack` source generator to extend your class with optimized serialization logic.
4.  **Suitable Constructor:**
    *   Provide a public parameterless constructor.
    *   Alternatively, if you have a constructor with parameters that should be used by `MemoryPack` for deserialization, annotate it with `[MemoryPackConstructor]`.
5.  **`[MemoryPackOrder(int)]` Attribute for Members:**
    *   All fields or properties that you want to be serialized must be public.
    *   Annotate each serializable member with `[MemoryPackOrder(int)]`, providing a unique, sequential integer starting from 0. This order is critical for schema evolution.

### Example POCO

```csharp
// Ensure your project references the MemoryPack NuGet package.
// Flink.NET's core libraries will depend on it, but your POCO project might also need a direct reference
// for attribute access or if you precompile serializers.
// Example: dotnet add package MemoryPack

using MemoryPack;
using System.Collections.Generic;

[MemoryPackable]
public partial class UserEvent
{
    [MemoryPackOrder(0)]
    public long UserId { get; set; }

    [MemoryPackOrder(1)]
    public string? EventType { get; set; } // Nullable string

    [MemoryPackOrder(2)]
    public DateTime Timestamp { get; set; }

    [MemoryPackOrder(3)]
    public Dictionary<string, string>? Properties { get; set; } // Example of a complex type

    // Parameterless constructor for MemoryPack (or use [MemoryPackConstructor] on another)
    public UserEvent() {}

    // Example of a constructor that could be used by your application logic
    public UserEvent(long userId, string eventType, DateTime timestamp, Dictionary<string, string>? properties = null)
    {
        UserId = userId;
        EventType = eventType;
        Timestamp = timestamp;
        Properties = properties;
    }
}
```

By following these guidelines, Flink.NET will use `MemoryPackSerializer<YourPocoType>` by default, providing efficient binary serialization.

## Basic Types

Flink.NET has built-in, optimized serializers for common .NET primitive types and some basic structures:
*   `string`
*   `int`, `long`, `short`, `byte`
*   `float`, `double`
*   `bool`
*   `char`
*   `DateTime`, `TimeSpan` (Support may vary based on specific serializer, check `BasicSerializers.cs`)
*   `byte[]`
*   Simple arrays and lists of these primitive types (e.g., `List<string>`, `int[]`) are also typically handled efficiently.

You generally do not need to do anything special to use these types.

## Custom Serialization

There might be cases where the default POCO serialization is not suitable or possible:
*   You are using types from external libraries that cannot be annotated with `MemoryPack` attributes.
*   You need highly specialized or optimized serialization logic for a particular type.
*   You want to integrate a different serialization framework (e.g., Apache Avro, Google Protocol Buffers) for specific types.

In such scenarios, you can implement a custom serializer.

### Implementing `ITypeSerializer<T>`

Create a class that implements the `FlinkDotNet.Core.Abstractions.Serializers.ITypeSerializer<T>` interface:

```csharp
using FlinkDotNet.Core.Abstractions.Serializers;
using System.Text.Json; // Example using System.Text.Json

public class MyCustomType
{
    public string Name { get; set; }
    public int Value { get; set; }
}

public class MyCustomTypeSerializer : ITypeSerializer<MyCustomType>
{
    public byte[] Serialize(MyCustomType obj)
    {
        // Example: Using System.Text.Json for custom serialization
        return JsonSerializer.SerializeToUtf8Bytes(obj);
    }

    public MyCustomType Deserialize(byte[] bytes)
    {
        // Example: Using System.Text.Json for custom deserialization
        return JsonSerializer.Deserialize<MyCustomType>(bytes)!;
    }
}
```

### Registering Custom Serializers

Register your custom serializer with the `StreamExecutionEnvironment` before defining your job:

```csharp
var env = StreamExecutionEnvironment.GetExecutionEnvironment();

// Option 1: Registering a serializer type (must have parameterless constructor)
env.SerializerRegistry.RegisterSerializer<MyCustomType, MyCustomTypeSerializer>();

// Option 2: Registering a serializer instance (if constructor needs arguments)
// ITypeSerializer<AnotherType> myInstanceSerializer = new AnotherTypeSerializer("config_value");
// env.SerializerRegistry.RegisterSerializer(typeof(AnotherType), myInstanceSerializer);
// (Note: Check SerializerRegistry for the exact method signature for instance registration)
```

## Serialization Fallback and Troubleshooting

If Flink.NET cannot determine a serializer for a type (it's not a basic type, not `MemoryPack`-compatible, and no custom serializer is registered), it will throw a `SerializationException` during job graph compilation or submission. The exception message will usually guide you on how to resolve this, typically by:
*   Making your POCO compatible with `MemoryPack`.
*   Registering a custom `ITypeSerializer<T>`.

For more details on Flink.NET's serialization strategy and `MemoryPack`, refer to:
*   [Core Concepts: Serialization Overview](./Core-Concepts-Serialization.md)
*   [Core Concepts: Serialization Strategy](./Core-Concepts-Serialization-Strategy.md)

Choosing appropriate data types and ensuring they are serialized efficiently is key to building robust and performant Flink.NET applications.
