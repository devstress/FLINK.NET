# Core Concepts: Serialization in Flink.NET

Serialization is a cornerstone of Flink.NET, enabling data to be transferred between distributed tasks, persisted for state checkpointing, and exchanged with external systems via sources and sinks. This guide provides an overview of how serialization works in Flink.NET and how to ensure your data types are handled efficiently.

## `ITypeSerializer<T>`: The Core Abstraction

At the heart of Flink.NET's serialization is the `FlinkDotNet.Core.Abstractions.Serializers.ITypeSerializer<T>` interface. Any data type `T` that flows through a Flink.NET job or is used in state must have a corresponding `ITypeSerializer<T>` implementation. This interface defines methods for:

*   `Serialize(T record)`: Converts an object of type `T` into a `byte[]`.
*   `Deserialize(byte[] serializedRecord)`: Converts a `byte[]` back into an object of type `T`.
*   *(Other methods like `CreateInstance`, `Copy`, `GetLength` might exist or be added in the future for more advanced operations and optimizations.)*

## `SerializerRegistry`: Discovering Serializers

Flink.NET uses a `FlinkDotNet.Core.Abstractions.Execution.SerializerRegistry` to manage and provide serializers for different data types. When Flink.NET needs to serialize or deserialize an object of type `T`, it queries the `SerializerRegistry` for an appropriate `ITypeSerializer<T>`.

The registry uses the following precedence:

1.  **Explicitly Registered Custom Serializers:** If a custom `ITypeSerializer<T>` has been registered for a specific type `T`, that serializer will always be used.
2.  **Built-in Basic Type Serializers:** Flink.NET provides optimized, built-in serializers for common .NET primitive types (`string`, `int`, `long`, `bool`, `double`, `byte[]`, etc.) and some basic collections of these. These are typically pre-registered.
3.  **Default POCO Serializer (`MemoryPack`):** For Plain Old C# Objects (POCOs) that are not covered by the above, Flink.NET now defaults to using a high-performance binary serializer based on **`MemoryPack`**.
4.  **Strict Fallback:** If no serializer can be found or determined for a type through the above mechanisms, Flink.NET will throw a `SerializationException` at job graph compilation or submission time.

For a detailed discussion of the strategy and rationale behind choosing `MemoryPack`, please see [Serialization Strategy](./Core-Concepts-Serialization-Strategy.md).

## Using `MemoryPack` for POCOs (Recommended Default)

To ensure your POCOs are handled by the high-performance `MemoryPack` default serializer, you need to make them compatible with `MemoryPack`'s source generation requirements:

1.  **Add `MemoryPack` NuGet Package:** Ensure your project containing the POCOs references the `MemoryPack` NuGet package (Flink.NET's core libraries that use it will have the reference, but your POCO project might also need it if you're pre-compiling or for attribute access).
2.  **Annotate Your POCOs:**
    *   Mark your class/struct with `[MemoryPackable]`.
    *   Make the class `partial` to allow the source generator to extend it.
    *   Ensure there's a constructor MemoryPack can use (e.g., a parameterless one, or one marked with `[MemoryPackConstructor]`).
    *   Mark serializable members (fields or properties) with `[MemoryPackOrder(int)]`, where the integer is a unique, sequential tag for that member.

**Example:**

```csharp
// Reference the MemoryPack NuGet package in your project
//PM> Install-Package MemoryPack

using MemoryPack;

[MemoryPackable]
public partial class MyCustomData
{
    [MemoryPackOrder(0)]
    public int Id { get; set; }

    [MemoryPackOrder(1)]
    public string? Name { get; set; }

    [MemoryPackOrder(2)]
    public List<double>? Values { get; set; }

    // A parameterless constructor is often simplest for MemoryPack,
    // or use [MemoryPackConstructor] on a specific constructor.
    public MyCustomData() {}
}
```

By following these conventions, `MemoryPackSerializer<MyCustomData>` will be automatically used by Flink.NET for this type, providing excellent performance.

## Registering Custom Serializers

If `MemoryPack` is not suitable for your type, or if your type is not a POCO (e.g., from an external library without `MemoryPack` annotations), or if you have a more optimized domain-specific serializer, you can register a custom `ITypeSerializer<T>` implementation.

This is done via the `SerializerRegistry` instance, usually accessible from the `StreamExecutionEnvironment`:

```csharp
var env = StreamExecutionEnvironment.GetExecutionEnvironment();

// Option 1: Registering a serializer type (must have parameterless constructor)
env.SerializerRegistry.RegisterSerializer<MySpecialType, MySpecialTypeSerializer>();

// Option 2: Registering a serializer instance (useful if constructor needs arguments)
// ITypeSerializer<AnotherType> myInstanceSerializer = new AnotherTypeSerializer("config_value");
// env.SerializerRegistry.RegisterSerializer(typeof(AnotherType), myInstanceSerializer); // (Assuming an overload for instance registration exists or is added)
```
*(Note: The `SerializerRegistry` API for instance registration might vary; check its current methods. The primary method shown in current code is `RegisterSerializer(Type dataType, Type serializerType)` and a generic `RegisterSerializer<TData, TSerializer>()`)*

Refer to the `ITypeSerializer<T>` interface and `SerializerRegistry` class for details on implementing and registering custom serializers.

## Fallback and Troubleshooting

If Flink.NET cannot find a serializer for your type (it's not a basic type, not `MemoryPack`-compatible, and no custom serializer is registered), it will throw a `SerializationException`. The exception message will typically guide you to:
*   Annotate your POCO for `MemoryPack` compatibility.
*   Or, register a custom `ITypeSerializer<T>`.

## Legacy `JsonPocoSerializer`

The `JsonPocoSerializer<T>` (which uses `System.Text.Json`) is still available within Flink.NET. However, it is **no longer the default fallback for arbitrary POCOs** due to performance considerations. If you specifically need JSON serialization for a type, you must explicitly register `JsonPocoSerializer<YourType>` for it:

```csharp
// Explicitly register JsonPocoSerializer for a specific type if JSON is needed
env.SerializerRegistry.RegisterSerializer(typeof(MyTypeForJson), typeof(JsonPocoSerializer<MyTypeForJson>));
```

This ensures that JSON serialization, with its performance implications, is an explicit choice by the developer for types on the data path.

---
Previous: [Core Concepts: Checkpointing - Barriers](./Core-Concepts-Checkpointing-Barriers.md)
[Home](https://github.com/devstress/FLINK.NET/blob/main/docs/wiki/Wiki-Structure-Outline.md)
Next: [Core Concepts: Serialization Strategy](./Core-Concepts-Serialization-Strategy.md)
