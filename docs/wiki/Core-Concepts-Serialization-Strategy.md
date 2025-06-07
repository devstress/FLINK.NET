# Core Concepts: Serialization Strategy in Flink.NET

Efficient serialization is critical for the performance of a distributed stream processing engine like Flink.NET. It impacts network data exchange, state storage and retrieval, and overall throughput and latency. This document outlines Flink.NET's strategy for serialization, focusing on Plain Old C# Objects (POCOs).

## Importance of Serialization

In Flink.NET, data records are frequently:
*   Serialized and sent over the network between TaskManagers.
*   Serialized for checkpointing state to durable storage.
*   Deserialized from state during recovery or for stateful operations.
*   Serialized/deserialized by source and sink connectors.

Inefficient serialization can lead to high CPU usage, excessive memory allocations (triggering garbage collection), and network bottlenecks.

## Goals for Flink.NET Serialization

*   **High Performance:** Maximize throughput and minimize latency. This implies fast serialization/deserialization routines and low memory allocation.
*   **Ease of Use for POCOs:** Users should be able to use their C# POCOs with minimal friction and good default performance.
*   **Schema Evolution:** Support for evolving data types over time, especially for application state, is crucial for long-running applications.
*   **Robustness:** Serializers should correctly handle a wide range of .NET types, including generics.
*   **Flexibility:** Allow users to provide custom serializers for specific types when needed.

## Evaluated Serialization Candidates

Several high-performance .NET serialization libraries and strategies were considered:

1.  **`System.Text.Json` (STJ) with Source Generation:** Built-in, good performance for JSON, but text-based.
2.  **`MemoryPack`:** Modern, very high-performance binary serializer using source generation, with schema evolution support.
3.  **`MessagePack-CSharp`:** Mature, very high-performance MessagePack implementation, typically attribute-based.
4.  **Apache Avro:** Robust schema evolution and cross-language support, binary. Schema-first paradigm.
5.  **Google Protocol Buffers:** Efficient binary format, schema-first (.proto files), excellent for interop.

Key evaluation criteria included raw performance (speed and allocation), ease of POCO integration, schema evolution capabilities, binary format preference for internal use, and maturity.

## Recommended Default POCO Serialization Strategy: `MemoryPack`

After analysis, **`MemoryPack` is recommended as the primary default serializer for POCOs in Flink.NET** where no other specific serializer (e.g., for primitives or user-registered custom serializer) takes precedence.

### Rationale for Choosing `MemoryPack`

*   **Exceptional Performance:** `MemoryPack` is designed for extreme speed and minimal memory allocations, often outperforming other .NET binary serializers. This is vital for Flink.NET.
*   **Source Generation:** It uses C# source generators to create efficient serialization logic at compile time, avoiding runtime reflection overhead for POCOs.
*   **Strong POCO Support:** Works well with C# POCOs, guided by a few attributes (e.g., `[MemoryPackable]`, `[MemoryPackOrder]`).
*   **Schema Evolution:** Provides built-in support for schema evolution through member ordering/tagging (`[MemoryPackOrder(int)]`) and versioning, which is critical for stateful applications.
*   **Binary Format:** Produces a compact binary representation suitable for network transfer and state persistence.
*   **Modern and Actively Developed:** Aligns with modern .NET development practices.

### User Guidance for `MemoryPack`

To have their POCOs serialized by the default `MemoryPack` path, users will typically need to:
1.  Annotate their POCO class with `[MemoryPackable]`.
2.  Ensure there's a constructor that MemoryPack can use (e.g., a parameterless constructor, or one marked with `[MemoryPackConstructor]`).
3.  Annotate serializable members with `[MemoryPackOrder(int)]` (which also acts as the field tag for versioning).
    ```csharp
    // Example
    [MemoryPackable]
    public partial class MyPoco // partial keyword is often needed for source generators
    {
        [MemoryPackOrder(0)]
        public int Id { get; set; }

        [MemoryPackOrder(1)]
        public string? Value { get; set; }

        // MemoryPack will choose a suitable constructor or one can be marked
    }
    ```
Flink.NET documentation will provide detailed guidance and examples.

## Fallback Strategy

If a type is encountered for which no specific serializer is registered and it's not compatible with `MemoryPack` (e.g., not annotated correctly):
*   **Flink.NET will throw a `SerializationException` at job graph compilation or submission time.**
*   The exception will guide the user to either:
    1.  Make the type compatible with `MemoryPack` (e.g., add necessary attributes).
    2.  Register a custom `ITypeSerializer<T>` for that type.
*   **Rationale:** This strict approach avoids silent performance degradation by falling back to a slower serializer (like JSON) for types intended for high-performance paths. It encourages explicit and efficient serialization choices.

## Custom Serializers

Flink.NET will maintain robust support for users to register their own `ITypeSerializer<T>` implementations for any type via the `SerializerRegistry`. This is essential for:
*   Types that cannot be handled by `MemoryPack`.
*   User-defined types with highly specialized serialization needs.
*   Integrating other serialization formats (e.g., Avro, Protobuf for specific types if the user chooses).

## Built-in Basic Type Serializers

Flink.NET will continue to provide highly optimized, built-in serializers for common .NET primitive types (int, long, string, bool, double, etc.), byte arrays, and potentially common simple collections of these primitives. These will typically take precedence over the default POCO serializer. These internal serializers may themselves leverage efficient techniques, potentially even `MemoryPack`'s low-level capabilities for these specific types.

## Conclusion

Adopting `MemoryPack` as the default high-performance POCO serializer, combined with a strict fallback strategy and strong support for custom serializers, aims to provide Flink.NET users with both excellent out-of-the-box performance for their data types and the flexibility to handle any serialization requirement.

---
Previous: [Core Concepts: Serialization](./Core-Concepts-Serialization.md)
[Home](https://github.com/devstress/FLINK.NET/blob/main/docs/wiki/Wiki-Structure-Outline.md)
Next: [Core Processing Features](./Core-Processing-Features.md)
