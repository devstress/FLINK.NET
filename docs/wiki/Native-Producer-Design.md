# High-Throughput .NET Kafka Producer Using Native librdkafka

This document outlines the design of the experimental native Kafka producer included in Flink.NET.

## Objective

Provide a .NET producer capable of sending over one million messages per second by calling the C++ `librdkafka` library directly. The goal is to match or exceed the throughput of the official Java producer while keeping the .NET layer lightweight.

## Components

- **C# Producer Layer**
  - Prepares message batches in pinned memory.
  - Invokes native methods via P/Invoke (`NativeProducer`).
  - Handles high-level retry logic and throughput metrics.

- **Native C++ Bridge (`nativekafkabridge`)**
  - Wraps `librdkafka` and exposes a minimal C interface.
  - Uses `rd_kafka_produce_batch()` to send thousands of messages per call.
  - Manages producer configuration (idempotence, acks, buffers).

- **Kafka Broker**
  - Tuned with many partitions and large socket buffers to sustain high throughput.

## Data Flow

1. The .NET layer builds an array of messages and pins it in memory.
2. A pointer to this batch is passed to the C++ bridge.
3. The bridge constructs `rd_kafka_message_t` structures and calls `rd_kafka_produce_batch()`.
4. The broker receives the batch and acknowledges according to producer settings.

## Design Priorities

- **Large batches** to minimize syscalls and overhead.
- **Pinned buffers** to avoid GC pressure and copies.
- **Parallel producers** across partitions for maximum throughput.
- **Cross-platform** by compiling the bridge for Windows and Linux.

## Expected Results

With proper tuning (100 partitions, 128 parallel producers, idempotence enabled) the native producer can exceed **1M messages/second** on modern hardware.

See [Advanced Performance Tuning](./Advanced-Performance-Tuning.md) for benchmark results and configuration details.
