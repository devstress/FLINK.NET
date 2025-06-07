# Core Processing Features

## Operator Chaining

FlinkDotNet implements operator chaining to optimize job performance by fusing chainable operators into a single task. This reduces thread-to-thread handovers, serialization/deserialization overhead, and network communication for co-located operators. Chaining behavior can be controlled globally and at the individual operator level.

For more details, see [Operator Chaining](./Operator-Chaining.md).

## Credit-Based Flow Control

To ensure stable data processing and prevent fast producers from overwhelming slower consumers, FlinkDotNet employs an explicit, Flink-inspired credit-based flow control mechanism for its network communication. This system regulates data transfer based on the availability of buffers on the receiver side, propagating backpressure efficiently.

For more details, see [Credit-Based Flow Control](./Credit-Based-Flow-Control.md).

---
Previous: [Core Concepts: Serialization Strategy](./Core-Concepts-Serialization-Strategy.md)
Next: [Operator Chaining](./Operator-Chaining.md)
