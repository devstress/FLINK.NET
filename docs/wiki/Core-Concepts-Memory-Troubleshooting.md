# Core Concepts: Memory Troubleshooting (Flink.NET)

[Back to Main Outline](./Wiki-Structure-Outline.md)

### Table of Contents
- [Common Memory Problems in Flink.NET](#common-memory-problems-in-flinknet)
- [Diagnosing Memory Issues](#diagnosing-memory-issues)
- [Specific Issue Resolution Strategies](#specific-issue-resolution-strategies)

---

Troubleshooting memory issues is a common task when running distributed data processing applications like Flink.NET. This guide provides advice on diagnosing and resolving common memory-related problems in Flink.NET, covering both .NET runtime aspects and interactions with Kubernetes.

Reference: [Memory Tuning (Flink.NET)](./Core-Concepts-Memory-Tuning.md)

## Common Memory Problems in Flink.NET

*   **`System.OutOfMemoryException` (.NET OOM):**
    *   **Symptom:** The Flink.NET JobManager or TaskManager process crashes with a `System.OutOfMemoryException`.
    *   **Cause:** Typically, the .NET runtime heap is exhausted. This can be due to:
        *   Insufficient heap size configured for the workload (e.g., `jobmanager.heap.memory.size` or `taskmanager.heap.memory.size` is too low). This includes memory for objects, large data structures in UDFs, and large state in memory.
        *   Memory leaks in user code or, less commonly, in the Flink.NET framework.
        *   Excessive allocation of large objects leading to LOH fragmentation.
    *   **Distinction:** It's important to distinguish this from a Kubernetes OOMKilled event. A .NET OOM happens *within* the process when the .NET runtime cannot allocate more managed memory, while a K8s OOMKill happens when the entire pod exceeds its cgroup limit set by Kubernetes.
*   **Excessive GC Pressure / Long GC Pauses:**
    *   **Symptom:** Application throughput is low, latencies are high, CPU usage by the Flink.NET process is consistently high (often due to GC work).
    *   **Cause:**
        *   Heap is too small, forcing frequent GCs.
        *   Very high rate of object allocation and deallocation (churn) in UDFs.
        *   Large objects frequently promoted to higher GC generations or LOH, making collection more expensive.
*   **Kubernetes Pod OOMKilled:**
    *   **Symptom:** The JobManager or TaskManager pod is abruptly terminated by Kubernetes. `kubectl describe pod <pod-name>` will show the reason as `OOMKilled`.
    *   **Cause:** The total memory consumed by all processes within the pod (including the Flink.NET process, any sidecars, and system utilities) exceeded the pod's `resources.limits.memory`. This could be due to:
        *   Flink.NET process memory (sum of .NET heap, network buffers, potential off-heap memory, and framework overhead) exceeding its configured share or growing unexpectedly.
        *   Underestimated overheads for the .NET runtime itself, or other components like file system caches used by RocksDB if integrated.
*   **Insufficient Network Buffers:**
    *   **Symptom:** Backpressure in the job graph, tasks stuck or slow, often indicated by Flink.NET metrics (if available) showing full output or input buffers.
    *   **Cause:** Not enough memory allocated for network buffers (e.g., `taskmanager.network.memory.size` - planned) to handle the data shuffle volume between tasks.
*   **Slow Performance due to Memory Configuration:**
    *   **Symptom:** General sluggishness not directly tied to crashes.
    *   **Cause:** Suboptimal partitioning of memory between heap, network, and other components. For example, too much heap and too little network memory, or vice-versa, for the specific workload.

## Diagnosing Memory Issues

A systematic approach is needed:

1.  **Gather Information:**
    *   **Logs:** Collect Flink.NET JobManager and TaskManager logs. Look for `OutOfMemoryException` stack traces, GC-related messages (if .NET GC logging is enabled), or other warnings.
    *   **Metrics:**
        *   **Flink.NET Metrics (Planned):** Check metrics for network buffer usage, configured memory pool sizes, heap usage, GC counts/time, queue lengths, and backpressure indicators.
        *   **.NET Counters:** Use `dotnet-counters` to get live GC statistics, heap size, LOH size, CPU usage, etc., from the running Flink.NET process.
            *   Example: `dotnet-counters monitor -p <process-id> System.Runtime`
        *   **Kubernetes Metrics:** `kubectl top pod <pod-name>` for current CPU/memory. For historical data, use your Kubernetes monitoring stack (e.g., Prometheus, Grafana) to inspect pod memory usage, CPU, and network I/O over time.
    *   **Job Graph & Configuration:** Understand the job structure, parallelism, and current Flink.NET memory configurations (both K8s limits and internal Flink.NET settings).
2.  **Analyze .NET Heap Dumps (for .NET OOMs or Leaks):**
    *   If you suspect a memory leak or need to understand heap composition at the time of an OOM:
        *   Capture a heap dump. This can be done using `dotnet-dump` or by configuring the process to dump on OOM (platform-dependent, e.g., using `DOTNET_DbgEnableMiniDump=1` and related env vars, or specific `dotnet-dump` triggers).
            *   Example: `dotnet-dump collect -p <process-id> --type Full` (Full dump is more useful for memory analysis).
        *   Analyze the dump using tools like `dotnet-dump analyze`, Visual Studio's memory analysis features, or third-party profilers (e.g., JetBrains dotMemory, SciTech Memory Profiler). Look for:
            *   Objects occupying the most space (e.g., using `dumpheap -stat` in `dotnet-dump analyze`).
            *   Unexpected object retention paths (potential leaks, using `gcroot`).
            *   LOH fragmentation.
3.  **Analyze GC Logs (If Enabled):**
    *   The .NET runtime can be configured (usually via environment variables like `DOTNET_EnableEventLog=1` and `DOTNET_GCLogFileSize`, or more detailed `EventPipe` configurations) to produce detailed GC logs. These logs provide insights into every collection, pause times, heap sizes before/after GC, and object promotion.
    *   Tools like PerfView or Visual Studio can analyze these logs, or manual inspection can help identify patterns if you are familiar with .NET GC internals.
4.  **Correlate with Kubernetes Events:**
    *   If a pod is OOMKilled, use `kubectl describe pod <pod-name>` to confirm the reason and check for any related Kubernetes events.
    *   Compare pod memory usage from Kubernetes monitoring with Flink.NET's internal memory configurations to see if there's a discrepancy or if total configured Flink.NET memory plus all overheads exceeds the pod limit.

## Specific Issue Resolution Strategies

*   **.NET `OutOfMemoryException`:**
    *   **Increase Heap:** The most straightforward solution if the workload genuinely needs more memory. Adjust relevant heap size parameters (e.g., `jobmanager.heap.memory.size`, `taskmanager.heap.memory.size` - planned) and ensure Kubernetes pod limits allow for this increase plus other memory segments.
    *   **Optimize UDFs:** This is crucial. Reduce object allocations, reuse objects where possible, release large objects promptly. Consider advanced techniques mentioned in the [Memory Tuning (Flink.NET)](./Core-Concepts-Memory-Tuning.md) guide if applicable.
    *   **Fix Leaks:** If a leak is identified via heap dump analysis, fix the code responsible for holding onto objects unnecessarily.
*   **Excessive GC Pressure:**
    *   **Increase Heap:** Can reduce GC frequency but might increase pause times if too large. Find a balance.
    *   **Optimize UDFs:** Aggressively reduce object churn (allocations/deallocations). This is often the most effective way to reduce GC pressure.
    *   **Tune GC (Advanced):** Ensure Server GC is used for production. Experimenting with .NET GC configuration variables (e.g., via `DOTNET_` environment variables like `DOTNET_GCConserveMemory` or `DOTNET_GCLatencyMode`) is highly advanced and should be a last resort after application-level optimizations.
*   **Kubernetes Pod OOMKilled:**
    *   **Increase Pod Limit:** Raise `resources.limits.memory` for the pod in your Kubernetes configuration.
    *   **Review Flink.NET Total Memory:** Ensure the sum of Flink.NET's configured memory components (heap, network, other, plus framework and .NET runtime overhead) is comfortably below the new pod limit. Refer to Flink.NET's guidance on calculating total process memory.
    *   **Check for Non-Flink Processes:** Ensure no other unexpected processes within the pod are consuming significant memory.
    *   **Investigate Framework Overhead:** Flink.NET will need to provide clear guidance on its framework memory overhead to allow for accurate sum-of-parts calculation.
*   **Insufficient Network Buffers:**
    *   Increase the total network memory (e.g., `taskmanager.network.memory.size` - planned).
    *   Analyze job structure to ensure network buffer configuration (buffers per channel, buffer size) is appropriate for the parallelism and data exchange patterns, as detailed in the [Network Memory Tuning (Flink.NET)](./Core-Concepts-Memory-Network.md) page.

---

*Further Reading:*
*   [Core Concepts: JobManager Memory (Flink.NET)](./Core-Concepts-Memory-JobManager.md)
*   [Core Concepts: TaskManager Memory (Flink.NET)](./Core-Concepts-Memory-TaskManager.md)
*   [Core Concepts: Memory Tuning (Flink.NET)](./Core-Concepts-Memory-Tuning.md)
*   [Core Concepts: Network Memory Tuning (Flink.NET)](./Core-Concepts-Memory-Network.md)

---
**Navigation**
*   Previous: [Memory Tuning](./Core-Concepts-Memory-Tuning.md)
