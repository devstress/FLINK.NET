namespace FlinkDotNet.Core.Abstractions.Collectors
{
    /// <summary>
    /// Interface for a collector that collects records of a specific type.
    /// This is used by operators like FlatMap to emit output elements.
    /// Similar to Flinks org.apache.flink.util.Collector.
    /// </summary>
    /// <typeparam name="T">The type of the records collected by this collector.</typeparam>
    public interface ICollector<in T>
    {
        /// <summary>
        /// Emits a record.
        /// </summary>
        /// <param name="record">The record to collect.</param>
        void Collect(T record);

        // Flinks Collector interface also has a close() method, though its often
        // managed by the operator lifecycle. For now, we will omit it to keep
        // this interface minimal, focusing on the primary Collect action.
        // It can be added later if a specific use case for explicit collector closing arises.
    }
}
