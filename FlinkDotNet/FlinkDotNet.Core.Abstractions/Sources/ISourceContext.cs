using System.Threading.Tasks;
using FlinkDotNet.Core.Abstractions.Windowing;

namespace FlinkDotNet.Core.Abstractions.Sources
{
    /// <summary>
    /// Context that is available to source functions, allowing them to emit records.
    /// </summary>
    /// <typeparam name="TOut">The type of the records produced by the source.</typeparam>
    public interface ISourceContext<in TOut>
    {
        /// <summary>
        /// Emits a record from the source.
        /// </summary>
        /// <param name="record">The record to emit.</param>
        void Collect(TOut record);

        /// <summary>
        /// Emits a record from the source asynchronously.
        /// </summary>
        /// <param name="record">The record to emit.</param>
        Task CollectAsync(TOut record);

        /// <summary>
        /// Emits a record with the given event time timestamp.
        /// </summary>
        /// <param name="record">The record to emit.</param>
        /// <param name="timestamp">The event time timestamp in milliseconds.</param>
        void CollectWithTimestamp(TOut record, long timestamp);

        /// <summary>
        /// Emits a record with the given event time timestamp asynchronously.
        /// </summary>
        /// <param name="record">The record to emit.</param>
        /// <param name="timestamp">The event time timestamp in milliseconds.</param>
        Task CollectWithTimestampAsync(TOut record, long timestamp);

        /// <summary>
        /// Emits a watermark to signal the progress of event time.
        /// </summary>
        /// <param name="watermark">The watermark to emit.</param>
        void EmitWatermark(Watermark watermark);

        /// <summary>
        /// Gets the current processing time in milliseconds.
        /// </summary>
        long ProcessingTime { get; }
    }
}
#nullable disable
