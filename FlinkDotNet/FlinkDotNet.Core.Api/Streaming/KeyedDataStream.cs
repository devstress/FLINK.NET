using System;
using FlinkDotNet.Core.Abstractions.Operators; // For IMapOperator, etc.
using FlinkDotNet.JobManager.Models.JobGraph; // For ShuffleMode

// Assuming StreamExecutionEnvironment is in FlinkDotNet.Core.Api
using FlinkDotNet.Core.Api;
using FlinkDotNet.Core.Api.Windowing; // For Window, WindowAssigner, WindowedStream

namespace FlinkDotNet.Core.Api.Streaming
{
    public class KeyedDataStream<TKey, TElement>
    {
        public StreamExecutionEnvironment Environment { get; }
        public KeyedTransformation<TKey, TElement> Transformation { get; }

        public KeyedDataStream(StreamExecutionEnvironment environment, KeyedTransformation<TKey, TElement> transformation)
        {
            Environment = environment ?? throw new ArgumentNullException(nameof(environment));
            Transformation = transformation ?? throw new ArgumentNullException(nameof(transformation));
        }

        /// <summary>
        /// Applies a Map transformation to this KeyedDataStream.
        /// The IMapOperator will be executed in parallel instances, each receiving data for a subset of keys.
        /// </summary>
        public DataStream<TOut> Map<TOut>(IMapOperator<TElement, TOut> mapper)
        {
            // When an operator is applied to a KeyedDataStream, the edge connecting the
            // input transformation (from KeyedTransformation.Input) to this new mapTransformation
            // should use ShuffleMode.Hash.
            var mapTransformation = new OneInputTransformation<TElement, TOut>(
                this.Transformation.Input, // Input is the transformation *before* keying conceptually for the edge
                "MapOnKeyed",
                mapper,
                typeof(TOut)
            );

            // The KeyedTransformation itself doesn't become a node with the operator.
            // Instead, the KeySelector and KeyType from this.Transformation are used to configure
            // the edge leading from this.Transformation.Input to mapTransformation.
            // This logic typically resides in how the JobGraph is built from transformations.
            // For now, we'll add it to the *input's* downstream transformations.
            this.Transformation.Input.AddDownstreamTransformation(mapTransformation, ShuffleMode.Hash);
            // And pass keySelector/keyType info with this edge, or JobGraphBuilder infers from KeyedTransformation.

            // The output is a regular DataStream, as Map doesn't preserve keying implicitly for the *next* operator
            // unless that operator is also key-aware and the stream is re-keyed or key is passed through.
            return new DataStream<TOut>(this.Environment, mapTransformation);
        }

        // Add other keyed operations here like Reduce, Aggregate, ProcessWindowFunction etc.
        // public DataStream<TElement> Reduce(IReduceOperator<TElement> reducer) { /* ... */ }

        /// <summary>
        /// Windows this KeyedDataStream into a WindowedStream, based on the provided WindowAssigner.
        /// </summary>
        /// <typeparam name="TNewWindow">The type of Window that the WindowAssigner creates.</typeparam>
        /// <param name="assigner">The WindowAssigner that assigns elements to windows.</param>
        /// <returns>A new WindowedStream.</returns>
        public WindowedStream<TElement, TKey, TNewWindow> Window<TNewWindow>(
            WindowAssigner<TElement, TNewWindow> assigner)
            where TNewWindow : Window
        {
            if (assigner == null)
                throw new ArgumentNullException(nameof(assigner));

            // this.Transformation is the KeyedTransformation<TKey, TElement>
            var windowedTransformation = new WindowedTransformation<TElement, TKey, TNewWindow>(
                this.Transformation,
                assigner
            );

            // The WindowedTransformation itself is not directly added as a downstream to KeyedTransformation's input.
            // Instead, it *wraps* the KeyedTransformation. Operations on the WindowedStream (like Reduce, Process)
            // will then create transformations that take this WindowedTransformation as input, and those
            // will be linked from the KeyedTransformation's input with ShuffleMode.Hash.
            // For now, WindowedTransformation primarily holds the windowing configuration.
            // The actual addition to the graph happens when a window function (Reduce, Process etc.) is applied.

            return new WindowedStream<TElement, TKey, TNewWindow>(this.Environment, windowedTransformation);
        }
    }
}
#nullable disable
