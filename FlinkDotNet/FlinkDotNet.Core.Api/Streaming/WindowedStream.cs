using FlinkDotNet.Core.Abstractions.Common; // For Time
using FlinkDotNet.Core.Abstractions.Windowing; // For Window, TimeWindow, Evictor, Trigger
using FlinkDotNet.Core.Api.Windowing; // Retain for other API-specific windowing types if any
using FlinkDotNet.Core.Abstractions.Operators;
using FlinkDotNet.JobManager.Models.JobGraph; // For ShuffleMode, assuming it's here from previous generations

// --- Minimal Stubs for Dependencies ---
// These would ideally be in their own files in appropriate namespaces.
// Adding them here to make WindowedStream.cs self-contained for this step.

// The namespace FlinkDotNet.Core.Api.Windowing and its contents (Evictor, IProcessWindowFunction, ProcessWindowContext stubs)
// have been removed as they are expected to be defined elsewhere.


// Assuming Transformation classes are in this namespace (e.g., from DataStream.cs or a new Transformations.cs)
namespace FlinkDotNet.Core.Api.Streaming
{
    // --- WindowedStream Class ---
    /// <summary>
    /// --- Transformation Stubs for Windowed Operations ---
    /// These (WindowedTransformation, WindowReduceTransformation, WindowAggregateTransformation, WindowProcessTransformation)
    /// have been removed as they are expected to be defined elsewhere, likely in a Transformations.cs file or similar.
    /// </summary>
    /// <typeparam name="TElement"></typeparam>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TWindow"></typeparam>
    public class WindowedStream<TElement, TWindow>
        where TWindow : Window
    {
        public StreamExecutionEnvironment Environment { get; }
        public WindowedTransformation<TElement, TWindow> Transformation { get; }

        internal WindowedStream(
            StreamExecutionEnvironment environment,
            WindowedTransformation<TElement, TWindow> transformation)
        {
            Environment = environment ?? throw new ArgumentNullException(nameof(environment));
            Transformation = transformation ?? throw new ArgumentNullException(nameof(transformation));
        }

        public WindowedStream<TElement, TWindow> Trigger(Trigger<TElement, TWindow> trigger)
        {
            if (trigger == null) throw new ArgumentNullException(nameof(trigger));
            Transformation.Trigger = trigger;
            return this;
        }

        public WindowedStream<TElement, TWindow> Evictor(IEvictor<TElement, TWindow> evictor)
        {
            if (evictor == null) throw new ArgumentNullException(nameof(evictor));
            Transformation.Evictor = evictor;
            return this;
        }

        public WindowedStream<TElement, TWindow> AllowedLateness(Time lateness)
        {
            if (!Transformation.Assigner.IsEventTime)
            {
                throw new InvalidOperationException("Allowed lateness can only be set for event-time windows.");
            }
            Transformation.AllowedLateness = lateness;
            return this;
        }

        public DataStream<TElement> Reduce(IReduceOperator<TElement> reduceFunction)
        {
            if (reduceFunction == null) throw new ArgumentNullException(nameof(reduceFunction));
            var reduceTrans = new WindowReduceTransformation<TElement>(Transformation, reduceFunction);
            Transformation.Input.AddDownstreamTransformation(reduceTrans, ShuffleMode.Forward); // Edges connect from KeyedTransform to Window*Function*Transform
            return new DataStream<TElement>(this.Environment, reduceTrans);
        }

        public DataStream<TResult> Aggregate<TResult>(object aggregateFunction)
        {
            if (aggregateFunction == null) throw new ArgumentNullException(nameof(aggregateFunction));
            var aggTrans = new WindowAggregateTransformation<TResult>(Transformation, aggregateFunction);
            Transformation.Input.AddDownstreamTransformation(aggTrans, ShuffleMode.Forward);
            return new DataStream<TResult>(this.Environment, aggTrans);
        }

        public DataStream<TResult> Process<TResult>(object processWindowFunction)
        {
            if (processWindowFunction == null) throw new ArgumentNullException(nameof(processWindowFunction));
            var procTrans = new WindowProcessTransformation<TResult>(Transformation, processWindowFunction);
            Transformation.Input.AddDownstreamTransformation(procTrans, ShuffleMode.Forward);
            return new DataStream<TResult>(this.Environment, procTrans);
        }
    }
}
