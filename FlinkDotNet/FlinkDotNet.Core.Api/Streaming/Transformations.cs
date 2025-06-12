using FlinkDotNet.Core.Api.Windowing;
using FlinkDotNet.Core.Abstractions.Operators;
using FlinkDotNet.Core.Abstractions.Common; // For Time
using FlinkDotNet.Core.Abstractions.Windowing; // For Window

namespace FlinkDotNet.Core.Api.Windowing // Place Evictor stub here for organization
{
    /// <summary>
    /// Interface for Evictor. Evictors can remove elements from a window
    /// after a trigger fires but before the window function is applied.
    /// </summary>
    public interface IEvictor<in TElement, TWindow> where TWindow : Window
    {
        /// <summary>
        /// Called before the window function is applied.
        /// </summary>
        void EvictBefore(IEnumerable<TElement> elements, int size, TWindow window);
        
        /// <summary>
        /// Called after the window function is applied.
        /// </summary>
        void EvictAfter(IEnumerable<TElement> elements, int size, TWindow window);
    }
}

namespace FlinkDotNet.Core.Api.Streaming
{
    public class WindowedTransformation<TElement, TWindow> : Transformation<TElement>
        where TWindow : Window
    {
        // Input is KeyedTransformation<TKey, TElement> which itself is a Transformation<TElement>
        public Transformation<TElement> Input { get; }
        public IWindowAssigner<TElement, TWindow> Assigner { get; }

        public Trigger<TElement, TWindow>? Trigger { get; internal set; }
        public IEvictor<TElement, TWindow>? Evictor { get; internal set; }
        public Time? AllowedLateness { get; internal set; }
        // The ITypeSerializer<TWindow> from Assigner.GetWindowSerializer() will be used by the runtime.

        public WindowedTransformation(
            Transformation<TElement> input, // Specifically, this will be a KeyedTransformation<TKey, TElement>
            IWindowAssigner<TElement, TWindow> assigner)
            : base(input.Name + $".Window({assigner.GetType().Name})")
        {
            Input = input;
            Assigner = assigner;
            // Set default trigger from assigner if GetDefaultTrigger can be called here.
            // It might need the StreamExecutionEnvironment, which isn't directly available here.
            // For now, WindowedStream constructor or .Trigger() method handles setting it.
            // Trigger = assigner.GetDefaultTrigger(null!); // Assuming null env is okay for some defaults or it's set later
        }
    }

    // Base for transformations that apply a window function
    public abstract class WindowFunctionTransformation<TOutput> : Transformation<TOutput>
    {
        public TransformationBase WindowedInput { get; }

        protected WindowFunctionTransformation(
            TransformationBase windowedInput,
            string transformationName)
            : base(transformationName)
        {
            WindowedInput = windowedInput;
        }
    }

    public class WindowReduceTransformation<TElement> : WindowFunctionTransformation<TElement>
    {
        public IReduceOperator<TElement> ReduceFunction { get; }

        public WindowReduceTransformation(
            TransformationBase windowedInput,
            IReduceOperator<TElement> reduceFunction)
            : base(windowedInput, $"{windowedInput.Name}.Reduce")
        {
            ReduceFunction = reduceFunction ?? throw new ArgumentNullException(nameof(reduceFunction));
        }
    }

    public class WindowAggregateTransformation<TResult> : WindowFunctionTransformation<TResult>
    {
        public object AggregateFunction { get; }

        public WindowAggregateTransformation(
            TransformationBase windowedInput,
            object aggregateFunction)
            : base(windowedInput, $"{windowedInput.Name}.Aggregate")
        {
            AggregateFunction = aggregateFunction ?? throw new ArgumentNullException(nameof(aggregateFunction));
        }
    }

    public class WindowProcessTransformation<TResult> : WindowFunctionTransformation<TResult>
    {
        public object ProcessWindowFunction { get; }

        public WindowProcessTransformation(
            TransformationBase windowedInput,
            object processWindowFunction)
            : base(windowedInput, $"{windowedInput.Name}.Process")
        {
            ProcessWindowFunction = processWindowFunction ?? throw new ArgumentNullException(nameof(processWindowFunction));
        }
    }
}
