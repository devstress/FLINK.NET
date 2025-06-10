using System;
using System.Collections.Generic; // For List in Transformation base
using FlinkDotNet.Core.Api.Windowing;
using FlinkDotNet.Core.Abstractions.Operators;
using FlinkDotNet.JobManager.Models.JobGraph; // For ShuffleMode in Transformation base
using FlinkDotNet.Core.Abstractions.Common; // For Time
using FlinkDotNet.Core.Abstractions.Windowing; // For Window

// This file contains Transformation types related to Windowing operations.
// Base Transformation types (Transformation, KeyedTransformation, etc.) are assumed to be in DataStream.cs.

namespace FlinkDotNet.Core.Api.Windowing // Place Evictor stub here for organization
{
    /// <summary>
    /// Placeholder for Evictor. Evictors can remove elements from a window
    /// after a trigger fires but before the window function is applied.
    /// </summary>
    public abstract class Evictor<TElement, TWindow> where TWindow : Window
    {
        // Example methods an evictor might have:
        // public abstract void EvictBefore(IEnumerable<TimestampedValue<TElement>> elements, int size, TWindow window, IEvictorContext evictorContext);
        // public abstract void EvictAfter(IEnumerable<TimestampedValue<TElement>> elements, int size, TWindow window, IEvictorContext evictorContext);
    }
}

namespace FlinkDotNet.Core.Api.Streaming
{
    public class WindowedTransformation<TElement, TKey, TWindow> : Transformation<TElement>
        where TWindow : Window
    {
        // Input is KeyedTransformation<TKey, TElement> which itself is a Transformation<TElement>
        public Transformation<TElement> Input { get; }
        public WindowAssigner<TElement, TWindow> Assigner { get; }

        public Trigger<TElement, TWindow>? Trigger { get; internal set; }
        public Evictor<TElement, TWindow>? Evictor { get; internal set; }
        public Time? AllowedLateness { get; internal set; }
        // The ITypeSerializer<TWindow> from Assigner.GetWindowSerializer() will be used by the runtime.

        public WindowedTransformation(
            Transformation<TElement> input, // Specifically, this will be a KeyedTransformation<TKey, TElement>
            WindowAssigner<TElement, TWindow> assigner)
            : base(input.Name + $".Window({assigner.GetType().Name})", input.OutputType)
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
    public abstract class WindowFunctionTransformation<TInputElement, TOutputElement, TKey, TWindow> : Transformation<TOutputElement>
        where TWindow : Window
    {
        public WindowedTransformation<TInputElement, TKey, TWindow> WindowedInput { get; }

        protected WindowFunctionTransformation(
            WindowedTransformation<TInputElement, TKey, TWindow> windowedInput,
            string transformationName, // Full name like "Window.Reduce", "Window.Process"
            Type outputType)
            : base(transformationName, outputType)
        {
            WindowedInput = windowedInput;
        }
    }

    public class WindowReduceTransformation<TElement, TKey, TWindow>
        : WindowFunctionTransformation<TElement, TElement, TKey, TWindow>
        where TWindow : Window
    {
        public IReduceOperator<TElement> ReduceFunction { get; }

        public WindowReduceTransformation(
            WindowedTransformation<TElement, TKey, TWindow> windowedInput,
            IReduceOperator<TElement> reduceFunction,
            Type outputType) // outputType will be typeof(TElement)
            : base(windowedInput, $"{windowedInput.Name}.Reduce", outputType)
        {
            ReduceFunction = reduceFunction ?? throw new ArgumentNullException(nameof(reduceFunction));
        }
    }

    public class WindowAggregateTransformation<TElement, TAccumulator, TResult, TKey, TWindow>
        : WindowFunctionTransformation<TElement, TResult, TKey, TWindow>
        where TWindow : Window
    {
        public IAggregateOperator<TElement, TAccumulator, TResult> AggregateFunction { get; }

        public WindowAggregateTransformation(
            WindowedTransformation<TElement, TKey, TWindow> windowedInput,
            IAggregateOperator<TElement, TAccumulator, TResult> aggregateFunction,
            Type outputType) // outputType will be typeof(TResult)
            : base(windowedInput, $"{windowedInput.Name}.Aggregate", outputType)
        {
            AggregateFunction = aggregateFunction ?? throw new ArgumentNullException(nameof(aggregateFunction));
        }
    }

    public class WindowProcessTransformation<TElement, TResult, TKey, TWindow>
        : WindowFunctionTransformation<TElement, TResult, TKey, TWindow>
        where TWindow : Window
    {
        public IProcessWindowFunction<TElement, TResult, TKey, TWindow> ProcessWindowFunction { get; }

        public WindowProcessTransformation(
            WindowedTransformation<TElement, TKey, TWindow> windowedInput,
            IProcessWindowFunction<TElement, TResult, TKey, TWindow> processWindowFunction,
            Type outputType) // outputType will be typeof(TResult)
            : base(windowedInput, $"{windowedInput.Name}.Process", outputType)
        {
            ProcessWindowFunction = processWindowFunction ?? throw new ArgumentNullException(nameof(processWindowFunction));
        }
    }
}
