using System.Collections.Generic;
using FlinkDotNet.Core.Abstractions.Collectors; // For ICollector
using FlinkDotNet.Core.Abstractions.Windowing; // For Window

namespace FlinkDotNet.Core.Api.Windowing
{
    /// <summary>
    /// Function to be applied to all elements in a window.
    /// Provides access to the window's elements, the key, the window object, and context information.
    /// This is the most general windowing function, allowing for per-window state, side outputs (future),
    /// and arbitrary computations on all elements of a window.
    /// </summary>
    /// <typeparam name="TIn">Type of the input elements.</typeparam>
    /// <typeparam name="TOut">Type of the output elements.</typeparam>
    /// <typeparam name="TKey">Type of the key.</typeparam>
    /// <typeparam name="TWindow">Type of the window.</typeparam>
    public interface IProcessWindowFunction<in TIn, TOut, TKey, TWindow>
        where TWindow : Window
    {
        /// <summary>
        /// Evaluates the window and outputs results.
        /// </summary>
        /// <param name="key">The key for which this window is being evaluated.</param>
        /// <param name="context">Context object with access to window metadata, time, and state.</param>
        /// <param name="elements">All elements assigned to the current window for the given key.
        /// The elements are iterable. Note that this requires buffering all elements in the window until it fires.</param>
        /// <param name="output">A collector to emit resulting elements.</param>
        void Process(
            TKey key,
            IProcessWindowContext<TKey, TWindow> context,
            IEnumerable<TIn> elements,
            ICollector<TOut> output);

        /// <summary>
        /// Called when a window is purged. This can be used to clean up any per-window state.
        /// This is typically called when a trigger signals to purge, or when event-time windows are finally cleaned up.
        /// </summary>
        /// <param name="key">The key for which this window is being evaluated.</param>
        /// <param name="context">Context object with access to window metadata, time, and state.</param>
        void Clear(TKey key, IProcessWindowContext<TKey, TWindow> context); // Added Clear method
    }
}
