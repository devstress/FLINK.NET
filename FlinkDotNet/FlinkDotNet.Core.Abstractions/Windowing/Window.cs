// Copyright (c) Geta Digital. All rights reserved.
// Licensed under Apache-2.0. See the LICENSE file in the project root for more information

using System.Diagnostics.CodeAnalysis;

namespace FlinkDotNet.Core.Abstractions.Windowing
{
    /// <summary>
    /// Base class for all window types (e.g., TimeWindow, GlobalWindow).
    /// A Window is a logical grouping of elements from a stream.
    /// 
    /// Note: This is intentionally an abstract class rather than an interface
    /// to provide common Equals/GetHashCode implementations which are essential
    /// for window objects used as keys in internal state management.
    /// </summary>
    [SuppressMessage("Design", "S1694:Convert this 'abstract' class to an interface", 
        Justification = "Abstract class is required to provide common Equals/GetHashCode implementations essential for window objects used as keys in internal state management")]
    public abstract class Window
    {
        /// <summary>
        /// Gets the maximum timestamp that is included in this window.
        /// For time-based windows, this is typically the window end timestamp minus one.
        /// For other window types (like count or global), this might have different semantics
        /// or could represent a logical point in time.
        /// </summary>
        public abstract long MaxTimestamp();

        // It's important for Window objects to implement Equals and GetHashCode
        // correctly, as they are often used as keys in internal state (e.g., for per-window state).
        public abstract override bool Equals(object? obj);
        public abstract override int GetHashCode();
    }
}
