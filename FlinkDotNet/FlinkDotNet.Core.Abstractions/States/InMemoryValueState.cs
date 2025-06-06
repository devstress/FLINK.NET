#nullable enable
using System;
using System.Collections.Generic;
using FlinkDotNet.Core.Abstractions.Context; // For IRuntimeContext
using FlinkDotNet.Core.Abstractions.Models.State; // For ValueStateDescriptor

namespace FlinkDotNet.Core.Abstractions.States
{
    public class InMemoryValueState<T> : IValueState<T>
    {
        private readonly ValueStateDescriptor<T> _descriptor;
        private readonly IRuntimeContext _runtimeContext;

        private readonly Dictionary<object, T> _keyedValues = new Dictionary<object, T>();
        private readonly Dictionary<object, bool> _keyedHasValue = new Dictionary<object, bool>();

        public InMemoryValueState(ValueStateDescriptor<T> descriptor, IRuntimeContext runtimeContext)
        {
            _descriptor = descriptor ?? throw new ArgumentNullException(nameof(descriptor));
            _runtimeContext = runtimeContext ?? throw new ArgumentNullException(nameof(runtimeContext));
        }

        public T Value()
        {
            object? currentKey = _runtimeContext.GetCurrentKey();
            if (currentKey == null)
            {
                throw new InvalidOperationException("Cannot access value state: current key is not set in the runtime context. Ensure this operator runs in a keyed stream context.");
            }

            if (_keyedHasValue.TryGetValue(currentKey, out bool hasValue) && hasValue)
            {
                return _keyedValues[currentKey];
            }
            else
            {
                // Return default from descriptor if no value is set for this key
                return _descriptor.DefaultValue;
            }
        }

        public void Update(T newValue)
        {
            object? currentKey = _runtimeContext.GetCurrentKey();
            if (currentKey == null)
            {
                throw new InvalidOperationException("Cannot update value state: current key is not set in the runtime context.");
            }

            _keyedValues[currentKey] = newValue;
            _keyedHasValue[currentKey] = true;
        }

        public void Clear()
        {
            object? currentKey = _runtimeContext.GetCurrentKey();
            if (currentKey == null)
            {
                throw new InvalidOperationException("Cannot clear value state: current key is not set in the runtime context.");
            }

            _keyedValues.Remove(currentKey);
            _keyedHasValue.Remove(currentKey);
        }

        /// <summary>
        /// FOR SNAPSHOTTING: Returns all key-value pairs stored in this state.
        /// The owning operator will use this to snapshot the state.
        /// </summary>
        public IDictionary<object, T> GetSerializedStateEntries()
        {
            // Return a copy to avoid modification during serialization
            return new Dictionary<object, T>(_keyedValues);
        }

        /// <summary>
        /// Restores the keyed state from a provided dictionary.
        /// Clears any existing state and populates it with the given entries.
        /// </summary>
        /// <param name="entries">The keyed data to restore.</param>
        public void RestoreSerializedStateEntries(IDictionary<object, T> entries)
        {
            _keyedValues.Clear();
            _keyedHasValue.Clear();

            if (entries != null)
            {
                foreach (var entry in entries)
                {
                    _keyedValues[entry.Key] = entry.Value;
                    _keyedHasValue[entry.Key] = true; // Assume all restored values are considered "set"
                }
            }
            Console.WriteLine($"InMemoryValueState for '{_descriptor.Name}': Restored {_keyedValues.Count} keyed entries via RestoreSerializedStateEntries.");
        }
    }
}
#nullable disable
