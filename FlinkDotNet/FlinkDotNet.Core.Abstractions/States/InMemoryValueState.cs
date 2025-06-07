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

        // Internal storage: Key is the key from IRuntimeContext.GetCurrentKey(), Value is the user's state.
        // Using a placeholder object for non-keyed/null-key scenarios.
        private static readonly object NonKeyedScopePlaceholder = new object(); // Used as a key if IRuntimeContext.GetCurrentKey() is null.
        private readonly Dictionary<object, T> _keyedStorage = new Dictionary<object, T>();

        public InMemoryValueState(ValueStateDescriptor<T> descriptor, IRuntimeContext runtimeContext)
        {
            _descriptor = descriptor ?? throw new ArgumentNullException(nameof(descriptor));
            _runtimeContext = runtimeContext ?? throw new ArgumentNullException(nameof(runtimeContext));
            // Note: The serializer from the descriptor is not explicitly used in this in-memory version
            // for Get/Update/Clear as it deals with live objects. It would be crucial for
            // actual snapshotting/restoration to bytes.
        }

        private object GetEffectiveKey()
        {
            return _runtimeContext.GetCurrentKey() ?? NonKeyedScopePlaceholder;
        }

        public T Value()
        {
            object keyToUse = GetEffectiveKey();
            if (_keyedStorage.TryGetValue(keyToUse, out T? value))
            {
                return value;
            }
            return _descriptor.DefaultValue;
        }

        public void Update(T value)
        {
            object keyToUse = GetEffectiveKey();
            // Standard Flink behavior: if value to update is null (for nullable types) or default for value types,
            // the state for that key is cleared.
            // For simplicity here, we check against _descriptor.DefaultValue.
            // A more robust check might involve EqualityComparer<T>.Default.Equals(value, _descriptor.DefaultValue)
            // or specific checks for null if T is a reference type or Nullable<U>.

            // If T can be null (reference type or Nullable<ValueType>) and the value is null,
            // or if T is a value type and value is its default (which isn't what descriptor.DefaultValue usually means for clearing state),
            // Flink typically clears state if updated with null.
            // For non-nullable value types, they can't be set to null.
            // Let's refine this: if T is a reference type or Nullable<U>, and value is null, then clear. Otherwise, update.
            // The original _descriptor.DefaultValue check was potentially problematic.
            if (default(T) == null && value == null) // Check if T is nullable and value is null
            {
                 _keyedStorage.Remove(keyToUse);
            }
            else
            {
                _keyedStorage[keyToUse] = value;
            }
        }

        public void Clear()
        {
            object keyToUse = GetEffectiveKey();
            _keyedStorage.Remove(keyToUse);
        }

        /// <summary>
        /// FOR SNAPSHOTTING: Returns all key-value pairs stored in this state.
        /// The owning operator will use this to snapshot the state.
        /// </summary>
        internal IEnumerable<KeyValuePair<object, T>> GetKeyedStateEntries()
        {
            // Return a copy to avoid issues if the collection is modified during snapshotting by another thread
            // (though typically a single TaskExecutor processes records sequentially for an operator instance).
            return new List<KeyValuePair<object, T>>(_keyedStorage);
        }

        /// <summary>
        /// FOR RESTORATION: Clears current state and loads the provided keyed state entries.
        /// The owning operator will use this to restore state.
        /// </summary>
        internal void SetKeyedStateEntries(IEnumerable<KeyValuePair<object, T>> entries)
        {
            _keyedStorage.Clear();
            if (entries != null)
            {
                foreach (var entry in entries)
                {
                    // Ensure placeholder isn't used as a persisted key if it was somehow snapshotted,
                    // though GetKeyedStateEntries should ideally not return it if it represents a "cleared" state.
                    // This check might be overly cautious depending on how NonKeyedScopePlaceholder is handled during snapshot.
                    // If NonKeyedScopePlaceholder itself could be a valid key a user provides, this logic is flawed.
                    // For now, assume user keys won't be the exact same object instance as NonKeyedScopePlaceholder.
                    if (entry.Key != NonKeyedScopePlaceholder || !object.ReferenceEquals(entry.Key, NonKeyedScopePlaceholder))
                    {
                        _keyedStorage[entry.Key] = entry.Value;
                    }
                }
            }
        }
    }
}
#nullable disable
