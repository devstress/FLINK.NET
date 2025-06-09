using System;
using System.Collections.Generic;
using FlinkDotNet.Core.Abstractions.Context; // For IRuntimeContext
using FlinkDotNet.Core.Abstractions.Models.State; // For ValueStateDescriptor
using FlinkDotNet.Core.Abstractions.Runtime; // For BasicRuntimeContext
using FlinkDotNet.Core.Abstractions.Serializers;

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

        // Convenience constructor used in unit tests
        public InMemoryValueState(T defaultValue, ITypeSerializer<T> serializer)
            : this(new ValueStateDescriptor<T>("in_memory", serializer, defaultValue), new BasicRuntimeContext())
        {
        }

        private object GetEffectiveKey()
        {
            return _runtimeContext.GetCurrentKey() ?? NonKeyedScopePlaceholder;
        }

        public T? Value()
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
            _keyedStorage[keyToUse] = value;
        }

        public void Clear()
        {
            object keyToUse = GetEffectiveKey();
            _keyedStorage[keyToUse] = default!;
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
            _keyedStorage.Clear(); // This line remains as is
            if (entries == null)
            {
                return;
            }

            foreach (var entry in entries)
            {
                // Simplified condition: only store if key is not the placeholder instance.
                if (object.ReferenceEquals(entry.Key, NonKeyedScopePlaceholder))
                {
                    continue;
                }

                _keyedStorage[entry.Key] = entry.Value;
            }
        }
    }
}
#nullable disable
