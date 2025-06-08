using FlinkDotNet.Core.Abstractions.Serializers;
using System; // Required for ArgumentNullException

namespace FlinkDotNet.Core.Abstractions.Models.State
{
    /// <summary>
    /// Base class for state descriptors. Contains the name of the state.
    /// </summary>
    public abstract class StateDescriptor
    {
        /// <summary>
        /// Gets the name of the state.
        /// </summary>
        public string Name { get; }

        protected StateDescriptor(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new System.ArgumentException("State name cannot be null or whitespace.", nameof(name));
            }
            Name = name;
        }

        // Future: Add TypeSerializer<T> properties here or in derived classes.
    }
}
