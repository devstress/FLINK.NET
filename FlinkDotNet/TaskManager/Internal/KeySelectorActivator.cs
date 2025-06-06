#nullable enable
using System;
using System.Collections.Concurrent;
using System.Reflection;
using FlinkDotNet.Core.Abstractions.Functions; // For IKeySelector

namespace FlinkDotNet.TaskManager.Internal
{
    public class KeySelectorActivator
    {
        private readonly ConcurrentDictionary<string, Func<object, object?>> _activatedSelectorsCache = new();

        public Func<object, object?>? GetOrCreateKeySelector(
            string serializedSelector,
            string elementTypeName,
            string? keyTypeName, // keyTypeName might not be strictly needed for prop/field if key type inferred from member
            string taskNameForLogging)
        {
            if (string.IsNullOrEmpty(serializedSelector))
            {
                Console.WriteLine($"[{taskNameForLogging}] KeySelectorActivator: Serialized selector is null or empty.");
                return null;
            }

            if (string.IsNullOrEmpty(elementTypeName))
            {
                 Console.WriteLine($"[{taskNameForLogging}] KeySelectorActivator: Element type name is null or empty for selector '{serializedSelector}'.");
                return null;
            }

            // Cache key could also include elementTypeName if selectors for same string but different types are possible (unlikely here)
            return _activatedSelectorsCache.GetOrAdd(serializedSelector, (selectorStr) => {
                Type? elementType = Type.GetType(elementTypeName, throwOnError: false);
                if (elementType == null)
                {
                    Console.WriteLine($"[{taskNameForLogging}] KeySelectorActivator ERROR: Element type '{elementTypeName}' not found for key selector '{selectorStr}'.");
                    return element => {
                        Console.WriteLine($"[{taskNameForLogging}] KeySelectorActivator Fallback: Element type '{elementTypeName}' was not found. Using element's hashcode.");
                        return element?.GetHashCode() ?? 0;
                    };
                }

                Func<object, object?> createdDelegate;
                try
                {
                    if (selectorStr.StartsWith("prop:"))
                    {
                        string propName = selectorStr.Substring("prop:".Length);
                        PropertyInfo? propertyInfo = elementType.GetProperty(propName, BindingFlags.Public | BindingFlags.Instance);
                        if (propertyInfo == null) throw new MissingMemberException(elementType.FullName, propName);
                        createdDelegate = (element) => element != null ? propertyInfo.GetValue(element) : null;
                    }
                    else if (selectorStr.StartsWith("field:"))
                    {
                        string fieldName = selectorStr.Substring("field:".Length);
                        FieldInfo? fieldInfo = elementType.GetField(fieldName, BindingFlags.Public | BindingFlags.Instance);
                        if (fieldInfo == null) throw new MissingMemberException(elementType.FullName, fieldName);
                        createdDelegate = (element) => element != null ? fieldInfo.GetValue(element) : null;
                    }
                    else if (selectorStr.StartsWith("type:"))
                    {
                        string typeName = selectorStr.Substring("type:".Length);
                        Type? keySelectorImplType = Type.GetType(typeName, throwOnError: true); // Let it throw if type not found
                        if (keySelectorImplType == null) throw new TypeLoadException($"IKeySelector implementation type '{typeName}' not found.");

                        object keySelectorInstance = Activator.CreateInstance(keySelectorImplType)!;

                        MethodInfo? getKeyMethod = null;
                        // Try to find GetKey(TIn) where TIn is exactly elementType or a base type
                        foreach (var m in keySelectorImplType.GetMethods(BindingFlags.Public | BindingFlags.Instance))
                        {
                            if (m.Name == "GetKey")
                            {
                                var parameters = m.GetParameters();
                                if (parameters.Length == 1 && parameters[0].ParameterType.IsAssignableFrom(elementType))
                                {
                                    getKeyMethod = m;
                                    break;
                                }
                            }
                        }
                        if (getKeyMethod == null) throw new MissingMethodException(keySelectorImplType.FullName, $"GetKey compatible with parameter type {elementType.Name}");

                        createdDelegate = (element) => element != null ? getKeyMethod.Invoke(keySelectorInstance, new object[] { element }) : null;
                    }
                    else
                    {
                        Console.WriteLine($"[{taskNameForLogging}] KeySelectorActivator WARNING: Unknown key selector format: '{selectorStr}'. Falling back to GetHashCode().");
                        createdDelegate = element => element?.GetHashCode() ?? 0;
                    }
                    Console.WriteLine($"[{taskNameForLogging}] KeySelectorActivator: Successfully activated key selector for '{selectorStr}'.");
                    return createdDelegate;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{taskNameForLogging}] KeySelectorActivator ERROR: Could not create key selector for '{selectorStr}' (ElementType: {elementType.Name}). Error: {ex.Message}. Falling back to GetHashCode().");
                    return element => {
                         Console.WriteLine($"[{taskNameForLogging}] KeySelectorActivator Fallback: Error during activation of '{selectorStr}'. Using element's hashcode.");
                         return element?.GetHashCode() ?? 0;
                    };
                }
            });
        }
    }
}
#nullable disable
