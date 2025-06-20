using FlinkDotNet.Core.Abstractions.Operators; // For IMapOperator as an example
using FlinkDotNet.JobManager.Models.JobGraph; // For ShuffleMode (might need to be more abstract later)
using System.Linq.Expressions; // For KeyBy expression overload
using System.Reflection; // For KeyBy expression overload
using FlinkDotNet.Core.Abstractions.Functions; // For IKeySelector
using FlinkDotNet.Core.Abstractions.Sinks; // For ISinkFunction

namespace FlinkDotNet.Core.Api.Streaming
{
    public class DataStream<TElement>
    {
        public StreamExecutionEnvironment Environment { get; }
        public Transformation<TElement> Transformation { get; } // Represents the current node in the logical plan

        public DataStream(StreamExecutionEnvironment environment, Transformation<TElement> transformation)
        {
            Environment = environment ?? throw new ArgumentNullException(nameof(environment));
            Transformation = transformation ?? throw new ArgumentNullException(nameof(transformation));
        }

        /// <summary>
        /// Partitions the DataStream by the given key selector.
        /// All elements with the same key will be sent to the same parallel instance of the next operator.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <param name="keySelector">The function to extract the key from each element.</param>
        /// <returns>A KeyedDataStream.</returns>
        public KeyedDataStream<TKey, TElement> KeyBy<TKey>(KeySelector<TElement, TKey> keySelector)
        {
            if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));

            // Create a new KeyedTransformation, which references the keySelector
            // and sets the shuffle mode to Hash for the edge leading to the next operator.
            // The actual JobEdge.ShuffleMode is set when an operator is applied *after* KeyBy.

            // For now, let's assume keySelector.Method.ToString() or similar as a placeholder.
            // A real system needs robust serialization for the key selector.
            // Using AssemblyQualifiedName of the delegate type and method name. This is highly fragile and likely
            // won't work across different assemblies or with complex lambda captures.
            // A proper solution would involve expression tree serialization or a dedicated lambda serialization library.
            string keySelectorRep = keySelector.GetType().AssemblyQualifiedName + "::" + keySelector.Method.Name;

            var keyedTransformation = new KeyedTransformation<TKey, TElement>(
                Transformation,
                keySelector,
                keySelectorRep);

            return new KeyedDataStream<TKey, TElement>(Environment, keyedTransformation);
        }

        /// <summary>
        /// Partitions the DataStream by the given expression key selector.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <param name="keySelectorExpression">The expression to extract the key from each element.</param>
        /// <returns>A KeyedDataStream.</returns>
        public KeyedDataStream<TKey, TElement> KeyBy<TKey>(
            Expression<Func<TElement, TKey>> keySelectorExpression)
        {
            if (keySelectorExpression == null)
                throw new ArgumentNullException(nameof(keySelectorExpression));

            string serializedSelectorRepresentation;
            // string keyTypeName = typeof(TKey).AssemblyQualifiedName!; // KeyType in KeyedTransformation is Type object

            Expression body = keySelectorExpression.Body;

            // Handle implicit conversions (e.g., int to object)
            if (body is UnaryExpression unaryExpression &&
                (unaryExpression.NodeType == ExpressionType.Convert || unaryExpression.NodeType == ExpressionType.ConvertChecked))
            {
                body = unaryExpression.Operand;
            }

            if (body is MemberExpression memberExpression)
            {
                // Ensure the member access is directly on the parameter of the lambda
                if (memberExpression.Expression == keySelectorExpression.Parameters[0])
                {
                    MemberInfo memberInfo = memberExpression.Member;
                    string memberName = memberInfo.Name;

                    if (memberInfo.MemberType == MemberTypes.Property)
                    {
                        serializedSelectorRepresentation = $"prop:{memberName}";
                    }
                    else if (memberInfo.MemberType == MemberTypes.Field)
                    {
                        serializedSelectorRepresentation = $"field:{memberName}";
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"Lambda expression's member access '{memberName}' is not a direct property or field access on the input element. " +
                            $"Unsupported member type: {memberInfo.MemberType}. " +
                            "Please use a simple property/field access (e.g., x => x.MyProperty) or an IKeySelector implementation.",
                            nameof(keySelectorExpression));
                    }
                }
                else
                {
                     throw new ArgumentException(
                        "Lambda expression involves nested member access (e.g., x.Property.NestedProperty) or static member access. " +
                        "Only direct property or field access on the input element (e.g., x => x.MyProperty) is supported for automatic translation. " +
                        "For complex key extraction, please use an IKeySelector implementation or pre-process with a Map operation.",
                        nameof(keySelectorExpression));
                }
            }
            else
            {
                throw new ArgumentException(
                    "Lambda expression is too complex. Only direct property or field access expressions (e.g., x => x.MyProperty) " +
                    "or those involving a simple cast (e.g., x => (object)x.MyProperty) are supported for automatic translation. " +
                    "For other logic, please implement IKeySelector<TElement, TKey> and use a different KeyBy overload.",
                    nameof(keySelectorExpression));
            }

            var keyedTransformation = new KeyedTransformation<TKey, TElement>(
                this.Transformation,
                keySelectorExpression, // Store the original expression for potential later analysis if needed
                serializedSelectorRepresentation
            );

            return new KeyedDataStream<TKey, TElement>(this.Environment, keyedTransformation);
        }

        /// <summary>
        /// Partitions the DataStream by the given key selector instance.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <param name="keySelectorInstance">The key selector instance.</param>
        /// <returns>A KeyedDataStream.</returns>
        public KeyedDataStream<TKey, TElement> KeyBy<TKey>(
            IKeySelector<TElement, TKey> keySelectorInstance)
        {
            if (keySelectorInstance == null)
                throw new ArgumentNullException(nameof(keySelectorInstance));

            Type keySelectorImplType = keySelectorInstance.GetType();
            string serializedSelectorRepresentation = $"type:{keySelectorImplType.AssemblyQualifiedName}";

            // string keyTypeName = typeof(TKey).AssemblyQualifiedName!; // KeyType in KeyedTransformation is Type object

            var keyedTransformation = new KeyedTransformation<TKey, TElement>(
                this.Transformation,
                keySelectorInstance, // Store the instance
                serializedSelectorRepresentation
            );

            return new KeyedDataStream<TKey, TElement>(this.Environment, keyedTransformation);
        }

        /// <summary>
        /// Partitions the DataStream by the given key selector type.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <param name="keySelectorType">The key selector type.</param>
        /// <returns>A KeyedDataStream.</returns>
        public KeyedDataStream<TKey, TElement> KeyBy<TKey>(Type keySelectorType)
        {
            if (keySelectorType == null)
                throw new ArgumentNullException(nameof(keySelectorType));

            // Basic validation could be added here to check if keySelectorType implements IKeySelector,
            // but full validation (including generic type arguments) is complex and might be
            // better deferred to the TaskExecutor's KeySelectorActivator, which will try to instantiate it.
            // Example check (might not work for all generic scenarios without more complex reflection):

            string serializedSelectorRepresentation = $"type:{keySelectorType.AssemblyQualifiedName}";
            // string keyTypeName = typeof(TKey).AssemblyQualifiedName!; // KeyType in KeyedTransformation is Type object

            var keyedTransformation = new KeyedTransformation<TKey, TElement>(
                this.Transformation,
                keySelectorType, // Store the Type; TaskExecutor will Activator.CreateInstance
                serializedSelectorRepresentation
            );

            return new KeyedDataStream<TKey, TElement>(this.Environment, keyedTransformation);
        }

        /// <summary>
        /// Applies a Map transformation to this DataStream.
        /// (Example of a typical DataStream operator to show context)
        /// </summary>
        public DataStream<TOut> Map<TOut>(IMapOperator<TElement, TOut> mapper)
        {
            var mapTransformation = new OneInputTransformation<TElement, TOut>(
                this.Transformation,
                "Map",
                mapper
                /* outputSerializer: null for now, JobManager will pick default or registered */
                );
            this.Transformation.AddDownstreamTransformation(mapTransformation, ShuffleMode.Forward); // Default for map
            return new DataStream<TOut>(this.Environment, mapTransformation);
        }

        // Add other common DataStream operations here like Filter, FlatMap, Sink, etc.
        public void AddSink(ISinkFunction<TElement> sinkFunction, string name = "Sink")
        {
            if (sinkFunction == null)
                throw new ArgumentNullException(nameof(sinkFunction));

            var sinkTransformation = new SinkTransformation<TElement>(
                this.Transformation,
                name,
                sinkFunction);

            // Link current transformation to this new sink transformation
            this.Transformation.AddDownstreamTransformation(sinkTransformation, ShuffleMode.Forward);
        }

        public DataStream<TElement> StartNewChain()
        {
            this.Transformation.ChainingStrategy = ChainingStrategy.HEAD;
            return this;
        }

        public DataStream<TElement> DisableChaining()
        {
            this.Transformation.ChainingStrategy = ChainingStrategy.NEVER;
            return this;
        }
    }

    // Simplified Transformation classes for illustration of the logical plan.
    // A more complete implementation would be needed for a full JobGraph builder.

    /// <summary>
    /// Non-generic base type for all transformations so that they can be stored
    /// in collections without invalid casts.
    /// </summary>
    public abstract class TransformationBase
    {
        public string Name { get; }
        public Type OutputType { get; }
        public int Parallelism { get; set; } = 1; // Default parallelism
        public List<(TransformationBase Output, ShuffleMode Mode)> DownstreamTransformations { get; } = new();
        public ChainingStrategy ChainingStrategy { get; set; } = ChainingStrategy.ALWAYS;

        protected TransformationBase(string name, Type outputType)
        {
            Name = name;
            OutputType = outputType;
        }

        public void AddDownstreamTransformation(TransformationBase downstream, ShuffleMode mode)
        {
            if (downstream is null)
            {
                throw new ArgumentNullException(nameof(downstream));
            }

            DownstreamTransformations.Add((downstream, mode));
        }
    }

    public abstract class Transformation<TElement> : TransformationBase
    {
        protected Transformation(string name)
            : base(name, typeof(TElement))
        {
        }
    }

    public class SourceTransformation<TElement> : Transformation<TElement>
    {
        public object SourceFunction { get; } // ISourceFunction<TElement>

        public SourceTransformation(string name, object sourceFunction)
            : base(name)
        {
            SourceFunction = sourceFunction;
        }
    }

    public class OneInputTransformation<TIn, TOut> : Transformation<TOut>
    {
        public Transformation<TIn> Input { get; }
        public object Operator { get; } // e.g., IMapOperator<TIn, TOut>

        public OneInputTransformation(Transformation<TIn> input, string name, object @operator)
            : base(name)
        {
            Input = input;
            Operator = @operator;
        }
    }

    public class KeyedTransformation<TKey, TElement> : Transformation<TElement> // Output type is still TElement
    {
        public Transformation<TElement> Input { get; }
        public object KeySelector { get; } // KeySelector<TElement, TKey> - store as object or serialized form
        public Type KeyType => typeof(TKey);
        public string SerializedKeySelectorRepresentation { get; }

        public KeyedTransformation(
            Transformation<TElement> input,
            object keySelector,
            string serializedKeySelectorRepresentation)
            : base(input.Name + ".Keyed")
        {
            Input = input;
            KeySelector = keySelector; // Store the delegate or its representation
            SerializedKeySelectorRepresentation = serializedKeySelectorRepresentation;
            // This transformation itself doesn't change data type, it's a partitioning instruction.
            // The next applied operator will be connected via a JobEdge with ShuffleMode.Hash.
        }
    }

    public class SinkTransformation<TElement> : Transformation<TElement>
    {
        public Transformation<TElement> Input { get; }
        public object SinkFunction { get; } // ISinkFunction<TElement>

        public SinkTransformation(Transformation<TElement> input, string name, object sinkFunction)
            : base(name) // OutputType is same as input, or could be typeof(void)
        {
            Input = input;
            SinkFunction = sinkFunction;
        }
    }
}
