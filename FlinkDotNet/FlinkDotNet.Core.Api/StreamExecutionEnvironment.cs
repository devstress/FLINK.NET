#nullable enable
using FlinkDotNet.Core.Abstractions.Execution; // For SerializerRegistry
using System; // For Convert.ToBase64String
using FlinkDotNet.Core.Abstractions.Serializers; // For ITypeSerializer
using System.Linq; // Added for ToDictionary
using System.Threading.Tasks; // Added for Task

// Assuming JobGraph might be in a different namespace, added a placeholder using.
// This might need adjustment based on actual project structure.
using FlinkDotNet.JobManager.Models.JobGraph; // For JobGraph, JobVertex, JobEdge, etc.
using FlinkDotNet.Core.Api.Streaming; // For Transformation, DataStream, etc. (assuming these exist)
using System.Collections.Generic; // For List, Dictionary

// Removed embedded placeholder for JobGraph

namespace FlinkDotNet.Core.Api
{
    public class StreamExecutionEnvironment
    {
        private static StreamExecutionEnvironment? _defaultInstance;
        public SerializerRegistry SerializerRegistry { get; }
        public List<Transformation> Transformations { get; } = new List<Transformation>();

        // Constructor
        public StreamExecutionEnvironment()
        {
            SerializerRegistry = new SerializerRegistry();
            // Initialize other properties if any
        }

        public static StreamExecutionEnvironment GetExecutionEnvironment()
        {
            _defaultInstance ??= new StreamExecutionEnvironment();
            return _defaultInstance;
        }

        internal void AddTransformation(Transformation transformation)
        {
            Transformations.Add(transformation);
        }

        public DataStream<T> AddSource<T>(Abstractions.Sources.ISourceFunction<T> sourceFunction, string name)
        {
            var transformation = new SourceTransformation<T>(name, sourceFunction, this);
            AddTransformation(transformation);
            return new DataStream<T>(this, transformation);
        }


        public FlinkDotNet.JobManager.Models.JobGraph.JobGraph CreateJobGraph(string jobName = "MyFlinkJob")
        {
            var jobGraph = new FlinkDotNet.JobManager.Models.JobGraph.JobGraph(jobName);

            jobGraph.SerializerTypeRegistrations = SerializerRegistry.GetNamedRegistrations()
                                                       .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

            var transformationToVertexId = new Dictionary<Guid, Guid>();

            // First pass: Create all vertices
            foreach (var transform in Transformations)
            {
                var vertexType = transform switch
                {
                    SourceTransformation<object> _ => VertexType.Source, // Simplified, need generic handling
                    SinkTransformation<object> _ => VertexType.Sink,     // Simplified, need generic handling
                    _ => VertexType.Operator
                };

                // This is highly simplified. OperatorDefinition would need real configuration.
                var opDef = new OperatorDefinition(
                    transform.OperatorType.AssemblyQualifiedName ?? "UnknownOperator",
                    "{}"); // Empty JSON config for now

                var vertex = new JobVertex(
                    transform.Name,
                    vertexType,
                    opDef,
                    transform.Parallelism,
                    transform.InputType?.AssemblyQualifiedName,
                    transform.OutputType?.AssemblyQualifiedName
                );

                // TODO: Populate vertex.InputSerializerTypeName and vertex.OutputSerializerTypeName
                // from SerializerRegistry based on InputType and OutputType.

                jobGraph.AddVertex(vertex);
                transformationToVertexId[transform.Id] = vertex.Id;
            }

            // Second pass: Create all edges
            foreach (var transform in Transformations)
            {
                Guid targetVertexId = transformationToVertexId[transform.Id];
                foreach (var inputTransform in transform.Inputs)
                {
                    Guid sourceVertexId = transformationToVertexId[inputTransform.Id];

                    // Determine ShuffleMode and OutputKeyingConfig
                    OutputKeyingConfigDto? keyingConfig = null; // Use the DTO
                    ShuffleMode shuffleMode;

                    // Check if inputTransform is a KeyedTransformation<TKey, TIn>
                    if (inputTransform.GetType().IsGenericType &&
                        inputTransform.GetType().GetGenericTypeDefinition() == typeof(KeyedTransformation<,>))
                    {
                        shuffleMode = ShuffleMode.Hash; // Definitely hash for keyed inputs

                        // Using dynamic to access properties of the generically typed KeyedTransformation
                        dynamic dynInputTransform = inputTransform;
                        object keySelectorInstance = dynInputTransform.KeySelectorInstance; // This is IKeySelector<TIn, TKey>
                        Type keySelectorType = keySelectorInstance.GetType();
                        Type keyType = dynInputTransform.KeyType; // This is typeof(TKey)

                        ITypeSerializer keySelectorSerializer = this.SerializerRegistry.GetSerializer(keySelectorType);
                        byte[] serializedKeySelectorBytes = keySelectorSerializer.Serialize(keySelectorInstance);
                        string base64SerializedKeySelector = Convert.ToBase64String(serializedKeySelectorBytes);

                        keyingConfig = new OutputKeyingConfigDto(
                            base64SerializedKeySelector,
                            keySelectorType.AssemblyQualifiedName,
                            keyType.AssemblyQualifiedName
                        );
                        Console.WriteLine($"StreamExecutionEnvironment: Created OutputKeyingConfig for edge. KeySelectorType: {keySelectorType.Name}, KeyType: {keyType.Name}");
                    }
                    else
                    {
                        shuffleMode = ShuffleMode.Forward; // Default if not a keyed transformation
                        keyingConfig = null;
                    }

                    // DataTypeName should be the OutputType of the sourceTransform (inputTransform here)
                    var edgeDataTypeName = inputTransform.OutputType?.AssemblyQualifiedName ?? "UnknownType";
                    // TODO: Get SerializerTypeName for edgeDataTypeName from SerializerRegistry

                    var edge = new JobEdge(
                        sourceVertexId,
                        targetVertexId,
                        edgeDataTypeName,
                        shuffleMode,
                        null, // serializerTypeName placeholder
                        keyingConfig
                    );
                    jobGraph.AddEdge(edge);

                    // Link edge to vertices (JobGraph.AddEdge should handle this if JobEdge constructor doesn't)
                    // Or, if JobVertex expects direct calls:
                    // jobGraph.Vertices.First(v => v.Id == sourceVertexId).AddOutputEdgeId(edge.Id);
                    // jobGraph.Vertices.First(v => v.Id == targetVertexId).AddInputEdgeId(edge.Id);
                    // Current JobVertex has internal AddInput/OutputEdgeId, JobGraph.AddEdge should be responsible.
                }
            }
            return jobGraph;
        }

        // Placeholder for ExecuteAsync if it's part of this class
        public Task ExecuteAsync(string jobName = "MyFlinkJob")
        {
            var jobGraph = CreateJobGraph(jobName);
            // TODO: Submit jobGraph to JobManager
            Console.WriteLine($"JobGraph '{jobGraph.JobName}' created. In a real scenario, this would be submitted.");
            return Task.CompletedTask;
        }
    }

    // Define Transformation and related classes if they are not in a separate file yet for PoC
    // These are very basic placeholders.
    namespace Streaming
    {
        public abstract class Transformation
        {
            public Guid Id { get; } = Guid.NewGuid();
            public string Name { get; }
            public Type? InputType { get; protected set; }
            public Type? OutputType { get; protected set; }
            public int Parallelism { get; set; } = 1;
            public List<Transformation> Inputs { get; } = new List<Transformation>();
            public abstract Type OperatorType { get; }


            protected Transformation(string name, StreamExecutionEnvironment environment)
            {
                Name = name;
            }
        }

        public class SourceTransformation<TOut> : Transformation
        {
            public Abstractions.Sources.ISourceFunction<TOut> SourceFunction { get; }
            public override Type OperatorType => SourceFunction.GetType();


            public SourceTransformation(string name, Abstractions.Sources.ISourceFunction<TOut> sourceFunction, StreamExecutionEnvironment environment)
                : base(name, environment)
            {
                SourceFunction = sourceFunction;
                OutputType = typeof(TOut);
            }
        }

        // TKey is the type of the key, TIn is the type of the input element
        public class KeyedTransformation<TKey, TIn> : Transformation
        {
            public Abstractions.Functions.IKeySelector<TIn, TKey> KeySelectorInstance { get; }
            public Type KeyType => typeof(TKey);
            public override Type OperatorType { get; }

            public KeyedTransformation(
                string name,
                Transformation input, // This input is of type TIn
                Abstractions.Functions.IKeySelector<TIn, TKey> keySelector,
                StreamExecutionEnvironment environment)
                : base(name, environment)
            {
                Inputs.Add(input);
                InputType = input.OutputType; // Should be TIn
                OutputType = input.OutputType; // Output type is still TIn after keying
                OperatorType = typeof(object); // Keying itself is not a runtime operator, but a property of an edge/stream
                KeySelectorInstance = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
            }
        }


        public class SinkTransformation<TIn> : Transformation
        {
            public Abstractions.Sinks.ISinkFunction<TIn> SinkFunction { get; }
            public override Type OperatorType => SinkFunction.GetType();

            public SinkTransformation(string name, DataStream<TIn> inputStream, Abstractions.Sinks.ISinkFunction<TIn> sinkFunction, StreamExecutionEnvironment environment)
                : base(name, environment)
            {
                SinkFunction = sinkFunction;
                Inputs.Add(inputStream.Transformation);
                InputType = inputStream.Transformation.OutputType;
                OutputType = null; // Sinks don't have an output type
            }
        }

        public class DataStream<T>
        {
            public StreamExecutionEnvironment Environment { get; }
            public Transformation Transformation { get; }

            public DataStream(StreamExecutionEnvironment environment, Transformation transformation)
            {
                Environment = environment;
                Transformation = transformation;
            }

            public DataStream<TOut> Map<TOut>(Abstractions.Operators.IMapOperator<T, TOut> mapper, string name)
            {
                var mapTransformation = new MapTransformation<T, TOut>(name, this, mapper, Environment);
                Environment.AddTransformation(mapTransformation);
                return new DataStream<TOut>(Environment, mapTransformation);
            }

            public KeyedDataStream<TKey, T> KeyBy<TKey>(
                Abstractions.Functions.IKeySelector<T, TKey> keySelector,
                string name)
            {
                // T is TIn (element type), TKey is TKey (key type)
                // KeyedTransformation is defined as KeyedTransformation<TKey, TIn>
                var keyedTransformation = new KeyedTransformation<TKey, T>( // Correctly TKey, T
                    name,
                    this.Transformation, // input transformation (produces T)
                    keySelector,        // IKeySelector<T, TKey>
                    this.Environment);

                return new KeyedDataStream<TKey, T>(this.Environment, keyedTransformation);
            }


            public DataStream<TOut> Process<TOut>(string name /* complex operator */)
            {
                // Placeholder for more complex operators
                // var processTransformation = new ProcessTransformation<T, TOut>(name, this, ..., Environment);
                // Environment.AddTransformation(processTransformation);
                // return new DataStream<TOut>(Environment, processTransformation);
                throw new NotImplementedException();
            }


            public void AddSink(Abstractions.Sinks.ISinkFunction<T> sinkFunction, string name)
            {
                var sinkTransformation = new SinkTransformation<T>(name, this, sinkFunction, Environment);
                Environment.AddTransformation(sinkTransformation);
            }
        }

        public class MapTransformation<TIn, TOut> : Transformation
        {
            public Abstractions.Operators.IMapOperator<TIn, TOut> Mapper { get; }
            public override Type OperatorType => Mapper.GetType();


            public MapTransformation(string name, DataStream<TIn> inputStream, Abstractions.Operators.IMapOperator<TIn, TOut> mapper, StreamExecutionEnvironment environment)
                : base(name, environment)
            {
                Mapper = mapper;
                Inputs.Add(inputStream.Transformation);
                InputType = inputStream.Transformation.OutputType;
                OutputType = typeof(TOut);
            }
        }
    }
}
#nullable disable
