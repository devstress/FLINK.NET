#nullable enable
using FlinkDotNet.Core.Abstractions.Execution; // For SerializerRegistry
// Potentially other using statements like FlinkDotNet.JobManager.Models.JobGraph
using System.Linq; // Added for ToDictionary
using System.Threading.Tasks; // Added for Task

// Assuming JobGraph might be in a different namespace, added a placeholder using.
// This might need adjustment based on actual project structure.
using FlinkDotNet.JobManager.Models.JobGraph; // For JobGraph, JobVertex, JobEdge, etc.
using FlinkDotNet.Core.Api.Streaming; // For Transformation, DataStream, etc. (assuming these exist)
using System.Collections.Generic; // For List, Dictionary
using FlinkDotNet.Core.Abstractions.Operators; // For ChainingStrategy
using FlinkDotNet.Core.Abstractions.Serializers; // Added for JsonPocoSerializer for KeyingInfo fallback
using System; // Added for Convert.ToBase64String, ArgumentNullException

// Removed embedded placeholder for JobGraph

namespace FlinkDotNet.Core.Api
{
    public class StreamExecutionEnvironment
    {
        private static StreamExecutionEnvironment? _defaultInstance;
        public SerializerRegistry SerializerRegistry { get; }
        public List<FlinkDotNet.Core.Api.Streaming.Transformation> Transformations { get; } = new List<FlinkDotNet.Core.Api.Streaming.Transformation>(); // Correctly typed list

        private bool _isChainingEnabled = true;
        private ChainingStrategy _defaultChainingStrategy = ChainingStrategy.ALWAYS; // Align with Flink's common operator default

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

        internal void AddTransformation(FlinkDotNet.Core.Api.Streaming.Transformation transformation) // Corrected parameter type
        {
            Transformations.Add(transformation);
        }

        public FlinkDotNet.Core.Api.Streaming.DataStream<T> AddSource<T>(Abstractions.Sources.ISourceFunction<T> sourceFunction, string name) // Corrected return type
        {
            // Correctly instantiate SourceTransformation from the Streaming namespace
            var transformation = new FlinkDotNet.Core.Api.Streaming.SourceTransformation<T>(name, sourceFunction, typeof(T));
            AddTransformation(transformation);
            return new FlinkDotNet.Core.Api.Streaming.DataStream<T>(this, transformation);
        }


        public FlinkDotNet.JobManager.Models.JobGraph.JobGraph CreateJobGraph(string jobName = "MyFlinkJob")
        {
            var jobGraph = new FlinkDotNet.JobManager.Models.JobGraph.JobGraph(jobName)
            {
                SerializerTypeRegistrations = SerializerRegistry.GetNamedRegistrations()
                                                               .ToDictionary(kvp => kvp.Key, kvp => kvp.Value)
            };

            var transformationToJobVertexIdMap = new Dictionary<FlinkDotNet.Core.Api.Streaming.Transformation, Guid>();
            var visitedTransformations = new HashSet<FlinkDotNet.Core.Api.Streaming.Transformation>();

            foreach (var rootTransform in Transformations)
            {
                // Ensure all transformations are treated as FlinkDotNet.Core.Api.Streaming.Transformation
                var streamingRootTransform = rootTransform as FlinkDotNet.Core.Api.Streaming.Transformation;
                if (streamingRootTransform == null)
                {
                    // This should not happen if Transformations list is correctly populated
                    throw new InvalidOperationException("Transformation in list is not of expected type.");
                }

                if (!visitedTransformations.Contains(streamingRootTransform))
                {
                    BuildGraphNodeRecursive(streamingRootTransform, jobGraph, transformationToJobVertexIdMap, visitedTransformations, null, null);
                }
            }

            return jobGraph;
        }

        private void BuildGraphNodeRecursive(
            FlinkDotNet.Core.Api.Streaming.Transformation currentTransform,
            JobGraph jobGraph,
            Dictionary<FlinkDotNet.Core.Api.Streaming.Transformation, Guid> transformationToJobVertexIdMap,
            HashSet<FlinkDotNet.Core.Api.Streaming.Transformation> visited,
            JobVertex? currentChainHeadVertex,
            FlinkDotNet.Core.Api.Streaming.Transformation? previousTransformInChain)
        {
            visited.Add(currentTransform);

            Type operatorClrType = typeof(object); // Default
            object? operatorInstance = null;

            // Correctly get Operator Type and Instance based on actual Transformation type
            if (currentTransform is FlinkDotNet.Core.Api.Streaming.SourceTransformation<object> srcT) // Assuming generic type for casting
            {
                operatorInstance = srcT.SourceFunction;
                operatorClrType = operatorInstance.GetType();
            }
            else if (currentTransform is FlinkDotNet.Core.Api.Streaming.OneInputTransformation<object, object> oneInT) // Assuming generic types
            {
                operatorInstance = oneInT.Operator;
                operatorClrType = operatorInstance.GetType();
            }
            // TODO: Add SinkTransformation if it exists and is used similarly.
            // else if (currentTransform is FlinkDotNet.Core.Api.Streaming.SinkTransformation<object> sinkT)
            // {
            //     operatorInstance = sinkT.SinkFunction;
            //     operatorClrType = operatorInstance.GetType();
            // }
            else if (currentTransform is FlinkDotNet.Core.Api.Streaming.KeyedTransformation<object,object> keyT)
            {
                operatorInstance = keyT;
                operatorClrType = keyT.GetType();
            }


            var opDef = new OperatorDefinition(
                operatorClrType.AssemblyQualifiedName ?? currentTransform.GetType().AssemblyQualifiedName ?? "UnknownOperator",
                "{}" // Empty JSON config for now
            );

            string? opInputTypeName = null;
            string opOutputTypeName = currentTransform.OutputType.AssemblyQualifiedName!;

            if (previousTransformInChain != null)
            {
                opInputTypeName = previousTransformInChain.OutputType.AssemblyQualifiedName;
            }
            else if (currentTransform is FlinkDotNet.Core.Api.Streaming.OneInputTransformation<object, object> oneInputNode)
            {
                 opInputTypeName = oneInputNode.Input.OutputType.AssemblyQualifiedName;
            }
             else if (currentTransform is FlinkDotNet.Core.Api.Streaming.KeyedTransformation<object,object> keyedNode)
            {
                 opInputTypeName = keyedNode.Input.OutputType.AssemblyQualifiedName;
            }

            if (currentChainHeadVertex == null)
            {
                VertexType vertexType = currentTransform is FlinkDotNet.Core.Api.Streaming.SourceTransformation<object>
                    ? VertexType.Source
                    : VertexType.Operator;

                string? vertexInputTypeName = opInputTypeName;

                var newChainHeadVertex = new JobVertex(
                    currentTransform.Name,
                    vertexType,
                    opDef,
                    currentTransform.Parallelism
                )
                {
                    InputTypeName = vertexInputTypeName,
                    OutputTypeName = opOutputTypeName
                };

                jobGraph.AddVertex(newChainHeadVertex);
                transformationToJobVertexIdMap[currentTransform] = newChainHeadVertex.Id;
                currentChainHeadVertex = newChainHeadVertex;
            }
            else
            {
                currentChainHeadVertex.ChainedOperators.Add(opDef);
                transformationToJobVertexIdMap[currentTransform] = currentChainHeadVertex.Id;
                currentChainHeadVertex.OutputTypeName = opOutputTypeName;
            }

            foreach (var (downstreamTransform, shuffleMode) in currentTransform.DownstreamTransformations)
            {
                Guid sourceVertexIdForEdge = transformationToJobVertexIdMap[currentTransform];
                JobVertex currentPhysicalVertex = jobGraph.Vertices.First(v => v.Id == sourceVertexIdForEdge);

                // --- Start of RESOLVED BLOCK 1 for StreamExecutionEnvironment.cs ---
                KeyingInfo? keyingConfig = null;

                if (shuffleMode == ShuffleMode.Hash)
                {
                    if (currentTransform is FlinkDotNet.Core.Api.Streaming.KeyedTransformation<object,object> keyedSourceTransform)
                    {
                        keyingConfig = new KeyingInfo(
                            keyedSourceTransform.SerializedKeySelectorRepresentation,
                            keyedSourceTransform.KeyType.AssemblyQualifiedName!
                        );
                        Console.WriteLine($"StreamExecutionEnvironment: Created KeyingInfo for edge. KeyType: {keyedSourceTransform.KeyType.Name}");
                    }
                    else
                    {
                        Console.WriteLine($"Warning: shuffleMode is Hash but currentTransform ({currentTransform.Name}) is not a KeyedTransformation.");
                    }
                }

                bool canChain = false;

                if (this._isChainingEnabled)
                {
                    ChainingStrategy upstreamStrategy = currentTransform.ChainingStrategy;
                    ChainingStrategy downstreamStrategy = downstreamTransform.ChainingStrategy;

                    bool strategiesAllow = true;
                    if (upstreamStrategy == ChainingStrategy.NEVER || downstreamStrategy == ChainingStrategy.NEVER)
                    {
                        strategiesAllow = false;
                    }

                    if (downstreamStrategy == ChainingStrategy.HEAD)
                    {
                        strategiesAllow = false;
                    }

                    if (strategiesAllow)
                    {
                        canChain = (shuffleMode == ShuffleMode.Forward &&
                                    currentTransform.Parallelism == downstreamTransform.Parallelism &&
                                    !(downstreamTransform is FlinkDotNet.Core.Api.Streaming.SourceTransformation<object>));
                    }
                }
                // --- End of RESOLVED BLOCK 1 ---

                if (canChain)
                {
                    BuildGraphNodeRecursive(downstreamTransform, jobGraph, transformationToJobVertexIdMap, visited, currentChainHeadVertex, currentTransform);
                }
                else
                {
                    Guid targetVertexIdForEdge;
                    if (transformationToJobVertexIdMap.TryGetValue(downstreamTransform, out var existingTargetId))
                    {
                        targetVertexIdForEdge = existingTargetId;
                    }
                    else
                    {
                        BuildGraphNodeRecursive(downstreamTransform, jobGraph, transformationToJobVertexIdMap, visited, null, null);
                        targetVertexIdForEdge = transformationToJobVertexIdMap[downstreamTransform];
                    }

                    var edge = new JobEdge(
                        sourceVertexIdForEdge,
                        targetVertexIdForEdge,
                        currentTransform.OutputType.AssemblyQualifiedName!,
                        shuffleMode,
                        null
                    );
                    jobGraph.AddEdge(edge);

                    if (shuffleMode == ShuffleMode.Hash && keyingConfig != null)
                    {
                         currentPhysicalVertex.OutputEdgeKeying[edge.Id] = keyingConfig;
                    }
                }
            }
        }

        public Task ExecuteAsync(string jobName = "MyFlinkJob")
        {
            var jobGraph = CreateJobGraph(jobName);
            Console.WriteLine($"JobGraph '{jobGraph.JobName}' created. In a real scenario, this would be submitted.");
            return Task.CompletedTask;
        }

        public void DisableOperatorChaining()
        {
            this._isChainingEnabled = false;
        }

        public bool IsChainingEnabled()
        {
            return this._isChainingEnabled;
        }

        // --- Start of RESOLVED BLOCK 2 for StreamExecutionEnvironment.cs ---
        public void SetDefaultChainingStrategy(ChainingStrategy strategy)
        {
            this._defaultChainingStrategy = strategy;
        }

        public class KeyedTransformation<TKey, TIn> : Transformation
        {
            public Abstractions.Functions.IKeySelector<TIn, TKey> KeySelectorInstance { get; }
            public Type KeyType => typeof(TKey);
            public string SerializedKeySelectorRepresentation { get; }
            public Type KeySelectorType { get; }
            // OperatorType is inherited or defined in base Transformation

            public KeyedTransformation(
                string name,
                Transformation input,
                Abstractions.Functions.IKeySelector<TIn, TKey> keySelector,
                StreamExecutionEnvironment environment,
                string serializedKeySelectorRepresentation,
                Type keySelectorType)
                : base(name, environment)
            {
                Inputs.Add(input);
                InputType = input.OutputType;
                OutputType = input.OutputType;
                KeySelectorInstance = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
                SerializedKeySelectorRepresentation = serializedKeySelectorRepresentation;
                KeySelectorType = keySelectorType;
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
                OutputType = null;
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
                string name,
                string serializedKeySelectorRepresentation, // Added
                Type keySelectorType) // Added
            {
                var keyedTransformation = new KeyedTransformation<TKey, T>(
                    name,
                    this.Transformation,
                    keySelector,
                    this.Environment,
                    serializedKeySelectorRepresentation,
                    keySelectorType);

                return new KeyedDataStream<TKey, T>(this.Environment, keyedTransformation);
            }

            public DataStream<TOut> Process<TOut>(string name /* complex operator */)
            {
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

        public ChainingStrategy GetDefaultChainingStrategy()
        {
            return this._defaultChainingStrategy;
        }
        // --- End of RESOLVED BLOCK 2 ---
    }
}
#nullable disable
