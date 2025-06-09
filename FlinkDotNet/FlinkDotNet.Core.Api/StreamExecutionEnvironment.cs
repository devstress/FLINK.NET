using FlinkDotNet.Core.Abstractions.Execution; // For SerializerRegistry
using FlinkDotNet.Core.Abstractions.Sources; // For ISourceFunction
using FlinkDotNet.Core.Abstractions.Common; // For Time, if needed by API
// Potentially other using statements like FlinkDotNet.JobManager.Models.JobGraph
using System.Linq; // Added for ToDictionary
using System.Threading.Tasks; // Added for Task

// Assuming JobGraph might be in a different namespace, added a placeholder using.
// This might need adjustment based on actual project structure.
using FlinkDotNet.JobManager.Models.JobGraph; // For JobGraph, JobVertex, JobEdge, etc.
using FlinkDotNet.Core.Api.Streaming; // For Transformation, DataStream, etc. (assuming these exist)
using System.Collections.Generic; // For List, Dictionary
using FlinkDotNet.Core.Abstractions.Operators; // For ChainingStrategy

// Removed embedded placeholder for JobGraph

namespace FlinkDotNet.Core.Api
{
    public class StreamExecutionEnvironment
    {
        private static StreamExecutionEnvironment? _defaultInstance;
        public SerializerRegistry SerializerRegistry { get; }
        public List<FlinkDotNet.Core.Api.Streaming.Transformation<object>> Transformations { get; } = new List<FlinkDotNet.Core.Api.Streaming.Transformation<object>>();

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

        internal void AddTransformation<T>(FlinkDotNet.Core.Api.Streaming.Transformation<T> transformation)
        {
            if (transformation is null)
            {
                throw new ArgumentNullException(nameof(transformation));
            }

            Transformations.Add((FlinkDotNet.Core.Api.Streaming.Transformation<object>)(object)transformation);
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

            var transformationToJobVertexIdMap = new Dictionary<FlinkDotNet.Core.Api.Streaming.Transformation<object>, Guid>();
            var visitedTransformations = new HashSet<FlinkDotNet.Core.Api.Streaming.Transformation<object>>();

            foreach (var rootTransform in Transformations)
            {
                // Ensure all transformations are treated as FlinkDotNet.Core.Api.Streaming.Transformation<object>
                var streamingRootTransform = rootTransform as FlinkDotNet.Core.Api.Streaming.Transformation<object>;
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
            FlinkDotNet.Core.Api.Streaming.Transformation<object> currentTransform,
            JobGraph jobGraph,
            Dictionary<FlinkDotNet.Core.Api.Streaming.Transformation<object>, Guid> transformationToJobVertexIdMap,
            HashSet<FlinkDotNet.Core.Api.Streaming.Transformation<object>> visited,
            JobVertex? currentChainHeadVertex,
            FlinkDotNet.Core.Api.Streaming.Transformation<object>? previousTransformInChain)
        {
            visited.Add(currentTransform);

            Type operatorClrType = typeof(object); // Default
            object? operatorInstance = null;

            // Correctly get Operator Type and Instance based on actual Transformation type
            if (currentTransform is FlinkDotNet.Core.Api.Streaming.SourceTransformation<object> srcT)
            {
                operatorInstance = srcT.SourceFunction;
                operatorClrType = operatorInstance.GetType();
            }
            else if (currentTransform is FlinkDotNet.Core.Api.Streaming.OneInputTransformation<object, object> oneInT)
            {
                operatorInstance = oneInT.Operator;
                operatorClrType = operatorInstance.GetType();
            }
            else if (currentTransform is FlinkDotNet.Core.Api.Streaming.SinkTransformation<object> sinkT)
            {
                operatorInstance = sinkT.SinkFunction;
                operatorClrType = operatorInstance.GetType();
            }
            else if (currentTransform is FlinkDotNet.Core.Api.Streaming.KeyedTransformation<object,object> keyT) // This cast might need to be <object, object> if TKey is object
            {
                // KeyedTransformation might not have a direct "operator" instance in the same way.
                // It represents a state/partitioning characteristic.
                // For OperatorDefinition, we might use a generic type or a specific marker type.
                // For now, let's use its own type. Its main effect is on the edge.
                operatorInstance = keyT; // The transformation itself can be the "operator" for definition purposes
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
            // Casts to specific transformation types to access their 'Input' property
            else if (currentTransform is FlinkDotNet.Core.Api.Streaming.OneInputTransformation<object, object> oneInputNode)
            {
                 opInputTypeName = oneInputNode.Input.OutputType.AssemblyQualifiedName;
            }
             else if (currentTransform is FlinkDotNet.Core.Api.Streaming.KeyedTransformation<object,object> keyedNode) // Assuming TKey is object for KeyedTransformation
            {
                 opInputTypeName = keyedNode.Input.OutputType.AssemblyQualifiedName;
            }
            // SourceTransformations will have null opInputTypeName if they are head, which is correct.

            if (currentChainHeadVertex == null) // Starts a new JobVertex
            {
                VertexType vertexType;
                if (currentTransform is FlinkDotNet.Core.Api.Streaming.SourceTransformation<object>)
                {
                    vertexType = VertexType.Source;
                }
                else if (currentTransform is FlinkDotNet.Core.Api.Streaming.SinkTransformation<object>)
                {
                    vertexType = VertexType.Sink;
                }
                else
                {
                    vertexType = VertexType.Operator;
                }

                string? vertexInputTypeName = opInputTypeName; // For the first operator in a chain

                // For a Source, opInputTypeName would be null, which is correct for VertexInputTypeName
                // For an operator that is head of a new chain, opInputTypeName is its actual input.

                var newChainHeadVertex = new JobVertex(
                    currentTransform.Name,
                    vertexType,
                    opDef,
                    currentTransform.Parallelism
                )
                {
                    InputTypeName = vertexInputTypeName,
                    OutputTypeName = opOutputTypeName // Initially, the output of the head is the output of the vertex
                };

                jobGraph.AddVertex(newChainHeadVertex);
                transformationToJobVertexIdMap[currentTransform] = newChainHeadVertex.Id;
                currentChainHeadVertex = newChainHeadVertex;
            }
            else // Chain currentTransform onto currentChainHeadVertex
            {
                currentChainHeadVertex.ChainedOperators.Add(opDef);
                transformationToJobVertexIdMap[currentTransform] = currentChainHeadVertex.Id;
                currentChainHeadVertex.OutputTypeName = opOutputTypeName; // Update vertex output to the end of the chain
            }

            foreach (var (downstreamTransform, shuffleMode) in currentTransform.DownstreamTransformations)
            {
                Guid sourceVertexIdForEdge = transformationToJobVertexIdMap[currentTransform];
                JobVertex currentPhysicalVertex = jobGraph.Vertices.First(v => v.Id == sourceVertexIdForEdge);

                // Determine if currentTransform can chain with downstreamTransform
                bool canChain = false; // Default to not chaining

                if (this._isChainingEnabled) // Check global chaining flag first
                {
                    // Get strategies. Transformation.ChainingStrategy defaults to ALWAYS.
                    ChainingStrategy upstreamStrategy = currentTransform.ChainingStrategy;
                    // Downstream is Transformation<object>, but its concrete type (e.g. OneInputTransformation) will have ChainingStrategy
                    ChainingStrategy downstreamStrategy = ((FlinkDotNet.Core.Api.Streaming.Transformation<object>)downstreamTransform).ChainingStrategy;

                    bool strategiesAllow = true;
                    if (upstreamStrategy == ChainingStrategy.NEVER || downstreamStrategy == ChainingStrategy.NEVER)
                    {
                        strategiesAllow = false;
                    }

                    // If the downstream operator wants to be the head of a chain,
                    // it cannot be chained to the current upstream operator.
                    if (downstreamStrategy == ChainingStrategy.HEAD)
                    {
                        strategiesAllow = false;
                    }

                    // Note: If upstreamStrategy is HEAD, it means currentTransform itself did not chain with *its*
                    // predecessor. It can still chain with its successor if downstreamStrategy is ALWAYS.
                    // This is implicitly handled because currentChainHeadVertex would have been null when
                    // BuildGraphNodeRecursive was called for currentTransform if it was HEAD.

                    if (strategiesAllow)
                    {
                        canChain = (shuffleMode == ShuffleMode.Forward &&
                                    currentTransform.Parallelism == ((FlinkDotNet.Core.Api.Streaming.Transformation<object>)downstreamTransform).Parallelism &&
                                    !(downstreamTransform is FlinkDotNet.Core.Api.Streaming.SourceTransformation<object>));
                                    // Ensure downstream is not a source.
                                    // Add other fundamental chaining conditions if any (e.g. not a sink that must terminate a chain if such a type exists).
                    }
                }
                // 'canChain' now holds the final decision.

                if (canChain)
                {
                        BuildGraphNodeRecursive((FlinkDotNet.Core.Api.Streaming.Transformation<object>)downstreamTransform, jobGraph, transformationToJobVertexIdMap, visited, currentChainHeadVertex, currentTransform);
                }
                else
                {
                    Guid targetVertexIdForEdge;
                        if (transformationToJobVertexIdMap.TryGetValue((FlinkDotNet.Core.Api.Streaming.Transformation<object>)downstreamTransform, out var existingTargetId))
                    {
                        targetVertexIdForEdge = existingTargetId;
                    }
                    else
                    {
                            BuildGraphNodeRecursive((FlinkDotNet.Core.Api.Streaming.Transformation<object>)downstreamTransform, jobGraph, transformationToJobVertexIdMap, visited, null, null);
                            targetVertexIdForEdge = transformationToJobVertexIdMap[(FlinkDotNet.Core.Api.Streaming.Transformation<object>)downstreamTransform];
                    }

                    var edge = new JobEdge(
                        sourceVertexIdForEdge,
                        targetVertexIdForEdge,
                        currentTransform.OutputType.AssemblyQualifiedName!,
                        shuffleMode,
                        null // Serializer type name for the edge
                    );
                    jobGraph.AddEdge(edge);

                        if (shuffleMode == ShuffleMode.Hash && currentTransform is FlinkDotNet.Core.Api.Streaming.KeyedTransformation<object,object> keyedSource) // Assuming TKey is object
                    {
                         currentPhysicalVertex.OutputEdgeKeying[edge.Id] = new KeyingInfo(
                            keyedSource.SerializedKeySelectorRepresentation,
                            keyedSource.KeyType.AssemblyQualifiedName!
                        );
                    }
                }
            }
        }

        // Placeholder for ExecuteAsync if it's part of this class
        public Task ExecuteAsync(string jobName = "MyFlinkJob")
        {
            var jobGraph = CreateJobGraph(jobName);
            // TODO: Implement local/embedded execution or connect to an embedded JobManager.
            // For distributed execution, serialize JobGraph and submit via gRPC (see FlinkJobSimulator for an example).
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

        public void SetDefaultChainingStrategy(ChainingStrategy strategy)
        {
            this._defaultChainingStrategy = strategy;
        }

        public ChainingStrategy GetDefaultChainingStrategy()
        {
            return this._defaultChainingStrategy;
        }
    }
}
