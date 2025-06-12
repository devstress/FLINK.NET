// TODO: Reduce cognitive complexity
using FlinkDotNet.Core.Abstractions.Execution; // For SerializerRegistry
using FlinkDotNet.JobManager.Models.JobGraph; // For JobGraph, JobVertex, JobEdge, etc.
using FlinkDotNet.Core.Abstractions.Operators; // For ChainingStrategy

namespace FlinkDotNet.Core.Api
{
    public class StreamExecutionEnvironment
    {
        private static StreamExecutionEnvironment? _defaultInstance;
        public SerializerRegistry SerializerRegistry { get; }
        public List<FlinkDotNet.Core.Api.Streaming.TransformationBase> Transformations { get; } = new List<FlinkDotNet.Core.Api.Streaming.TransformationBase>();

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

        internal void AddTransformation(FlinkDotNet.Core.Api.Streaming.TransformationBase transformation)
        {
            if (transformation is null)
            {
                throw new ArgumentNullException(nameof(transformation));
            }

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

            var transformationToJobVertexIdMap = new Dictionary<FlinkDotNet.Core.Api.Streaming.TransformationBase, Guid>();
            var visitedTransformations = new HashSet<FlinkDotNet.Core.Api.Streaming.TransformationBase>();

            foreach (var rootTransform in Transformations)
            {
                var streamingRootTransform = rootTransform;

                if (!visitedTransformations.Contains(streamingRootTransform))
                {
                    BuildGraphNodeRecursive(streamingRootTransform, jobGraph, transformationToJobVertexIdMap, visitedTransformations, null, null);
                }
            }

            return jobGraph;
        }

        private void BuildGraphNodeRecursive(
            FlinkDotNet.Core.Api.Streaming.TransformationBase currentTransform,
            JobGraph jobGraph,
            Dictionary<FlinkDotNet.Core.Api.Streaming.TransformationBase, Guid> transformationToJobVertexIdMap,
            HashSet<FlinkDotNet.Core.Api.Streaming.TransformationBase> visited,
            JobVertex? currentChainHeadVertex,
            FlinkDotNet.Core.Api.Streaming.TransformationBase? previousTransformInChain)
        {
            visited.Add(currentTransform);

            Type operatorClrType = typeof(object); // Default
            object? operatorInstance = null;

            // Determine operator type and instance using reflection to avoid generic casts
            var transformType = currentTransform.GetType();
            if (transformType.IsGenericType && transformType.GetGenericTypeDefinition() == typeof(FlinkDotNet.Core.Api.Streaming.SourceTransformation<>))
            {
                dynamic srcT = currentTransform;
                operatorInstance = srcT.SourceFunction;
                operatorClrType = operatorInstance.GetType();
            }
            else if (transformType.IsGenericType && transformType.GetGenericTypeDefinition() == typeof(FlinkDotNet.Core.Api.Streaming.OneInputTransformation<,>))
            {
                dynamic oneInT = currentTransform;
                operatorInstance = oneInT.Operator;
                operatorClrType = operatorInstance.GetType();
            }
            else if (transformType.IsGenericType && transformType.GetGenericTypeDefinition() == typeof(FlinkDotNet.Core.Api.Streaming.SinkTransformation<>))
            {
                dynamic sinkT = currentTransform;
                operatorInstance = sinkT.SinkFunction;
                operatorClrType = operatorInstance.GetType();
            }
            else if (transformType.IsGenericType && transformType.GetGenericTypeDefinition() == typeof(FlinkDotNet.Core.Api.Streaming.KeyedTransformation<,>))
            {
                // KeyedTransformation does not wrap an operator instance directly
                operatorInstance = currentTransform;
                operatorClrType = transformType;
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
            // Use reflection to access transformation inputs when needed
            else if (transformType.IsGenericType && transformType.GetGenericTypeDefinition() == typeof(FlinkDotNet.Core.Api.Streaming.OneInputTransformation<,>))
            {
                dynamic oneInputNode = currentTransform;
                opInputTypeName = ((FlinkDotNet.Core.Api.Streaming.TransformationBase)oneInputNode.Input).OutputType.AssemblyQualifiedName;
            }
            else if (transformType.IsGenericType && transformType.GetGenericTypeDefinition() == typeof(FlinkDotNet.Core.Api.Streaming.KeyedTransformation<,>))
            {
                dynamic keyedNode = currentTransform;
                opInputTypeName = ((FlinkDotNet.Core.Api.Streaming.TransformationBase)keyedNode.Input).OutputType.AssemblyQualifiedName;
            }
            // SourceTransformations will have null opInputTypeName if they are head, which is correct.

            if (currentChainHeadVertex == null) // Starts a new JobVertex
            {
                VertexType vertexType;
                if (transformType.IsGenericType && transformType.GetGenericTypeDefinition() == typeof(FlinkDotNet.Core.Api.Streaming.SourceTransformation<>))
                {
                    vertexType = VertexType.Source;
                }
                else if (transformType.IsGenericType && transformType.GetGenericTypeDefinition() == typeof(FlinkDotNet.Core.Api.Streaming.SinkTransformation<>))
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
                    ChainingStrategy downstreamStrategy = downstreamTransform.ChainingStrategy;

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
                                    currentTransform.Parallelism == downstreamTransform.Parallelism &&
                                    !(downstreamTransform.GetType().IsGenericType &&
                                      downstreamTransform.GetType().GetGenericTypeDefinition() == typeof(FlinkDotNet.Core.Api.Streaming.SourceTransformation<>)));
                                    // Ensure downstream is not a source.
                                    // Add other fundamental chaining conditions if any (e.g. not a sink that must terminate a chain if such a type exists).
                    }
                }
                // 'canChain' now holds the final decision.

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
                        null // Serializer type name for the edge
                    );
                    jobGraph.AddEdge(edge);

                        if (shuffleMode == ShuffleMode.Hash &&
                            transformType.IsGenericType &&
                            transformType.GetGenericTypeDefinition() == typeof(FlinkDotNet.Core.Api.Streaming.KeyedTransformation<,>))
                    {
                        dynamic keyedSource = currentTransform;
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
            // For distributed execution, serialize JobGraph and submit via gRPC (see FlinkJobSimulator for an example).
            Console.WriteLine($"JobGraph '{jobGraph.JobName}' created. In a real scenario, this would be submitted.");
            return Task.CompletedTask;
        }

        /// <summary>
        /// Executes the job locally in the current process using the LocalStreamExecutor.
        /// This provides Apache Flink 2.0 compatible execution for development and testing.
        /// </summary>
        public async Task ExecuteLocallyAsync(string jobName = "MyFlinkJob", CancellationToken cancellationToken = default)
        {
            var jobGraph = CreateJobGraph(jobName);
            Console.WriteLine($"Executing JobGraph '{jobGraph.JobName}' locally...");
            
            var localExecutor = new FlinkDotNet.Core.Api.Execution.LocalStreamExecutor(this);
            await localExecutor.ExecuteJobAsync(jobGraph, cancellationToken);
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
