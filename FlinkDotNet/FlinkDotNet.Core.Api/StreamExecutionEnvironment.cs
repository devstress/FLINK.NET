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
            var transformation = new FlinkDotNet.Core.Api.Streaming.SourceTransformation<T>(name, sourceFunction);
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

            var (operatorClrType, _) = DetermineOperatorTypeAndInstance(currentTransform);
            var transformType = currentTransform.GetType();


            var opDef = new OperatorDefinition(
                operatorClrType.AssemblyQualifiedName ?? currentTransform.GetType().AssemblyQualifiedName ?? "UnknownOperator",
                "{}" // Empty JSON config for now
            );

            string? opInputTypeName = ResolveOperatorInputTypeName(currentTransform, previousTransformInChain);
            string opOutputTypeName = currentTransform.OutputType.AssemblyQualifiedName!;

            currentChainHeadVertex = ProcessVertexCreationOrChaining(new VertexCreationContext(
                currentTransform, jobGraph, transformationToJobVertexIdMap, 
                currentChainHeadVertex, opDef, transformType, (opInputTypeName, opOutputTypeName)));

            ProcessDownstreamTransformations(currentTransform, jobGraph, transformationToJobVertexIdMap, visited, currentChainHeadVertex, transformType);
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
        /// This provides FlinkDotnet 2.0 compatible execution for development and testing.
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

        private static (Type operatorClrType, object? operatorInstance) DetermineOperatorTypeAndInstance(
            FlinkDotNet.Core.Api.Streaming.TransformationBase currentTransform)
        {
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
                operatorClrType = transformType;
            }

            return (operatorClrType, operatorInstance);
        }

        private static string? ResolveOperatorInputTypeName(
            FlinkDotNet.Core.Api.Streaming.TransformationBase currentTransform,
            FlinkDotNet.Core.Api.Streaming.TransformationBase? previousTransformInChain)
        {
            if (previousTransformInChain != null)
            {
                return previousTransformInChain.OutputType.AssemblyQualifiedName;
            }

            // Use reflection to access transformation inputs when needed
            var transformType = currentTransform.GetType();
            if (transformType.IsGenericType && transformType.GetGenericTypeDefinition() == typeof(FlinkDotNet.Core.Api.Streaming.OneInputTransformation<,>))
            {
                dynamic oneInputNode = currentTransform;
                return ((FlinkDotNet.Core.Api.Streaming.TransformationBase)oneInputNode.Input).OutputType.AssemblyQualifiedName;
            }
            else if (transformType.IsGenericType && transformType.GetGenericTypeDefinition() == typeof(FlinkDotNet.Core.Api.Streaming.KeyedTransformation<,>))
            {
                dynamic keyedNode = currentTransform;
                return ((FlinkDotNet.Core.Api.Streaming.TransformationBase)keyedNode.Input).OutputType.AssemblyQualifiedName;
            }
            // SourceTransformations will have null input type name if they are head, which is correct.
            return null;
        }

        private bool CanChainTransformations(
            FlinkDotNet.Core.Api.Streaming.TransformationBase currentTransform,
            FlinkDotNet.Core.Api.Streaming.TransformationBase downstreamTransform,
            ShuffleMode shuffleMode)
        {
            if (!this._isChainingEnabled) // Check global chaining flag first
            {
                return false;
            }

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
                return (shuffleMode == ShuffleMode.Forward &&
                        currentTransform.Parallelism == downstreamTransform.Parallelism &&
                        !(downstreamTransform.GetType().IsGenericType &&
                          downstreamTransform.GetType().GetGenericTypeDefinition() == typeof(FlinkDotNet.Core.Api.Streaming.SourceTransformation<>)));
                        // Ensure downstream is not a source.
                        // Add other fundamental chaining conditions if any (e.g. not a sink that must terminate a chain if such a type exists).
            }

            return false;
        }

        private readonly struct VertexCreationContext
        {
            public readonly FlinkDotNet.Core.Api.Streaming.TransformationBase CurrentTransform;
            public readonly JobGraph JobGraph;
            public readonly Dictionary<FlinkDotNet.Core.Api.Streaming.TransformationBase, Guid> TransformationToJobVertexIdMap;
            public readonly JobVertex? CurrentChainHeadVertex;
            public readonly OperatorDefinition OpDef;
            public readonly Type TransformType;
            public readonly string? OpInputTypeName;
            public readonly string OpOutputTypeName;

            public VertexCreationContext(
                FlinkDotNet.Core.Api.Streaming.TransformationBase currentTransform,
                JobGraph jobGraph,
                Dictionary<FlinkDotNet.Core.Api.Streaming.TransformationBase, Guid> transformationToJobVertexIdMap,
                JobVertex? currentChainHeadVertex,
                OperatorDefinition opDef,
                Type transformType,
                (string? input, string output) typeNames)
            {
                CurrentTransform = currentTransform;
                JobGraph = jobGraph;
                TransformationToJobVertexIdMap = transformationToJobVertexIdMap;
                CurrentChainHeadVertex = currentChainHeadVertex;
                OpDef = opDef;
                TransformType = transformType;
                OpInputTypeName = typeNames.input;
                OpOutputTypeName = typeNames.output;
            }
        }

        private static JobVertex ProcessVertexCreationOrChaining(VertexCreationContext context)
        {
            if (context.CurrentChainHeadVertex == null) // Starts a new JobVertex
            {
                VertexType vertexType;
                if (context.TransformType.IsGenericType && context.TransformType.GetGenericTypeDefinition() == typeof(FlinkDotNet.Core.Api.Streaming.SourceTransformation<>))
                {
                    vertexType = VertexType.Source;
                }
                else if (context.TransformType.IsGenericType && context.TransformType.GetGenericTypeDefinition() == typeof(FlinkDotNet.Core.Api.Streaming.SinkTransformation<>))
                {
                    vertexType = VertexType.Sink;
                }
                else
                {
                    vertexType = VertexType.Operator;
                }

                string? vertexInputTypeName = context.OpInputTypeName; // For the first operator in a chain

                // For a Source, opInputTypeName would be null, which is correct for VertexInputTypeName
                // For an operator that is head of a new chain, opInputTypeName is its actual input.

                var newChainHeadVertex = new JobVertex(
                    context.CurrentTransform.Name,
                    vertexType,
                    context.OpDef,
                    context.CurrentTransform.Parallelism
                )
                {
                    InputTypeName = vertexInputTypeName,
                    OutputTypeName = context.OpOutputTypeName // Initially, the output of the head is the output of the vertex
                };

                context.JobGraph.AddVertex(newChainHeadVertex);
                context.TransformationToJobVertexIdMap[context.CurrentTransform] = newChainHeadVertex.Id;
                return newChainHeadVertex;
            }
            else // Chain currentTransform onto currentChainHeadVertex
            {
                context.CurrentChainHeadVertex.ChainedOperators.Add(context.OpDef);
                context.TransformationToJobVertexIdMap[context.CurrentTransform] = context.CurrentChainHeadVertex.Id;
                context.CurrentChainHeadVertex.OutputTypeName = context.OpOutputTypeName; // Update vertex output to the end of the chain
                return context.CurrentChainHeadVertex;
            }
        }

        private void ProcessDownstreamTransformations(
            FlinkDotNet.Core.Api.Streaming.TransformationBase currentTransform,
            JobGraph jobGraph,
            Dictionary<FlinkDotNet.Core.Api.Streaming.TransformationBase, Guid> transformationToJobVertexIdMap,
            HashSet<FlinkDotNet.Core.Api.Streaming.TransformationBase> visited,
            JobVertex currentChainHeadVertex,
            Type transformType)
        {
            foreach (var (downstreamTransform, shuffleMode) in currentTransform.DownstreamTransformations)
            {
                Guid sourceVertexIdForEdge = transformationToJobVertexIdMap[currentTransform];
                JobVertex currentPhysicalVertex = jobGraph.Vertices.First(v => v.Id == sourceVertexIdForEdge);

                bool canChain = CanChainTransformations(currentTransform, downstreamTransform, shuffleMode);

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
    }
}
