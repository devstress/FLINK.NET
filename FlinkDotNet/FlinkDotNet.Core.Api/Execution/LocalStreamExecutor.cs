using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Sources;
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Operators;
using FlinkDotNet.JobManager.Models.JobGraph;
using FlinkDotNet.Core.Api.BackPressure;
using System.Collections.Concurrent;
using System.Reflection;
using Microsoft.Extensions.Logging;

namespace FlinkDotNet.Core.Api.Execution
{
    /// <summary>
    /// Local execution engine for FlinkDotNet that can execute JobGraphs in a single process.
    /// This enables local testing and development without requiring a full distributed setup.
    /// Implements core Apache Flink 2.0 execution concepts including back pressure handling.
    /// </summary>
    public class LocalStreamExecutor : IDisposable
    {
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly ConcurrentDictionary<Guid, ConcurrentQueue<object>> _dataChannels;
        private readonly LocalBackPressureDetector _backPressureDetector;
        private readonly ILogger<LocalStreamExecutor>? _logger;
        private bool _disposed;

        public LocalStreamExecutor(StreamExecutionEnvironment environment, ILogger<LocalStreamExecutor>? logger = null)
        {
            if (environment == null) throw new ArgumentNullException(nameof(environment));
            _cancellationTokenSource = new CancellationTokenSource();
            _dataChannels = new ConcurrentDictionary<Guid, ConcurrentQueue<object>>();
            _logger = logger;
            
            // Initialize back pressure detection for Apache Flink 2.0 compatibility
            _backPressureDetector = new LocalBackPressureDetector(
                new LocalBackPressureConfiguration
                {
                    BackPressureThreshold = 0.8, // 80% queue utilization triggers throttling
                    BaseThrottleDelayMs = 10,
                    DefaultMaxQueueSize = 1000
                });
            
            _logger?.LogInformation("LocalStreamExecutor initialized with Apache Flink 2.0 style back pressure detection");
        }

        /// <summary>
        /// Executes the given JobGraph locally in the current process.
        /// This provides Apache Flink 2.0 compatible execution semantics.
        /// </summary>
        public async Task ExecuteJobAsync(JobGraph jobGraph, CancellationToken cancellationToken = default)
        {
            if (jobGraph == null)
                throw new ArgumentNullException(nameof(jobGraph));

            Console.WriteLine($"[LocalStreamExecutor] Starting execution of JobGraph: {jobGraph.JobName}");
            Console.WriteLine($"[LocalStreamExecutor] Job has {jobGraph.Vertices.Count} vertices and {jobGraph.Edges.Count} edges");

            // Create a combined cancellation token
            using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken, _cancellationTokenSource.Token);

            try
            {
                // Step 1: Initialize data channels for communication between operators
                InitializeDataChannels(jobGraph);

                // Step 2: Create operator instances for each vertex
                var operatorInstances = CreateOperatorInstances(jobGraph);

                // Step 3: Start execution tasks for each vertex
                var executionTasks = StartVertexExecutionTasks(jobGraph, operatorInstances, combinedCts.Token);

                // Step 4: Wait for all tasks to complete
                Console.WriteLine($"[LocalStreamExecutor] Waiting for {executionTasks.Count} execution tasks to complete...");
                await Task.WhenAll(executionTasks);

                Console.WriteLine($"[LocalStreamExecutor] Job execution completed successfully");
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                Console.WriteLine($"[LocalStreamExecutor] Job execution was cancelled");
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[LocalStreamExecutor] Job execution failed: {ex.Message}");
                throw;
            }
        }

        private void InitializeDataChannels(JobGraph jobGraph)
        {
            foreach (var edgeId in jobGraph.Edges.Select(edge => edge.Id))
            {
                _dataChannels[edgeId] = new ConcurrentQueue<object>();
                Console.WriteLine($"[LocalStreamExecutor] Initialized data channel for edge {edgeId}");
            }
        }

        private Dictionary<Guid, OperatorInstance> CreateOperatorInstances(JobGraph jobGraph)
        {
            var instances = new Dictionary<Guid, OperatorInstance>();

            foreach (var vertex in jobGraph.Vertices)
            {
                Console.WriteLine($"[LocalStreamExecutor] Creating operator instance for vertex {vertex.Name}");
                
                // Get the main operator definition
                var operatorDef = vertex.OperatorDefinition;
                var operatorInstance = CreateOperatorFromDefinition(operatorDef);
                
                // Create runtime context
                var runtimeContext = new LocalRuntimeContext(vertex.Name, jobGraph.JobName);

                instances[vertex.Id] = new OperatorInstance
                {
                    Vertex = vertex,
                    Operator = operatorInstance, // Updated property name
                    RuntimeContext = runtimeContext,
                    ChainedOperators = vertex.ChainedOperators.Select(CreateOperatorFromDefinition).ToList()
                };

                Console.WriteLine($"[LocalStreamExecutor] Created operator instance for {vertex.Name} with {vertex.ChainedOperators.Count} chained operators");
            }

            return instances;
        }

        private static object CreateOperatorFromDefinition(OperatorDefinition operatorDef)
        {
            var operatorType = Type.GetType(operatorDef.FullyQualifiedName);
            if (operatorType == null)
            {
                throw new InvalidOperationException($"Could not load operator type: {operatorDef.FullyQualifiedName}");
            }

            // Try to create instance - for now, assume parameterless constructor
            // In a full implementation, this would handle constructor parameters from configuration
            var instance = Activator.CreateInstance(operatorType);
            if (instance == null)
            {
                throw new InvalidOperationException($"Could not create instance of operator type: {operatorType}");
            }

            return instance;
        }

        private List<Task> StartVertexExecutionTasks(
            JobGraph jobGraph, 
            Dictionary<Guid, OperatorInstance> operatorInstances, 
            CancellationToken cancellationToken)
        {
            var tasks = new List<Task>();

            foreach (var vertex in jobGraph.Vertices)
            {
                var operatorInstance = operatorInstances[vertex.Id];
                
                var task = vertex.Type switch
                {
                    VertexType.Source => StartSourceVertexTask(vertex, operatorInstance, jobGraph, cancellationToken),
                    VertexType.Operator => StartOperatorVertexTask(vertex, operatorInstance, jobGraph, cancellationToken),
                    VertexType.Sink => StartSinkVertexTask(vertex, operatorInstance, jobGraph, cancellationToken),
                    _ => throw new ArgumentOutOfRangeException($"Unknown vertex type: {vertex.Type}")
                };

                tasks.Add(task);
            }

            return tasks;
        }

        private Task StartSourceVertexTask(
            JobVertex vertex, 
            OperatorInstance operatorInstance, 
            JobGraph jobGraph, 
            CancellationToken cancellationToken)
        {
            return Task.Run(async () =>
            {
                Console.WriteLine($"[LocalStreamExecutor] Starting source vertex: {vertex.Name}");
                
                try
                {
                    // Open the operator
                    if (operatorInstance.Operator is IOperatorLifecycle lifecycle && operatorInstance.RuntimeContext != null)
                    {
                        lifecycle.Open(operatorInstance.RuntimeContext);
                    }

                    // Create source context that routes data to output channels
                    var outputChannels = GetOutputChannelsForVertex(vertex, jobGraph);
                    var sourceContext = new LocalSourceContext(outputChannels, cancellationToken);

                    // Run the source
                    if (operatorInstance.Operator is ISourceFunction<string> stringSource)
                    {
                        Console.WriteLine($"[LocalStreamExecutor] Running string source function directly");
                        await Task.Run(() => 
                        {
                            try
                            {
                                stringSource.Run(sourceContext);
                                Console.WriteLine($"[LocalStreamExecutor] String source function Run() completed successfully");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[LocalStreamExecutor] String source function Run() failed: {ex.GetType().Name}: {ex.Message}");
                                Console.WriteLine($"[LocalStreamExecutor] Stack trace: {ex.StackTrace}");
                                throw;
                            }
                        }, cancellationToken);
                    }
                    else if (operatorInstance.Operator != null)
                    {
                        // Handle other source types using reflection
                        Console.WriteLine($"[LocalStreamExecutor] Running source function using reflection");
                        await RunSourceUsingReflection(operatorInstance.Operator, sourceContext);
                    }

                    Console.WriteLine($"[LocalStreamExecutor] Source vertex {vertex.Name} completed");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[LocalStreamExecutor] Source vertex {vertex.Name} failed: {ex.Message}");
                    throw;
                }
                finally
                {
                    // Close the operator
                    if (operatorInstance.Operator is IOperatorLifecycle lifecycle)
                    {
                        lifecycle.Close();
                    }
                }
            }, cancellationToken);
        }

        private Task StartSinkVertexTask(
            JobVertex vertex, 
            OperatorInstance operatorInstance, 
            JobGraph jobGraph, 
            CancellationToken cancellationToken)
        {
            return Task.Run(async () =>
            {
                Console.WriteLine($"[LocalStreamExecutor] Starting sink vertex: {vertex.Name}");
                
                try
                {
                    // Open the operator
                    if (operatorInstance.Operator is IOperatorLifecycle lifecycle && operatorInstance.RuntimeContext != null)
                    {
                        lifecycle.Open(operatorInstance.RuntimeContext);
                    }

                    // Get input channels
                    var inputChannels = GetInputChannelsForVertex(vertex, jobGraph);
                    var sinkContext = new LocalSinkContext();

                    // Process data from input channels with back pressure monitoring
                    if (operatorInstance.Operator != null)
                    {
                        await ProcessSinkData(operatorInstance.Operator, inputChannels, sinkContext, cancellationToken, vertex.Name);
                    }

                    Console.WriteLine($"[LocalStreamExecutor] Sink vertex {vertex.Name} completed");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[LocalStreamExecutor] Sink vertex {vertex.Name} failed: {ex.Message}");
                    throw;
                }
                finally
                {
                    // Close the operator
                    if (operatorInstance.Operator is IOperatorLifecycle lifecycle)
                    {
                        lifecycle.Close();
                    }
                }
            }, cancellationToken);
        }

        private Task StartOperatorVertexTask(
            JobVertex vertex, 
            OperatorInstance operatorInstance, 
            JobGraph jobGraph, 
            CancellationToken cancellationToken)
        {
            return Task.Run(async () =>
            {
                Console.WriteLine($"[LocalStreamExecutor] Starting operator vertex: {vertex.Name}");
                
                try
                {
                    // Open the operator
                    if (operatorInstance.Operator is IOperatorLifecycle lifecycle && operatorInstance.RuntimeContext != null)
                    {
                        lifecycle.Open(operatorInstance.RuntimeContext);
                    }

                    // Get input and output channels
                    var inputChannels = GetInputChannelsForVertex(vertex, jobGraph);
                    var outputChannels = GetOutputChannelsForVertex(vertex, jobGraph);

                    // Process data through the operator with back pressure monitoring
                    if (operatorInstance.Operator != null)
                    {
                        await ProcessOperatorData(operatorInstance.Operator, inputChannels, outputChannels, cancellationToken, vertex.Name);
                    }

                    Console.WriteLine($"[LocalStreamExecutor] Operator vertex {vertex.Name} completed");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[LocalStreamExecutor] Operator vertex {vertex.Name} failed: {ex.Message}");
                    throw;
                }
                finally
                {
                    // Close the operator
                    if (operatorInstance.Operator is IOperatorLifecycle lifecycle)
                    {
                        lifecycle.Close();
                    }
                }
            }, cancellationToken);
        }

        private List<ConcurrentQueue<object>> GetOutputChannelsForVertex(JobVertex vertex, JobGraph jobGraph)
        {
            var channels = new List<ConcurrentQueue<object>>();
            
            foreach (var edge in jobGraph.Edges.Where(e => e.SourceVertexId == vertex.Id))
            {
                if (_dataChannels.TryGetValue(edge.Id, out var channel))
                {
                    channels.Add(channel);
                }
            }
            
            return channels;
        }

        private List<ConcurrentQueue<object>> GetInputChannelsForVertex(JobVertex vertex, JobGraph jobGraph)
        {
            var channels = new List<ConcurrentQueue<object>>();
            
            foreach (var edge in jobGraph.Edges.Where(e => e.TargetVertexId == vertex.Id))
            {
                if (_dataChannels.TryGetValue(edge.Id, out var channel))
                {
                    channels.Add(channel);
                }
            }
            
            return channels;
        }

        private static async Task RunSourceUsingReflection(object sourceInstance, LocalSourceContext sourceContext)
        {
            // Find and invoke Run method using reflection
            var runMethod = sourceInstance.GetType().GetMethod("Run");
            if (runMethod != null)
            {
                var parameters = runMethod.GetParameters();
                if (parameters.Length == 1)
                {
                    runMethod.Invoke(sourceInstance, new object[] { sourceContext });
                }
            }
            
            await Task.CompletedTask;
        }

        private async Task ProcessSinkData(object sinkInstance, List<ConcurrentQueue<object>> inputChannels, ISinkContext sinkContext, CancellationToken cancellationToken, string sinkId)
        {
            var invokeMethod = sinkInstance.GetType().GetMethod("Invoke");
            if (invokeMethod == null) return;

            // Process all data from input channels with back pressure handling
            await ProcessSinkDataLoop(sinkInstance, inputChannels, sinkContext, invokeMethod, cancellationToken, sinkId);
        }

        private async Task ProcessSinkDataLoop(object sinkInstance, List<ConcurrentQueue<object>> inputChannels, ISinkContext sinkContext, MethodInfo invokeMethod, CancellationToken cancellationToken, string sinkId)
        {
            var processed = 0;
            var noDataCount = 0;
            const int maxNoDataIterations = 1440000; // Exit after ~2 hours of no data (1440000 * 5ms)
            
            Console.WriteLine($"[LocalStreamExecutor] 🚀 SINK STARTING: Sink starting data processing loop with {inputChannels.Count} input channels...");
            
            while (!cancellationToken.IsCancellationRequested)
            {
                var hasData = ProcessSinkChannels(inputChannels, sinkInstance, sinkContext, invokeMethod, ref processed, sinkId);
                
                if (hasData)
                {
                    noDataCount = 0; // Reset counter when data is found
                    LogSinkProgress(inputChannels, processed);
                }
                else if (await HandleSinkNoData(inputChannels, processed, ++noDataCount, maxNoDataIterations, cancellationToken))
                {
                    break;
                }
            }
            
            Console.WriteLine($"[LocalStreamExecutor] ✅ SINK COMPLETED: Sink completed processing {processed} total records");
        }

        private static void LogSinkProgress(List<ConcurrentQueue<object>> inputChannels, int processed)
        {
            if (processed % 10000 == 0 && processed > 0)
            {
                var totalQueueSize = inputChannels.Sum(c => c.Count);
                Console.WriteLine($"[LocalStreamExecutor] Sink processed {processed} records, queue size: {totalQueueSize}");
            }
        }

        private static async Task<bool> HandleSinkNoData(List<ConcurrentQueue<object>> inputChannels, int processed, int noDataCount, int maxNoDataIterations, CancellationToken cancellationToken)
        {
            LogSinkWaitingStatus(inputChannels, processed, noDataCount);
            
            if (noDataCount >= maxNoDataIterations)
            {
                Console.WriteLine($"[LocalStreamExecutor] 🛑 SINK TIMEOUT: Sink stopping after processing {processed} records (no more data available after 2 hours)");
                return true;
            }
            
            await Task.Delay(5, cancellationToken);
            return false;
        }

        private static void LogSinkWaitingStatus(List<ConcurrentQueue<object>> inputChannels, int processed, int noDataCount)
        {
            if (noDataCount % 60000 == 0) // Every 5 minutes
            {
                var totalQueueSize = inputChannels.Sum(c => c.Count);
                Console.WriteLine($"[LocalStreamExecutor] 🔍 SINK WAITING: No data for {noDataCount * 5}ms ({noDataCount * 5 / 60000:F1} min). Processed: {processed}, Queue size: {totalQueueSize}");
            }
            
            if (noDataCount <= 10 || noDataCount % 1000 == 0)
            {
                var totalQueueSize = inputChannels.Sum(c => c.Count);
                Console.WriteLine($"[LocalStreamExecutor] 🔍 SINK DEBUG: No data iteration {noDataCount}, processed: {processed}, input queue size: {totalQueueSize}");
            }
        }

        private bool ProcessSinkChannels(List<ConcurrentQueue<object>> inputChannels, object sinkInstance, ISinkContext sinkContext, MethodInfo invokeMethod, ref int processed, string sinkId)
        {
            var hasData = false;
            
            // Monitor queue sizes for back pressure detection
            var totalQueueSize = inputChannels.Sum(c => c.Count);
            var maxQueueCapacity = inputChannels.Count * 1000; // Assume 1000 capacity per channel
            _backPressureDetector.RecordQueueSize(sinkId, totalQueueSize, maxQueueCapacity);
            
            // Apply throttling if back pressure is detected
            if (_backPressureDetector.ShouldThrottle(sinkId))
            {
                var throttleDelay = _backPressureDetector.GetThrottleDelayMs();
                if (throttleDelay > 0)
                {
                    _logger?.LogDebug("Applying back pressure throttling for sink {SinkId}: {DelayMs}ms", sinkId, throttleDelay);
                    Thread.Sleep(Math.Min(throttleDelay, 100)); // Cap at 100ms to prevent excessive delays
                }
            }
            
            foreach (var channel in inputChannels)
            {
                if (channel.TryDequeue(out var item))
                {
                    invokeMethod.Invoke(sinkInstance, new object[] { item!, sinkContext });
                    processed++;
                    hasData = true;
                    
                    // Log progress for large volumes
                    if (processed % 100000 == 0)
                    {
                        Console.WriteLine($"[LocalStreamExecutor] Sink processed {processed} records (pressure: {_backPressureDetector.GetOverallPressureLevel():F2})");
                    }
                }
            }
            
            return hasData;
        }

        private async Task ProcessOperatorData(object operatorInstance, List<ConcurrentQueue<object>> inputChannels, List<ConcurrentQueue<object>> outputChannels, CancellationToken cancellationToken, string operatorId)
        {
            // Find the appropriate method to invoke (Map, Filter, etc.)
            var mapMethod = operatorInstance.GetType().GetMethod("Map");
            if (mapMethod == null) return;

            await ProcessOperatorDataLoop(operatorInstance, inputChannels, outputChannels, mapMethod, cancellationToken, operatorId);
        }

        private async Task ProcessOperatorDataLoop(object operatorInstance, List<ConcurrentQueue<object>> inputChannels, List<ConcurrentQueue<object>> outputChannels, MethodInfo mapMethod, CancellationToken cancellationToken, string operatorId)
        {
            var processed = 0;
            var noDataCount = 0;
            const int maxNoDataIterations = 1440000; // Exit after ~2 hours of no data (1440000 * 5ms)
            
            Console.WriteLine($"[LocalStreamExecutor] Operator starting data processing loop...");
            
            while (!cancellationToken.IsCancellationRequested)
            {
                MonitorAndApplyBackPressure(inputChannels, outputChannels, operatorId);
                
                var hasData = ProcessOperatorChannels(inputChannels, outputChannels, operatorInstance, mapMethod, ref processed, operatorId);
                
                if (!hasData)
                {
                    if (await HandleNoDataScenario(inputChannels, outputChannels, operatorId, processed, noDataCount, maxNoDataIterations, cancellationToken))
                    {
                        break;
                    }
                    noDataCount++;
                }
                else
                {
                    HandleDataProcessingProgress(inputChannels, outputChannels, processed);
                    noDataCount = 0;
                }
            }
            
            Console.WriteLine($"[LocalStreamExecutor] Operator completed processing {processed} total records");
        }

        private void MonitorAndApplyBackPressure(List<ConcurrentQueue<object>> inputChannels, List<ConcurrentQueue<object>> outputChannels, string operatorId)
        {
            var inputQueueSize = inputChannels.Sum(c => c.Count);
            var outputQueueSize = outputChannels.Sum(c => c.Count);
            var maxInputCapacity = inputChannels.Count * 1000;
            var maxOutputCapacity = outputChannels.Count * 1000;
            
            _backPressureDetector.RecordQueueSize($"{operatorId}-input", inputQueueSize, maxInputCapacity);
            _backPressureDetector.RecordQueueSize($"{operatorId}-output", outputQueueSize, maxOutputCapacity);
            
            if (_backPressureDetector.ShouldThrottle(operatorId))
            {
                var throttleDelay = _backPressureDetector.GetThrottleDelayMs();
                if (throttleDelay > 0)
                {
                    _logger?.LogDebug("Applying back pressure throttling for operator {OperatorId}: {DelayMs}ms", operatorId, throttleDelay);
                    Thread.Sleep(Math.Min(throttleDelay, 50)); // Cap at 50ms for operators
                }
            }
        }

        private async Task<bool> HandleNoDataScenario(List<ConcurrentQueue<object>> inputChannels, List<ConcurrentQueue<object>> outputChannels, string operatorId, int processed, int noDataCount, int maxNoDataIterations, CancellationToken cancellationToken)
        {
            if (noDataCount % 60000 == 0) // Every 5 minutes
            {
                var inputQueueSize = inputChannels.Sum(c => c.Count);
                var outputQueueSize = outputChannels.Sum(c => c.Count);
                Console.WriteLine($"[LocalStreamExecutor] Operator waiting for data: {noDataCount * 5}ms. Processed: {processed}, Input queue: {inputQueueSize}, Output queue: {outputQueueSize}, Pressure: {_backPressureDetector.GetOverallPressureLevel():F2}");
            }
            
            if (noDataCount >= maxNoDataIterations)
            {
                Console.WriteLine($"[LocalStreamExecutor] Operator stopping after processing {processed} records (no more data available after 2 hours)");
                return true;
            }
            
            await Task.Delay(5, cancellationToken);
            return false;
        }

        private static void HandleDataProcessingProgress(List<ConcurrentQueue<object>> inputChannels, List<ConcurrentQueue<object>> outputChannels, int processed)
        {
            if (processed % 10000 == 0 && processed > 0)
            {
                var inputQueueSizeForLogging = inputChannels.Sum(c => c.Count);
                var outputQueueSizeForLogging = outputChannels.Sum(c => c.Count);
                Console.WriteLine($"[LocalStreamExecutor] Operator processed {processed} records, input queue: {inputQueueSizeForLogging}, output queue: {outputQueueSizeForLogging}");
            }
        }

        private static bool ProcessOperatorChannels(List<ConcurrentQueue<object>> inputChannels, List<ConcurrentQueue<object>> outputChannels, object operatorInstance, MethodInfo mapMethod, ref int processed, string operatorId)
        {
            var hasData = false;
            
            foreach (var inputChannel in inputChannels)
            {
                if (inputChannel.TryDequeue(out var item))
                {
                    // Apply transformation
                    var result = mapMethod.Invoke(operatorInstance, new object[] { item! });
                    
                    // Send to output channels
                    foreach (var outputChannel in outputChannels)
                    {
                        outputChannel.Enqueue(result!);
                    }
                    
                    processed++;
                    hasData = true;
                    
                    // Log progress for large volumes with back pressure info
                    if (processed % 100000 == 0)
                    {
                        Console.WriteLine($"[LocalStreamExecutor] Operator {operatorId} processed {processed} records");
                        Console.WriteLine($"[LocalStreamExecutor] Operator processed {processed} records");
                    }
                }
            }
            
            return hasData;
        }

        public void Cancel()
        {
            _cancellationTokenSource.Cancel();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _cancellationTokenSource?.Dispose();
                    _backPressureDetector?.Dispose();
                }
                _disposed = true;
            }
        }
    }

    internal class OperatorInstance
    {
        public JobVertex? Vertex { get; set; }
        public object? Operator { get; set; } // Renamed from OperatorInstance to Operator
        public IRuntimeContext? RuntimeContext { get; set; }
        public List<object> ChainedOperators { get; set; } = new();
    }

    internal class LocalRuntimeContext : IRuntimeContext
    {
        public string JobName { get; }
        public string TaskName { get; }
        public int IndexOfThisSubtask { get; }
        public int NumberOfParallelSubtasks { get; }
        public FlinkDotNet.Core.Abstractions.Models.JobConfiguration JobConfiguration { get; }
        public FlinkDotNet.Core.Abstractions.Storage.IStateSnapshotStore StateSnapshotStore { get; }

        private object? _currentKey;

        public LocalRuntimeContext(string taskName, string jobName)
        {
            TaskName = taskName;
            JobName = jobName;
            IndexOfThisSubtask = 0;
            NumberOfParallelSubtasks = 1;
            JobConfiguration = new FlinkDotNet.Core.Abstractions.Models.JobConfiguration();
            StateSnapshotStore = new SimpleStateSnapshotStore();
        }

        public object? GetCurrentKey() => _currentKey;
        public void SetCurrentKey(object? key) => _currentKey = key;

        public FlinkDotNet.Core.Abstractions.States.IValueState<T> GetValueState<T>(FlinkDotNet.Core.Abstractions.Models.State.ValueStateDescriptor<T> stateDescriptor)
        {
            return new SimpleValueState<T>();
        }

        public FlinkDotNet.Core.Abstractions.States.IListState<T> GetListState<T>(FlinkDotNet.Core.Abstractions.Models.State.ListStateDescriptor<T> stateDescriptor)
        {
            return new SimpleListState<T>();
        }

        public FlinkDotNet.Core.Abstractions.States.IMapState<TK, TV> GetMapState<TK, TV>(FlinkDotNet.Core.Abstractions.Models.State.MapStateDescriptor<TK, TV> stateDescriptor) where TK : notnull
        {
            return new SimpleMapState<TK, TV>();
        }
    }

    internal class LocalSourceContext : ISourceContext<string>
    {
        private readonly List<ConcurrentQueue<object>> _outputChannels;
        private long _totalCollected = 0;

        public LocalSourceContext(List<ConcurrentQueue<object>> outputChannels, CancellationToken cancellationToken)
        {
            _outputChannels = outputChannels;
        }

        public void Collect(string record)
        {
            foreach (var channel in _outputChannels)
            {
                channel.Enqueue(record);
            }
            
            var collected = Interlocked.Increment(ref _totalCollected);
            if (collected % 10000 == 0)
            {
                var totalQueueSize = _outputChannels.Sum(c => c.Count);
                Console.WriteLine($"[LocalSourceContext] Collected {collected} records, total queue size: {totalQueueSize}");
                
                // Enhanced backpressure monitoring
                if (totalQueueSize > 100000)
                {
                    Console.WriteLine($"[LocalSourceContext] WARNING: High queue backpressure detected! Queue size: {totalQueueSize}");
                }
            }
        }

        public Task CollectAsync(string record)
        {
            Collect(record);
            return Task.CompletedTask;
        }

        public void CollectWithTimestamp(string record, long timestamp)
        {
            Collect(record); // For now, ignore timestamp
        }

        public Task CollectWithTimestampAsync(string record, long timestamp)
        {
            Collect(record);
            return Task.CompletedTask;
        }

        public void EmitWatermark(FlinkDotNet.Core.Abstractions.Windowing.Watermark watermark)
        {
            // Not implemented for local execution
        }

        public long ProcessingTime => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    internal class LocalSourceContext<T> : ISourceContext<T>
    {
        private readonly List<ConcurrentQueue<object>> _outputChannels;

        public LocalSourceContext(List<ConcurrentQueue<object>> outputChannels, CancellationToken cancellationToken)
        {
            _outputChannels = outputChannels;
        }

        public void Collect(T record)
        {
            foreach (var channel in _outputChannels)
            {
                channel.Enqueue(record!);
            }
        }

        public Task CollectAsync(T record)
        {
            Collect(record);
            return Task.CompletedTask;
        }

        public void CollectWithTimestamp(T record, long timestamp)
        {
            Collect(record); // For now, ignore timestamp
        }

        public Task CollectWithTimestampAsync(T record, long timestamp)
        {
            Collect(record);
            return Task.CompletedTask;
        }

        public void EmitWatermark(FlinkDotNet.Core.Abstractions.Windowing.Watermark watermark)
        {
            // Not implemented for local execution
        }

        public long ProcessingTime => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    internal class LocalSinkContext : ISinkContext
    {
        public long CurrentProcessingTimeMillis() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    // Simple state implementations for local execution
    internal class SimpleValueState<T> : FlinkDotNet.Core.Abstractions.States.IValueState<T>
    {
        private T? _value;
        public T? Value() => _value;
        public void Update(T? value) => _value = value;
        public void Clear() => _value = default;
    }

    internal class SimpleListState<T> : FlinkDotNet.Core.Abstractions.States.IListState<T>
    {
        private readonly List<T> _list = new();
        public IEnumerable<T> GetValues() => _list;
        public IEnumerable<T> Get() => _list;
        public void Add(T value) => _list.Add(value);
        public void Update(IEnumerable<T> values) { _list.Clear(); _list.AddRange(values); }
        public void AddAll(IEnumerable<T> values) => _list.AddRange(values);
        public void Clear() => _list.Clear();
    }

    internal class SimpleMapState<TK, TV> : FlinkDotNet.Core.Abstractions.States.IMapState<TK, TV> where TK : notnull
    {
        private readonly Dictionary<TK, TV> _map = new();
        public TV GetValueForKey(TK key) => _map.TryGetValue(key, out TV? value) ? value : default!;
        public TV Get(TK key) => GetValueForKey(key);
        public void Put(TK key, TV value) => _map[key] = value;
        public void PutAll(IDictionary<TK, TV> map) { foreach (var kvp in map) _map[kvp.Key] = kvp.Value; }
        public void Remove(TK key) => _map.Remove(key);
        public bool Contains(TK key) => _map.ContainsKey(key);
        public IEnumerable<TK> Keys() => _map.Keys;
        public IEnumerable<TV> Values() => _map.Values;
        public IEnumerable<KeyValuePair<TK, TV>> Entries() => _map;
        public bool IsEmpty() => _map.Count == 0;
        public void Clear() => _map.Clear();
    }

    internal class SimpleStateSnapshotStore : FlinkDotNet.Core.Abstractions.Storage.IStateSnapshotStore
    {
        public Task<FlinkDotNet.Core.Abstractions.Storage.SnapshotHandle> StoreSnapshot(string jobId, long checkpointId, string taskManagerId, string operatorId, byte[] snapshotData)
        {
            return Task.FromResult(new FlinkDotNet.Core.Abstractions.Storage.SnapshotHandle($"local-{jobId}-{checkpointId}-{taskManagerId}-{operatorId}"));
        }
        
        public Task<byte[]?> RetrieveSnapshot(FlinkDotNet.Core.Abstractions.Storage.SnapshotHandle handle)
        {
            return Task.FromResult<byte[]?>(null);
        }
    }
}