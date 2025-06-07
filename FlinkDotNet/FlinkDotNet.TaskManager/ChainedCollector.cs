#nullable enable
using System;
using System.Collections.Generic;
using FlinkDotNet.Core.Abstractions.Collectors;
using FlinkDotNet.Core.Abstractions.Operators;
using FlinkDotNet.Core.Abstractions.Sinks;
// Assuming SimpleSinkContext is accessible or will be made accessible.
// If TaskExecutor.SimpleSinkContext is not public, this might cause an issue later.
// For now, we proceed as if it can be newed up.

namespace FlinkDotNet.TaskManager
{
    /// <summary>
    /// Collects records from an operator and forwards them to the next operator in a chain.
    /// </summary>
    /// <typeparam name="TIn">The type of record accepted by this collector (output from the previous operator).</typeparam>
    public class ChainedCollector<TIn> : ICollector<TIn>
    {
        private readonly object _nextOperator;
        private readonly object? _nextStepTarget; // Renamed: Can be ICollector<TNextOut> or List<CreditAwareTaskOutput> or null
        private readonly Type _nextOperatorInputType;
        // private readonly ITypeSerializer<object> _outputSerializerForNextOperator; // Placeholder for actual output serializer if needed

        /// <summary>
        /// Initializes a new instance of the <see cref="ChainedCollector{TIn}"/> class.
        /// </summary>
        /// <param name="nextOperator">The next operator instance in the chain.</param>
        /// <param name="nextStepTarget">The target for the output of the nextOperator.
        /// This can be another ChainedCollector (as ICollector<object>), a List<CreditAwareTaskOutput>, or null.</param>
        /// <param name="nextOperatorInputType">The exact input type expected by the next operator's processing method.</param>
        // public ChainedCollector(object nextOperator, object? nextStepTarget, Type nextOperatorInputType, ITypeSerializer<object> outputSerializerForNextOperator) // With serializer
        public ChainedCollector(object nextOperator, object? nextStepTarget, Type nextOperatorInputType)
        {
            _nextOperator = nextOperator ?? throw new ArgumentNullException(nameof(nextOperator));
            _nextStepTarget = nextStepTarget;
            _nextOperatorInputType = nextOperatorInputType ?? throw new ArgumentNullException(nameof(nextOperatorInputType));
            // _outputSerializerForNextOperator = outputSerializerForNextOperator ?? throw new ArgumentNullException(nameof(outputSerializerForNextOperator));
        }

        public void Collect(TIn record)
        {
            // Input validation/conversion:
            // For now, we assume TIn is directly assignable to _nextOperatorInputType.
            // A more robust solution might involve checking _nextOperatorInputType.IsAssignableFrom(typeof(TIn))
            // or even more complex type conversion logic if needed.
            // If TIn is object, this cast is trivial. If TIn is specific, this assumes it matches.
            object? inputForNextOperator = record;

            try
            {
                // Dispatch to Next Operator
                if (_nextOperator is IMapOperator<object, object> mapOperator)
                {
                    var mappedRecord = mapOperator.Map(inputForNextOperator!); // inputForNextOperator is 'record' cast to object

                    if (_nextStepTarget is ICollector<object> chainedCollectorForNextOp)
                    {
                        chainedCollectorForNextOp.Collect(mappedRecord);
                    }
                    else if (_nextStepTarget is List<CreditAwareTaskOutput> networkOutputs)
                    {
                        // Placeholder for actual serialization
                        byte[] payloadBytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(mappedRecord);
                        bool isBarrier = false;
                        long checkpointId = 0;
                        long checkpointTs = 0;

                        // Conceptual: Check if 'record' (original TIn) indicated a barrier
                        // if (record is BarrierMarker bm) { ... extract barrier info ...; isBarrier = true; }

                        var dataRecordProto = new Proto.Internal.DataRecord
                        {
                            DataPayload = Google.Protobuf.ByteString.CopyFrom(payloadBytes),
                            IsCheckpointBarrier = isBarrier,
                        };
                        if (isBarrier) {
                            dataRecordProto.BarrierPayload = new Proto.Internal.CheckpointBarrier { CheckpointId = checkpointId, CheckpointTimestamp = checkpointTs };
                        }

                        foreach (var sender in networkOutputs)
                        {
                            sender.SendRecord(dataRecordProto);
                        }
                    }
                    else if (_nextStepTarget == null)
                    {
                        // Output is dropped (e.g., map operator is the last in a chain with no network output)
                    }
                    else
                    {
                        Console.WriteLine($"ChainedCollector: IMapOperator {_nextOperator.GetType().Name}'s nextStepTarget is of unexpected type: {_nextStepTarget.GetType().Name}");
                    }
                }
                else if (_nextOperator is IFlatMapOperator<object, object> flatMapOperator)
                {
                    if (_nextStepTarget is ICollector<object> outputCollectorForFlatMap)
                    {
                        flatMapOperator.FlatMap(inputForNextOperator!, outputCollectorForFlatMap);
                    }
                    // FlatMap to network directly is more complex as FlatMap relies on the collector passed to it.
                    // To send FlatMap output to network, the outputCollectorForFlatMap would need to be a special
                    // collector that itself contains the List<CreditAwareTaskOutput> and handles serialization/SendRecord.
                    // This is a more advanced scenario than covered by simply passing List<CreditAwareTaskOutput> as _nextStepTarget for FlatMap.
                    else if (_nextStepTarget is List<CreditAwareTaskOutput>)
                    {
                         Console.WriteLine($"ChainedCollector: ERROR - FlatMapOperator {_nextOperator.GetType().Name} cannot directly use List<CreditAwareTaskOutput> as its collector. It needs an ICollector that wraps it.");
                    }
                    else if (_nextStepTarget == null && !(_nextOperator is ISinkFunction<object>))
                    {
                         Console.WriteLine($"ChainedCollector: FlatMapOperator {_nextOperator.GetType().Name} is not a sink but its output collector (_nextStepTarget) is null. Output will be dropped.");
                    }
                     else if (_nextStepTarget != null) // Not ICollector and not List, and not null
                    {
                        Console.WriteLine($"ChainedCollector: FlatMapOperator {_nextOperator.GetType().Name} received a _nextStepTarget of unexpected type: {_nextStepTarget.GetType().Name}. Expected ICollector<object>.");
                    }
                }
                else if (_nextOperator is ISinkFunction<object> sinkOperator)
                {
                    // Sinks are terminal. _nextStepTarget should be null or ignored.
                    sinkOperator.Invoke(inputForNextOperator!, new SimpleSinkContext());
                }
                else
                {
                    Console.WriteLine($"ChainedCollector: Next operator type {_nextOperator.GetType().Name} not recognized for direct invocation in chain.");
                    // Potentially throw new InvalidOperationException here.
                }
            }
            catch (InvalidCastException ice)
            {
                Console.WriteLine($"ChainedCollector: Type mismatch. Record type {typeof(TIn).Name} (value: '{record}') cannot be cast or is not compatible with next operator {_nextOperator.GetType().Name}'s expected input type {_nextOperatorInputType.Name}. Exception: {ice.Message}");
                // Potentially rethrow or handle error more gracefully.
            }
            catch (Exception ex)
            {
                 Console.WriteLine($"ChainedCollector: Error processing record for next operator {_nextOperator.GetType().Name}. Exception: {ex.Message} {ex.StackTrace}");
                 // Rethrow to propagate the error?
                 throw;
            }
        }
    }
}
#nullable disable
