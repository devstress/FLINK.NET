using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using FlinkDotNet.Core.Abstractions.Sources;
using Microsoft.Extensions.Logging;

namespace FlinkDotNet.Connectors.Sources.Kafka
{
    /// <summary>
    /// Kafka source function that implements the unified source API for both bounded and unbounded reading.
    /// Supports stream processing with event time and watermarks.
    /// </summary>
    /// <typeparam name="T">The type of records to produce</typeparam>
    public class KafkaSourceFunction<T> : IUnifiedSource<T>
    {
        private readonly ConsumerConfig _consumerConfig;
        private readonly List<string> _topics;
        private readonly IDeserializer<T> _valueDeserializer;
        private readonly bool _isBounded;
        private readonly TimeSpan? _readTimeout;
        private readonly ILogger? _logger;
        private IConsumer<Ignore, T>? _consumer;
        private bool _isRunning;

        public KafkaSourceFunction(
            ConsumerConfig consumerConfig,
            List<string> topics,
            IDeserializer<T> valueDeserializer,
            bool isBounded = false,
            TimeSpan? readTimeout = null,
            ILogger? logger = null)
        {
            _consumerConfig = consumerConfig ?? throw new ArgumentNullException(nameof(consumerConfig));
            _topics = topics ?? throw new ArgumentNullException(nameof(topics));
            _valueDeserializer = valueDeserializer ?? throw new ArgumentNullException(nameof(valueDeserializer));
            _isBounded = isBounded;
            _readTimeout = readTimeout;
            _logger = logger;
        }

        public bool IsBounded => _isBounded;

        public void Run(ISourceContext<T> context)
        {
            RunAsync(context, CancellationToken.None).GetAwaiter().GetResult();
        }

        public async Task RunAsync(ISourceContext<T> context, CancellationToken cancellationToken)
        {
            _isRunning = true;
            
            var consumerBuilder = new ConsumerBuilder<Ignore, T>(_consumerConfig)
                .SetValueDeserializer(_valueDeserializer)
                .SetErrorHandler((_, e) => _logger?.LogError("Kafka consumer error: {Error}", e.Reason));

            _consumer = consumerBuilder.Build();
            
            try
            {
                _consumer.Subscribe(_topics);
                _logger?.LogInformation("Kafka consumer subscribed to topics: {Topics}", string.Join(", ", _topics));

                while (_isRunning && !cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = _consumer.Consume(_readTimeout ?? TimeSpan.FromMilliseconds(100));
                        
                        if (consumeResult?.Message != null)
                        {
                            var timestamp = consumeResult.Message.Timestamp.UnixTimestampMs;
                            
                            // Emit the record with event time if available
                            if (timestamp > 0)
                            {
                                await context.CollectWithTimestampAsync(consumeResult.Message.Value, timestamp);
                            }
                            else
                            {
                                await context.CollectAsync(consumeResult.Message.Value);
                            }
                            
                            // For bounded mode, we might want to stop after consuming all available messages
                            if (_isBounded && consumeResult.IsPartitionEOF)
                            {
                                _logger?.LogInformation("Reached end of partition for bounded Kafka source");
                                break;
                            }
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        _logger?.LogError(ex, "Error consuming from Kafka: {Error}", ex.Error.Reason);
                        
                        // For critical errors, we should stop processing
                        if (ex.Error.IsFatal)
                        {
                            break;
                        }
                    }
                }
            }
            finally
            {
                _consumer?.Close();
                _consumer?.Dispose();
                _logger?.LogInformation("Kafka consumer closed");
            }
        }

        public void Cancel()
        {
            _isRunning = false;
            _logger?.LogInformation("Kafka source cancellation requested");
        }
    }

    /// <summary>
    /// Builder for creating Kafka source functions with fluent API
    /// </summary>
    public class KafkaSourceBuilder<T>
    {
        private ConsumerConfig? _consumerConfig;
        private List<string> _topics = new();
        private IDeserializer<T>? _valueDeserializer;
        private bool _isBounded = false;
        private TimeSpan? _readTimeout;
        private ILogger? _logger;

        public KafkaSourceBuilder<T> BootstrapServers(string servers)
        {
            _consumerConfig ??= new ConsumerConfig();
            _consumerConfig.BootstrapServers = servers;
            return this;
        }

        public KafkaSourceBuilder<T> GroupId(string groupId)
        {
            _consumerConfig ??= new ConsumerConfig();
            _consumerConfig.GroupId = groupId;
            return this;
        }

        public KafkaSourceBuilder<T> Topic(string topic)
        {
            _topics.Add(topic);
            return this;
        }

        public KafkaSourceBuilder<T> Topics(params string[] topics)
        {
            _topics.AddRange(topics);
            return this;
        }

        public KafkaSourceBuilder<T> ValueDeserializer(IDeserializer<T> deserializer)
        {
            _valueDeserializer = deserializer;
            return this;
        }

        public KafkaSourceBuilder<T> Bounded(bool bounded = true)
        {
            _isBounded = bounded;
            return this;
        }

        public KafkaSourceBuilder<T> ReadTimeout(TimeSpan timeout)
        {
            _readTimeout = timeout;
            return this;
        }

        public KafkaSourceBuilder<T> Logger(ILogger logger)
        {
            _logger = logger;
            return this;
        }

        public KafkaSourceBuilder<T> ConsumerConfig(ConsumerConfig config)
        {
            _consumerConfig = config;
            return this;
        }

        public KafkaSourceFunction<T> Build()
        {
            if (_consumerConfig == null)
                throw new InvalidOperationException("Consumer configuration is required");
            if (_topics.Count == 0)
                throw new InvalidOperationException("At least one topic must be specified");
            if (_valueDeserializer == null)
                throw new InvalidOperationException("Value deserializer is required");

            return new KafkaSourceFunction<T>(
                _consumerConfig, 
                _topics, 
                _valueDeserializer, 
                _isBounded, 
                _readTimeout,
                _logger);
        }
    }
}