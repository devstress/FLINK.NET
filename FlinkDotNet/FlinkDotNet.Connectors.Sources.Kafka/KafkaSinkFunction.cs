using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using FlinkDotNet.Core.Abstractions.Sinks;
using Microsoft.Extensions.Logging;

namespace FlinkDotNet.Connectors.Sources.Kafka
{
    /// <summary>
    /// Kafka sink function that supports exactly-once semantics via transactions
    /// </summary>
    /// <typeparam name="T">The type of records to write</typeparam>
    public class KafkaSinkFunction<T> : ISinkFunction<T>, ITwoPhaseCommitSink<T>
    {
        private readonly ProducerConfig _producerConfig;
        private readonly string _topic;
        private readonly ISerializer<T> _valueSerializer;
        private readonly ILogger? _logger;
        private IProducer<Null, T>? _producer;
        private bool _transactional;

        public KafkaSinkFunction(
            ProducerConfig producerConfig,
            string topic,
            ISerializer<T> valueSerializer,
            ILogger? logger = null)
        {
            _producerConfig = producerConfig ?? throw new ArgumentNullException(nameof(producerConfig));
            _topic = topic ?? throw new ArgumentNullException(nameof(topic));
            _valueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));
            _logger = logger;
            _transactional = !string.IsNullOrEmpty(_producerConfig.TransactionalId);
        }

        public async Task Open(ISinkContext context)
        {
            var producerBuilder = new ProducerBuilder<Null, T>(_producerConfig)
                .SetValueSerializer(_valueSerializer)
                .SetErrorHandler((_, e) => _logger?.LogError("Kafka producer error: {Error}", e.Reason));

            _producer = producerBuilder.Build();

            if (_transactional)
            {
                _producer.InitTransactions(TimeSpan.FromSeconds(30));
                _logger?.LogInformation("Kafka producer initialized with transactions");
            }

            _logger?.LogInformation("Kafka sink opened for topic: {Topic}", _topic);
        }

        public async Task Invoke(T value, ISinkContext context)
        {
            if (_producer == null)
                throw new InvalidOperationException("Sink not opened");

            try
            {
                var message = new Message<Null, T>
                {
                    Value = value,
                    Timestamp = new Timestamp(context.CurrentEventTime ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
                };

                var deliveryResult = await _producer.ProduceAsync(_topic, message);
                _logger?.LogDebug("Message delivered to {TopicPartition} at offset {Offset}", 
                    deliveryResult.TopicPartition, deliveryResult.Offset);
            }
            catch (ProduceException<Null, T> ex)
            {
                _logger?.LogError(ex, "Failed to produce message to Kafka topic {Topic}: {Error}", _topic, ex.Error.Reason);
                throw;
            }
        }

        public async Task Close()
        {
            if (_producer != null)
            {
                _producer.Flush(TimeSpan.FromSeconds(10));
                _producer.Dispose();
                _producer = null;
                _logger?.LogInformation("Kafka sink closed");
            }
        }

        // ITwoPhaseCommitSink implementation for exactly-once semantics
        public async Task<string> BeginTransaction(long checkpointId)
        {
            if (!_transactional || _producer == null)
                return string.Empty;

            try
            {
                _producer.BeginTransaction();
                _logger?.LogDebug("Kafka transaction begun for checkpoint {CheckpointId}", checkpointId);
                return $"kafka-txn-{checkpointId}";
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to begin Kafka transaction for checkpoint {CheckpointId}", checkpointId);
                throw;
            }
        }

        public async Task PreCommit(string transactionHandle)
        {
            // For Kafka, pre-commit doesn't require specific action
            // The transaction is prepared when we call CommitTransaction
            _logger?.LogDebug("Kafka pre-commit for transaction {TransactionHandle}", transactionHandle);
        }

        public async Task Commit(string transactionHandle)
        {
            if (!_transactional || _producer == null)
                return;

            try
            {
                _producer.CommitTransaction();
                _logger?.LogDebug("Kafka transaction committed: {TransactionHandle}", transactionHandle);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to commit Kafka transaction {TransactionHandle}", transactionHandle);
                throw;
            }
        }

        public async Task Abort(string transactionHandle)
        {
            if (!_transactional || _producer == null)
                return;

            try
            {
                _producer.AbortTransaction();
                _logger?.LogDebug("Kafka transaction aborted: {TransactionHandle}", transactionHandle);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to abort Kafka transaction {TransactionHandle}", transactionHandle);
                throw;
            }
        }

        public void Dispose()
        {
            Close().GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// Builder for creating Kafka sink functions with fluent API
    /// </summary>
    public class KafkaSinkBuilder<T>
    {
        private ProducerConfig? _producerConfig;
        private string? _topic;
        private ISerializer<T>? _valueSerializer;
        private ILogger? _logger;

        public KafkaSinkBuilder<T> BootstrapServers(string servers)
        {
            _producerConfig ??= new ProducerConfig();
            _producerConfig.BootstrapServers = servers;
            return this;
        }

        public KafkaSinkBuilder<T> Topic(string topic)
        {
            _topic = topic;
            return this;
        }

        public KafkaSinkBuilder<T> ValueSerializer(ISerializer<T> serializer)
        {
            _valueSerializer = serializer;
            return this;
        }

        public KafkaSinkBuilder<T> EnableTransactions(string transactionalId)
        {
            _producerConfig ??= new ProducerConfig();
            _producerConfig.TransactionalId = transactionalId;
            _producerConfig.EnableIdempotence = true;
            return this;
        }

        public KafkaSinkBuilder<T> Logger(ILogger logger)
        {
            _logger = logger;
            return this;
        }

        public KafkaSinkBuilder<T> ProducerConfig(ProducerConfig config)
        {
            _producerConfig = config;
            return this;
        }

        public KafkaSinkFunction<T> Build()
        {
            if (_producerConfig == null)
                throw new InvalidOperationException("Producer configuration is required");
            if (string.IsNullOrEmpty(_topic))
                throw new InvalidOperationException("Topic is required");
            if (_valueSerializer == null)
                throw new InvalidOperationException("Value serializer is required");

            return new KafkaSinkFunction<T>(_producerConfig, _topic, _valueSerializer, _logger);
        }
    }
}