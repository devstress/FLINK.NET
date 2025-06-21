using System;
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Connectors.Sources.Kafka.Native;
using Microsoft.Extensions.Logging;
using System.Text;

namespace FlinkDotNet.Connectors.Sources.Kafka
{
    /// <summary>
    /// High-performance Kafka sink function that uses native librdkafka for maximum throughput.
    /// Designed to achieve 1M+ messages/second with minimal .NET overhead.
    /// </summary>
    /// <typeparam name="T">The type of records to write</typeparam>
    public class KafkaSinkFunction<T> : ISinkFunction<T>, IDisposable
    {
        private readonly HighPerformanceKafkaProducer.Config _config;
        private readonly ILogger? _logger;
        private HighPerformanceKafkaProducer? _producer;
        private readonly Func<T, byte[]> _serializer;

        public KafkaSinkFunction(
            HighPerformanceKafkaProducer.Config config,
            Func<T, byte[]> serializer,
            ILogger? logger = null)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _logger = logger;
        }

        public void Open(IRuntimeContext context)
        {
            _producer = new HighPerformanceKafkaProducer(_config);
            _logger?.LogInformation("High-performance native Kafka sink opened for topic: {Topic}", _config.Topic);
        }

        public void Invoke(T record, ISinkContext context)
        {
            if (_producer == null)
                throw new InvalidOperationException("Sink not opened");

            try
            {
                var messageBytes = _serializer(record);
                var batch = new byte[][] { messageBytes };
                _producer.ProduceBatch(batch);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to produce message to Kafka");
                throw;
            }
        }

        public void Close()
        {
            if (_producer != null)
            {
                try
                {
                    _producer.Flush(10000); // 10 second flush timeout
                    _logger?.LogInformation("Kafka sink flushed and closed");
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error during Kafka sink close");
                }
            }
        }

        public void Dispose()
        {
            Close();
            _producer?.Dispose();
            _producer = null;
        }
    }

    /// <summary>
    /// Builder for creating high-performance Kafka sink functions
    /// </summary>
    public class KafkaSinkBuilder<T>
    {
        private HighPerformanceKafkaProducer.Config? _config;
        private Func<T, byte[]>? _serializer;
        private ILogger? _logger;

        public KafkaSinkBuilder<T> BootstrapServers(string servers)
        {
            _config ??= new HighPerformanceKafkaProducer.Config();
            _config.BootstrapServers = servers;
            return this;
        }

        public KafkaSinkBuilder<T> Topic(string topic)
        {
            _config ??= new HighPerformanceKafkaProducer.Config();
            _config.Topic = topic;
            return this;
        }

        public KafkaSinkBuilder<T> ValueSerializer(Func<T, byte[]> serializer)
        {
            _serializer = serializer;
            return this;
        }

        public KafkaSinkBuilder<T> Logger(ILogger logger)
        {
            _logger = logger;
            return this;
        }

        public KafkaSinkBuilder<T> ProducerConfig(HighPerformanceKafkaProducer.Config config)
        {
            _config = config;
            return this;
        }

        public KafkaSinkFunction<T> Build()
        {
            if (_config == null)
                throw new InvalidOperationException("Producer configuration is required");
            if (string.IsNullOrEmpty(_config.Topic))
                throw new InvalidOperationException("Topic is required");
            if (_serializer == null)
                throw new InvalidOperationException("Value serializer is required");

            return new KafkaSinkFunction<T>(_config, _serializer, _logger);
        }
    }

    /// <summary>
    /// Common serializers for convenience
    /// </summary>
    public static class Serializers
    {
        public static Func<string, byte[]> Utf8 => s => Encoding.UTF8.GetBytes(s);
        public static Func<byte[], byte[]> ByteArray => b => b;
        public static Func<int, byte[]> Int32 => i => BitConverter.GetBytes(i);
        public static Func<long, byte[]> Int64 => l => BitConverter.GetBytes(l);
    }
}