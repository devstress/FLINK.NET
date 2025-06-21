using System;
using System.Runtime.InteropServices;
using System.Text;

namespace FlinkDotNet.Connectors.Sources.Kafka.Native
{
    /// <summary>
    /// Native wrapper for high-performance Kafka producer using librdkafka directly.
    /// Provides 1M+ messages/second throughput by using batch operations and minimal .NET overhead.
    /// </summary>
    public static class NativeKafkaProducer
    {
        private const string LibraryName = "libflinkkafka";

        // Error codes matching native library
        public const int FLINK_KAFKA_SUCCESS = 0;
        public const int FLINK_KAFKA_ERROR = -1;
        public const int FLINK_KAFKA_INVALID_PARAM = -2;
        public const int FLINK_KAFKA_PRODUCER_NOT_INITIALIZED = -3;

        // Native structures
        [StructLayout(LayoutKind.Sequential)]
        public struct NativeMessage
        {
            public IntPtr Key;
            public int KeyLen;
            public IntPtr Value;
            public int ValueLen;
            public int Partition;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct NativeProducer
        {
            public IntPtr Rk;
            public IntPtr Rkt;
            public IntPtr TopicName;
            public int Initialized;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct NativeConfig
        {
            [MarshalAs(UnmanagedType.LPStr)]
            public string BootstrapServers;
            [MarshalAs(UnmanagedType.LPStr)]
            public string TopicName;
            public int BatchSize;
            public int LingerMs;
            public int QueueBufferingMaxKbytes;
            public int QueueBufferingMaxMessages;
            public int SocketSendBufferBytes;
            public int SocketReceiveBufferBytes;
            [MarshalAs(UnmanagedType.LPStr)]
            public string CompressionType;
            public int EnableIdempotence;
            public int Acks;
            public int Retries;
            public int RequestTimeoutMs;
            public int MessageTimeoutMs;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct NativeStats
        {
            public ulong MessagesProduced;
            public ulong MessagesFailed;
            public ulong BytesProduced;
            public ulong QueueSize;
            public double CurrentRate;
        }

        // Native function imports
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int flink_kafka_producer_init(ref NativeProducer producer, ref NativeConfig config);

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int flink_kafka_produce_batch(ref NativeProducer producer, 
                                                          [In] NativeMessage[] messages, 
                                                          int messageCount);

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int flink_kafka_producer_flush(ref NativeProducer producer, int timeoutMs);

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int flink_kafka_get_stats(ref NativeProducer producer, out NativeStats stats);

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void flink_kafka_producer_destroy(ref NativeProducer producer);

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr flink_kafka_get_last_error();

        // Helper method to get last error as string
        public static string GetLastError()
        {
            IntPtr errorPtr = flink_kafka_get_last_error();
            return errorPtr != IntPtr.Zero ? Marshal.PtrToStringAnsi(errorPtr) ?? "Unknown error" : "No error";
        }
    }

    /// <summary>
    /// High-performance Kafka producer that uses native librdkafka for maximum throughput.
    /// Designed to achieve 1M+ messages/second with minimal GC pressure.
    /// </summary>
    public class HighPerformanceKafkaProducer : IDisposable
    {
        private NativeKafkaProducer.NativeProducer _producer;
        private bool _disposed = false;
        private readonly string _topic;

        /// <summary>
        /// Configuration for high-performance producer optimized for 1M+ msg/sec
        /// </summary>
        public class Config
        {
            public string BootstrapServers { get; set; } = "localhost:9092";
            public string Topic { get; set; } = "test-topic";
            
            // High-throughput settings (optimized for 1M+ msg/sec)
            public int BatchSize { get; set; } = 64 * 1024 * 1024; // 64MB batches
            public int LingerMs { get; set; } = 10; // Small linger for batching
            public int QueueBufferingMaxKbytes { get; set; } = 128 * 1024; // 128MB buffer
            public int QueueBufferingMaxMessages { get; set; } = 10_000_000; // 10M messages
            public int SocketSendBufferBytes { get; set; } = 100_000_000; // 100MB send buffer
            public int SocketReceiveBufferBytes { get; set; } = 100_000_000; // 100MB receive buffer
            public string CompressionType { get; set; } = "lz4"; // Fast compression
            public bool EnableIdempotence { get; set; } = true; // Exactly-once semantics
            public int Acks { get; set; } = -1; // Wait for all replicas (acks=all)
            public int Retries { get; set; } = 3; // Reduced retries for speed
            public int RequestTimeoutMs { get; set; } = 5000; // 5s timeout
            public int MessageTimeoutMs { get; set; } = 120000; // 2 minutes message timeout
        }

        public HighPerformanceKafkaProducer(Config config)
        {
            _topic = config.Topic ?? throw new ArgumentNullException(nameof(config.Topic));
            
            var nativeConfig = new NativeKafkaProducer.NativeConfig
            {
                BootstrapServers = config.BootstrapServers,
                TopicName = config.Topic,
                BatchSize = config.BatchSize,
                LingerMs = config.LingerMs,
                QueueBufferingMaxKbytes = config.QueueBufferingMaxKbytes,
                QueueBufferingMaxMessages = config.QueueBufferingMaxMessages,
                SocketSendBufferBytes = config.SocketSendBufferBytes,
                SocketReceiveBufferBytes = config.SocketReceiveBufferBytes,
                CompressionType = config.CompressionType,
                EnableIdempotence = config.EnableIdempotence ? 1 : 0,
                Acks = config.Acks,
                Retries = config.Retries,
                RequestTimeoutMs = config.RequestTimeoutMs,
                MessageTimeoutMs = config.MessageTimeoutMs
            };

            int result = NativeKafkaProducer.flink_kafka_producer_init(ref _producer, ref nativeConfig);
            if (result != NativeKafkaProducer.FLINK_KAFKA_SUCCESS)
            {
                throw new InvalidOperationException($"Failed to initialize native Kafka producer: {NativeKafkaProducer.GetLastError()}");
            }
        }

        /// <summary>
        /// Produce messages in batch for maximum throughput.
        /// This method is optimized for high-volume scenarios with minimal allocations.
        /// </summary>
        /// <param name="messages">Array of message data as byte arrays</param>
        /// <param name="keys">Optional array of message keys as byte arrays (can be null)</param>
        /// <returns>Number of messages successfully queued for sending</returns>
        public int ProduceBatch(byte[][] messages, byte[][]? keys = null)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(HighPerformanceKafkaProducer));

            if (messages == null || messages.Length == 0)
                return 0;

            // Pin memory for zero-copy operation
            var handles = new GCHandle[messages.Length * 2]; // For messages and optionally keys
            var nativeMessages = new NativeKafkaProducer.NativeMessage[messages.Length];
            
            try
            {
                for (int i = 0; i < messages.Length; i++)
                {
                    // Pin message data
                    handles[i * 2] = GCHandle.Alloc(messages[i], GCHandleType.Pinned);
                    nativeMessages[i].Value = handles[i * 2].AddrOfPinnedObject();
                    nativeMessages[i].ValueLen = messages[i].Length;

                    // Pin key data if provided
                    if (keys != null && i < keys.Length && keys[i] != null)
                    {
                        handles[i * 2 + 1] = GCHandle.Alloc(keys[i], GCHandleType.Pinned);
                        nativeMessages[i].Key = handles[i * 2 + 1].AddrOfPinnedObject();
                        nativeMessages[i].KeyLen = keys[i].Length;
                    }
                    else
                    {
                        nativeMessages[i].Key = IntPtr.Zero;
                        nativeMessages[i].KeyLen = 0;
                    }

                    nativeMessages[i].Partition = -1; // Use automatic partitioning
                }

                int result = NativeKafkaProducer.flink_kafka_produce_batch(ref _producer, nativeMessages, messages.Length);
                
                if (result != NativeKafkaProducer.FLINK_KAFKA_SUCCESS)
                {
                    throw new InvalidOperationException($"Batch produce failed: {NativeKafkaProducer.GetLastError()}");
                }

                return messages.Length;
            }
            finally
            {
                // Free pinned memory
                for (int i = 0; i < handles.Length; i++)
                {
                    if (handles[i].IsAllocated)
                        handles[i].Free();
                }
            }
        }

        /// <summary>
        /// Flush all pending messages. 
        /// Should be called periodically to ensure messages are sent.
        /// </summary>
        /// <param name="timeoutMs">Timeout in milliseconds</param>
        public void Flush(int timeoutMs = 10000)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(HighPerformanceKafkaProducer));

            int result = NativeKafkaProducer.flink_kafka_producer_flush(ref _producer, timeoutMs);
            if (result != NativeKafkaProducer.FLINK_KAFKA_SUCCESS)
            {
                throw new InvalidOperationException($"Flush failed: {NativeKafkaProducer.GetLastError()}");
            }
        }

        /// <summary>
        /// Get producer statistics for monitoring performance
        /// </summary>
        public NativeKafkaProducer.NativeStats GetStats()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(HighPerformanceKafkaProducer));

            int result = NativeKafkaProducer.flink_kafka_get_stats(ref _producer, out var stats);
            if (result != NativeKafkaProducer.FLINK_KAFKA_SUCCESS)
            {
                throw new InvalidOperationException($"Failed to get stats: {NativeKafkaProducer.GetLastError()}");
            }

            return stats;
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                NativeKafkaProducer.flink_kafka_producer_destroy(ref _producer);
                _disposed = true;
            }
            GC.SuppressFinalize(this);
        }

        ~HighPerformanceKafkaProducer()
        {
            Dispose();
        }
    }
}