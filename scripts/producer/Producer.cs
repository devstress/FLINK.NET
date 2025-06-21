using System.Diagnostics;
using System.Security.Cryptography;
using Confluent.Kafka;

namespace Flink.Net.Producer;

static class Program
{
    static async Task Main(string[] args)
    {
        if (args.Length != 5)
        {
            Console.WriteLine("Usage: <bootstrap> <topic> <messageCount> <producers> <partitions>");
            return;
        }

        string bootstrap = args[0];
        string topic = args[1];
        long messageCount = long.Parse(args[2]);
        int producers = int.Parse(args[3]);

        if (!OperatingSystem.IsWindows())
            PrintUlimit();

        await PreheatPartitions(bootstrap, topic);
        var payloads = PreGeneratePayloads(messageCount);
        var perProducerCounter = new long[producers];

        var sw = Stopwatch.StartNew();
        var progressTask = TrackProgress(perProducerCounter, messageCount, sw);

        var tasks = new Task[producers];
        for (int i = 0; i < producers; i++)
        {
            int id = i;
            tasks[i] = ProduceMessages(id, bootstrap, topic, messageCount, producers, payloads, perProducerCounter);
        }

        await Task.WhenAll(tasks);
        sw.Stop();
        await progressTask;

        long finalSent = perProducerCounter.Sum();
        Console.WriteLine($"\n[FINISH] Total: {finalSent:N0} Time: {sw.Elapsed.TotalSeconds:F3}s Rate: {finalSent / sw.Elapsed.TotalSeconds:N0} msg/sec");
    }

    static async Task ProduceMessages(int id, string bootstrap, string topic, long messageCount, int producers, byte[][] payloads, long[] counter)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            EnableIdempotence = true,  // MANDATORY per requirements
            Acks = Acks.All,  // Required when EnableIdempotence = true
            
            // ULTRA-HIGH PERFORMANCE CONFIG for >1M msg/sec target
            LingerMs = 10,  // Higher linger for better batching (trade small latency for throughput)
            BatchSize = 64 * 1024 * 1024,  // 64MB batches for maximum throughput
            CompressionType = CompressionType.Lz4,  // LZ4 is faster than Zstd for high throughput
            QueueBufferingMaxKbytes = 128 * 1024,  // 128MB buffer (increased from 2MB)
            QueueBufferingMaxMessages = 10_000_000,  // 10M message capacity
            
            // NETWORK OPTIMIZATIONS
            SocketSendBufferBytes = 100_000_000,  // 100MB send buffer (max allowed)
            SocketReceiveBufferBytes = 100_000_000,  // 100MB receive buffer (max allowed) 
            SocketNagleDisable = true,  // Disable Nagle's algorithm for lower latency
            SocketKeepaliveEnable = true,
            SocketTimeoutMs = 60000,  // 60s timeout to prevent drops under load
            
            // RETRY OPTIMIZATIONS
            MessageSendMaxRetries = 3,  // Reduced retries for speed (was 10)
            RetryBackoffMs = 50,  // Faster retry (was 100ms)
            RequestTimeoutMs = 5000,  // 5s request timeout
            
            // PERFORMANCE OPTIMIZATIONS
            EnableDeliveryReports = true,  // Enable for better error tracking
            ConnectionsMaxIdleMs = 600000,  // 10 minutes
            ClientId = $"ultra-producer-{id}",
            
            // METADATA OPTIMIZATIONS
            MetadataMaxAgeMs = 300000,  // 5 minutes metadata refresh
            TopicMetadataRefreshIntervalMs = 60000  // 1 minute topic refresh
        };

        using var producer = new ProducerBuilder<long, byte[]>(config)
            .SetKeySerializer(Serializers.Int64)
            .SetValueSerializer(Serializers.ByteArray)
            .Build();

        long sliceStart = id * messageCount / producers;
        long sliceEnd = (id + 1) * messageCount / producers;

        // ULTRA-FAST ASYNC BATCH MODE for maximum throughput
        var tasks = new List<Task>(8192);  // Batch async operations
        
        for (long i = sliceStart; i < sliceEnd; i++)
        {
            var msg = new Message<long, byte[]> { Key = i, Value = payloads[i] };
            
            // Async produce for maximum speed
            var task = producer.ProduceAsync(topic, msg);
            tasks.Add(task);
            
            // Process in batches to avoid overwhelming memory
            if (tasks.Count >= 8192)
            {
                await Task.WhenAll(tasks);
                // Count completed tasks
                Interlocked.Add(ref counter[id], tasks.Count);
                tasks.Clear();
            }
        }
        
        // Handle remaining tasks
        if (tasks.Count > 0)
        {
            await Task.WhenAll(tasks);
            Interlocked.Add(ref counter[id], tasks.Count);
        }

        // Final flush to ensure all messages are sent
        producer.Flush(TimeSpan.FromSeconds(30));  // Longer flush timeout for large batches
    }

    static Task TrackProgress(long[] counters, long target, Stopwatch sw) => Task.Run(() =>
    {
        while (true)
        {
            long totalSent = counters.Sum();
            double rate = totalSent / Math.Max(sw.Elapsed.TotalSeconds, 1);
            Console.Write($"\r[PROGRESS] Sent={totalSent:N0}  Rate={rate:N0} msg/sec");

            if (totalSent >= target && !sw.IsRunning)
                break;

            Thread.Sleep(100);
        }
    });

    static byte[][] PreGeneratePayloads(long total)
    {
        var payloads = new byte[total][];
        long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        Parallel.For(0, (int)total, i =>
        {
            var buffer = new byte[32];
            BitConverter.TryWriteBytes(buffer.AsSpan(0, 8), i);
            BitConverter.TryWriteBytes(buffer.AsSpan(8, 8), timestamp);
            RandomNumberGenerator.Fill(buffer.AsSpan(16, 16));
            payloads[i] = buffer;
        });

        return payloads;
    }

    static async Task PreheatPartitions(string bootstrap, string topic)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = bootstrap
        }).Build();

        // Wait for topic metadata and partition leaders - optimized for speed
        int retries = 5;  // Reduced retries for faster startup
        var partitions = 0;
        while (retries-- > 0)
        {
            try
            {
                var metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(5));
                var topicMeta = metadata.Topics.FirstOrDefault(t => t.Topic == topic);

                if (topicMeta != null && topicMeta.Partitions.All(p => p.Leader != -1))
                {
                    Console.WriteLine($"✅ Kafka topic '{topic}' ready with {topicMeta.Partitions.Count} partitions.");
                    partitions = topicMeta.Partitions.Count;
                    if (partitions > 0)
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⏳ Metadata check attempt {5-retries}: {ex.Message}");
            }

            Console.WriteLine("⏳ Waiting for Kafka topic and partition leaders...");
            Thread.Sleep(2000);  // 2s wait between retries
        }

        // Fast preheater with ultra-optimized config matching main producer
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            EnableIdempotence = true,  // MANDATORY
            Acks = Acks.All,  // Required when EnableIdempotence = true
            LingerMs = 0,  // No linger for preheating - send immediately
            BatchSize = 1024 * 1024,  // 1MB batch
            CompressionType = CompressionType.Lz4,  // Match main config
            QueueBufferingMaxKbytes = 8 * 1024,  // 8MB for preheating
            ClientId = "ultra-preheater",
            MessageSendMaxRetries = 1,  // Single retry for speed
            RetryBackoffMs = 50,
            EnableDeliveryReports = false,  // Consistent with main config
            RequestTimeoutMs = 5000  // 5s timeout
        };

        using var producer = new ProducerBuilder<Null, byte[]>(config)
            .SetKeySerializer(Serializers.Null)
            .SetValueSerializer(Serializers.ByteArray)
            .Build();

        var payload = new byte[16];
        // Send one message per partition to preheat connections and establish idempotence
        var preheatTasks = new List<Task>();
        for (int i = 0; i < Math.Min(partitions, 20); i++)  // Limit to 20 partitions for speed
        {
            var tp = new TopicPartition(topic, new Partition(i));
            var task = producer.ProduceAsync(tp, new Message<Null, byte[]> { Value = payload });
            preheatTasks.Add(task);
        }
        
        // Wait for all preheat messages to complete
        await Task.WhenAll(preheatTasks);

        Console.WriteLine($"🔥 Preheated producer for {Math.Min(partitions, 20)} partitions with ultra-optimized config.");
        producer.Flush(TimeSpan.FromSeconds(5));  // Quick flush
    }

    static void PrintUlimit()
    {
        try
        {
            using var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "/bin/sh",
                    Arguments = "-c \"ulimit -n\"",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                }
            };

            process.Start();
            string output = process.StandardOutput.ReadToEnd();
            process.WaitForExit();
            Console.WriteLine($"[ULIMIT] Open file soft limit: {output.Trim()}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ULIMIT] Failed to check ulimit: {ex.Message}");
        }
    }
}
