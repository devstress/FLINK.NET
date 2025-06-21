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
            EnableIdempotence = true,
            Acks = Acks.All,
            LingerMs = 1,  // Reduced for faster batching
            BatchSize = 16 * 1024 * 1024,  // Increased batch size 
            CompressionType = CompressionType.Zstd,
            QueueBufferingMaxKbytes = 4 * 1024 * 1024,  // Increased queue size
            QueueBufferingMaxMessages = 2_000_000,  // Increased queue messages
            SocketSendBufferBytes = 100_000_000,  
            SocketReceiveBufferBytes = 100_000_000,
            SocketNagleDisable = true,  
            MessageSendMaxRetries = 5,  // Reduced retries for speed
            RetryBackoffMs = 50,  // Reduced backoff for speed
            SocketTimeoutMs = 30000,  
            SocketKeepaliveEnable = true,
            ClientId = $"producer-{id}",
            EnableDeliveryReports = false,  // Optimized: Disabled for performance
            ConnectionsMaxIdleMs = 300000,
            RequestTimeoutMs = 30000,  // Added for better timeout control
            MessageTimeoutMs = 300000  // Added overall message timeout
        };

        using var producer = new ProducerBuilder<long, byte[]>(config)
            .SetKeySerializer(Serializers.Int64)
            .SetValueSerializer(Serializers.ByteArray)
            .Build();

        long sliceStart = id * messageCount / producers;
        long sliceEnd = (id + 1) * messageCount / producers;

        var batch = new List<Task>(100000);  // Increased batch size for better throughput

        for (long i = sliceStart; i < sliceEnd; i++)
        {
            var msg = new Message<long, byte[]> { Key = i, Value = payloads[i] };

            // Optimized: No delivery report checking since EnableDeliveryReports = false
            var task = producer.ProduceAsync(topic, msg)
                .ContinueWith(t =>
                {
                    // Count successful completions for progress tracking
                    if (t.IsCompletedSuccessfully)
                        Interlocked.Increment(ref counter[id]);
                }, TaskContinuationOptions.ExecuteSynchronously);

            batch.Add(task);

            if (batch.Count >= 100000)  // Larger batch before waiting
            {
                await Task.WhenAll(batch);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
            await Task.WhenAll(batch);

        producer.Flush(TimeSpan.FromSeconds(30));  // Increased flush timeout
    }

    static Task TrackProgress(long[] counters, long target, Stopwatch sw) => Task.Run(() =>
    {
        long lastTotalSent = 0;
        var lastTime = sw.Elapsed;
        
        while (true)
        {
            long totalSent = counters.Sum();
            var currentTime = sw.Elapsed;
            var timeDiff = currentTime - lastTime;
            
            if (timeDiff.TotalSeconds >= 1.0)  // Update every second
            {
                double overallRate = totalSent / Math.Max(sw.Elapsed.TotalSeconds, 1);
                double instantRate = (totalSent - lastTotalSent) / Math.Max(timeDiff.TotalSeconds, 1);
                
                Console.Write($"\r[PROGRESS] Sent={totalSent:N0}  Rate={overallRate:N0} msg/sec  Instant={instantRate:N0} msg/sec");
                
                lastTotalSent = totalSent;
                lastTime = currentTime;
            }

            if (totalSent >= target && !sw.IsRunning)
                break;

            Thread.Sleep(250);  // Check more frequently for better responsiveness
        }
    });

    static byte[][] PreGeneratePayloads(long total)
    {
        Console.WriteLine($"📊 Pre-generating {total:N0} message payloads...");
        var payloads = new byte[total][];
        long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Use more parallel processing for better performance
        var partitioner = System.Collections.Concurrent.Partitioner.Create(0, (int)total, Math.Max(1, (int)total / Environment.ProcessorCount));
        
        Parallel.ForEach(partitioner, range =>
        {
            for (int i = range.Item1; i < range.Item2; i++)
            {
                var buffer = new byte[32];
                BitConverter.TryWriteBytes(buffer.AsSpan(0, 8), i);
                BitConverter.TryWriteBytes(buffer.AsSpan(8, 8), timestamp);
                RandomNumberGenerator.Fill(buffer.AsSpan(16, 16));
                payloads[i] = buffer;
            }
        });

        Console.WriteLine($"✅ Payload generation completed");
        return payloads;
    }

    static async Task PreheatPartitions(string bootstrap, string topic)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = bootstrap
        }).Build();

        // Wait for topic metadata and partition leaders
        int retries = 10;
        var partitions = 0;
        while (retries-- > 0)
        {
            var metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(3));
            var topicMeta = metadata.Topics.FirstOrDefault(t => t.Topic == topic);

            if (topicMeta != null && topicMeta.Partitions.All(p => p.Leader != -1))
            {
                Console.WriteLine($"✅ Kafka topic '{topic}' is ready with {topicMeta.Partitions.Count} partitions.");
                partitions = topicMeta.Partitions.Count;
                if (partitions > 0)
                    break;
            }

            Console.WriteLine("⏳ Waiting for Kafka topic and partition leaders...");
            Thread.Sleep(1000);
        }

        // Real preheater to trigger idempotence init with optimized config
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            EnableIdempotence = true,
            Acks = Acks.All,  // Required when EnableIdempotence = true
            LingerMs = 1,  // Faster for preheating
            BatchSize = 1024 * 1024,
            CompressionType = CompressionType.Zstd,
            ClientId = "preheater",
            MessageSendMaxRetries = 3,
            RetryBackoffMs = 100,
            EnableDeliveryReports = false  // Consistent with main config
        };

        using var producer = new ProducerBuilder<Null, byte[]>(config)
            .SetKeySerializer(Serializers.Null)
            .SetValueSerializer(Serializers.ByteArray)
            .Build();

        var payload = new byte[16];
        for (int i = 0; i < partitions; i++)
        {
            var tp = new TopicPartition(topic, new Partition(i));
            await producer.ProduceAsync(tp, new Message<Null, byte[]> { Value = payload });
        }

        Console.WriteLine("🔥 Preheated producer with real config and metadata.");
        producer.Flush();
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
