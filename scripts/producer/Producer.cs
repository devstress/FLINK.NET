using System.Diagnostics;
using System.Security.Cryptography;
using FlinkDotNet.Connectors.Sources.Kafka.Native;

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

        await PreheatTopic(bootstrap, topic);
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
        // Create high-performance native producer configuration
        var config = new HighPerformanceKafkaProducer.Config
        {
            BootstrapServers = bootstrap,
            Topic = topic,
            
            // ULTRA-HIGH PERFORMANCE CONFIG for >1M msg/sec target
            BatchSize = 64 * 1024 * 1024,  // 64MB batches for maximum throughput
            LingerMs = 10,  // Higher linger for better batching (trade small latency for throughput)
            QueueBufferingMaxKbytes = 128 * 1024,  // 128MB buffer (increased from default)
            QueueBufferingMaxMessages = 10_000_000,  // 10M message capacity
            
            // NETWORK OPTIMIZATIONS
            SocketSendBufferBytes = 100_000_000,  // 100MB send buffer (max allowed)
            SocketReceiveBufferBytes = 100_000_000,  // 100MB receive buffer (max allowed) 
            CompressionType = "lz4",  // LZ4 is faster than Zstd for high throughput
            
            // RELIABILITY SETTINGS (required for production)
            EnableIdempotence = true,  // MANDATORY per requirements
            Acks = -1,  // Required when EnableIdempotence = true (acks=all)
            
            // PERFORMANCE OPTIMIZATIONS
            Retries = 3,  // Reduced retries for speed (was 10)
            RequestTimeoutMs = 5000,  // 5s request timeout
            MessageTimeoutMs = 120000,  // 2 minutes message timeout
        };

        using var producer = new HighPerformanceKafkaProducer(config);

        long sliceStart = id * messageCount / producers;
        long sliceEnd = (id + 1) * messageCount / producers;
        long sliceSize = sliceEnd - sliceStart;

        // ULTRA-FAST BATCH MODE for maximum throughput
        const int batchSize = 8192;  // Process in large batches
        
        for (long i = 0; i < sliceSize; i += batchSize)
        {
            int currentBatchSize = (int)Math.Min(batchSize, sliceSize - i);
            var batchMessages = new byte[currentBatchSize][];
            
            // Prepare batch without additional allocations
            for (int j = 0; j < currentBatchSize; j++)
            {
                batchMessages[j] = payloads[sliceStart + i + j];
            }
            
            // Produce batch using native high-performance producer
            int sent = producer.ProduceBatch(batchMessages);
            Interlocked.Add(ref counter[id], sent);
            
            // Flush periodically to avoid overwhelming queues
            if (i % (batchSize * 10) == 0)
            {
                producer.Flush(1000); // 1 second flush timeout
            }
        }

        // Final flush to ensure all messages are sent
        producer.Flush(30000);  // 30 second final flush timeout for large batches
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

    static async Task PreheatTopic(string bootstrap, string topic)
    {
        // For the native implementation, we'll do a simple preheat
        // by creating a producer and sending a few test messages
        Console.WriteLine($"🔥 Preheating native Kafka producer for topic '{topic}'...");
        
        var config = new HighPerformanceKafkaProducer.Config
        {
            BootstrapServers = bootstrap,
            Topic = topic,
            BatchSize = 1024 * 1024,  // 1MB batch for preheating
            LingerMs = 0,  // No linger for preheating - send immediately
            QueueBufferingMaxKbytes = 8 * 1024,  // 8MB for preheating
            EnableIdempotence = true,  // MANDATORY
            Acks = -1,  // Required when EnableIdempotence = true
            Retries = 1,  // Single retry for speed
            RequestTimeoutMs = 5000  // 5s timeout
        };

        try
        {
            using var producer = new HighPerformanceKafkaProducer(config);
            
            // Send a few preheat messages
            var preheatMessages = new byte[5][];
            for (int i = 0; i < 5; i++)
            {
                preheatMessages[i] = new byte[16];
                RandomNumberGenerator.Fill(preheatMessages[i]);
            }
            
            producer.ProduceBatch(preheatMessages);
            producer.Flush(5000);  // Quick flush
            
            Console.WriteLine($"✅ Native producer preheated successfully for topic '{topic}'.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️ Preheating failed (continuing anyway): {ex.Message}");
        }
        
        // Small delay to ensure everything is ready
        await Task.Delay(1000);
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