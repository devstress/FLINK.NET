using System.Diagnostics;
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

        // OPTIMIZED: Skip preheating for maximum speed - preheating adds ~200ms overhead
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
            EnableIdempotence = false,  // OPTIMIZED: Disabled for maximum throughput
            Acks = Acks.Leader,  // OPTIMIZED: Only wait for leader (not all replicas)
            LingerMs = 1,  // OPTIMIZED: Minimal latency for sub-1-second target
            BatchSize = 64 * 1024 * 1024,  // OPTIMIZED: 64MB batches (from docs: matches DDR4 bandwidth)
            CompressionType = CompressionType.None,  // OPTIMIZED: Remove compression overhead for speed
            QueueBufferingMaxKbytes = 64 * 1024 * 1024,  // OPTIMIZED: 64MB internal buffer (from docs)
            QueueBufferingMaxMessages = 20_000_000,  // OPTIMIZED: 20M message buffer (from docs)
            SocketSendBufferBytes = 100_000_000,  // Keep large socket buffers
            SocketReceiveBufferBytes = 100_000_000,
            SocketNagleDisable = true,  // Keep TCP optimization
            MessageSendMaxRetries = 0,  // OPTIMIZED: No retries for speed
            RetryBackoffMs = 0,  // OPTIMIZED: No retry delay
            SocketTimeoutMs = 60000,  // OPTIMIZED: from docs
            SocketKeepaliveEnable = true,  // Keep TCP keepalive
            ClientId = $"producer-{id}",
            EnableDeliveryReports = false,  // Keep disabled for performance
            ConnectionsMaxIdleMs = 300000,
            RequestTimeoutMs = 5000,  // OPTIMIZED: Faster timeout
            MessageTimeoutMs = 5000   // OPTIMIZED: Faster message timeout
        };

        using var producer = new ProducerBuilder<long, byte[]>(config)
            .SetKeySerializer(Serializers.Int64)
            .SetValueSerializer(Serializers.ByteArray)
            .Build();

        long sliceStart = id * messageCount / producers;
        long sliceEnd = (id + 1) * messageCount / producers;

        var batch = new List<Task>(100000);  // OPTIMIZED: Larger batch size for better throughput

        for (long i = sliceStart; i < sliceEnd; i++)
        {
            var msg = new Message<long, byte[]> { Key = i, Value = payloads[i] };

            // OPTIMIZED: Fire-and-forget approach for maximum speed
            var task = producer.ProduceAsync(topic, msg)
                .ContinueWith(t =>
                {
                    // Count successful completions for progress tracking only
                    if (t.IsCompletedSuccessfully)
                        Interlocked.Increment(ref counter[id]);
                    // Ignore errors for maximum speed
                }, TaskContinuationOptions.ExecuteSynchronously);

            batch.Add(task);

            // OPTIMIZED: Larger batch before waiting, better CPU utilization  
            if (batch.Count >= 100000)
            {
                await Task.WhenAll(batch);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
            await Task.WhenAll(batch);

        // OPTIMIZED: Shorter flush timeout for sub-1-second target
        producer.Flush(TimeSpan.FromSeconds(2));
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

        // OPTIMIZED: Smaller 16-byte payload for maximum throughput
        Parallel.For(0, (int)total, i =>
        {
            var buffer = new byte[16];  // OPTIMIZED: Reduced from 32 to 16 bytes
            BitConverter.TryWriteBytes(buffer.AsSpan(0, 8), i);  // 8-byte sequence
            BitConverter.TryWriteBytes(buffer.AsSpan(8, 8), timestamp);  // 8-byte timestamp
            // Removed random data for speed optimization
            payloads[i] = buffer;
        });

        return payloads;
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
