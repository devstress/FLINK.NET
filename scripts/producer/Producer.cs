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
        int partitions = int.Parse(args[4]);

        if (!OperatingSystem.IsWindows())
            PrintUlimit();

        PreheatPartitions(bootstrap, topic, partitions);
        var payloads = PreGeneratePayloads(messageCount);
        var perProducerCounter = new long[producers];

        var sw = Stopwatch.StartNew();
        var progressTask = TrackProgress(perProducerCounter, messageCount, sw);

        var tasks = new Task[producers];
        for (int i = 0; i < producers; i++)
        {
            int id = i;
            tasks[i] = Task.Run(() => ProduceMessages(id, bootstrap, topic, messageCount, producers, payloads, perProducerCounter));
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
            LingerMs = 2,
            BatchSize = 2 * 1024 * 1024,
            CompressionType = CompressionType.Zstd,
            QueueBufferingMaxKbytes = 1024 * 1024,
            QueueBufferingMaxMessages = 100_000_000,
            SocketSendBufferBytes = 100000_000,
            MessageSendMaxRetries = 15,
            RetryBackoffMs = 25,
            SocketTimeoutMs = 15000,
            SocketKeepaliveEnable = true,
            ClientId = $"producer-{id}",
            EnableDeliveryReports = false
        };

        using var producer = new ProducerBuilder<long, byte[]>(config)
            .SetKeySerializer(Serializers.Int64)
            .SetValueSerializer(Serializers.ByteArray)
            .Build();

        long sliceStart = id * messageCount / producers;
        long sliceEnd = (id + 1) * messageCount / producers;

        var batch = new List<Task>(100_000);

        for (long i = sliceStart; i < sliceEnd; i++)
        {
            var msg = new Message<long, byte[]> { Key = i, Value = payloads[i] };
            batch.Add(producer.ProduceAsync(topic, msg));
            counter[id]++;

            if (batch.Count >= 100_000)
            {
                await Task.WhenAll(batch);
                batch.Clear();
            }
        }

        await Task.WhenAll(batch);
        producer.Flush(TimeSpan.FromSeconds(10));
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

    static void PreheatPartitions(string bootstrap, string topic, int partitions)
    {
        var config = new ProducerConfig { BootstrapServers = bootstrap };
        using var producer = new ProducerBuilder<Null, byte[]>(config)
            .SetValueSerializer(Serializers.ByteArray)
            .Build();

        var payload = new byte[16];
        for (int pid = 0; pid < partitions; pid++)
        {
            var tp = new TopicPartition(topic, new Partition(pid));
            producer.Produce(tp, new Message<Null, byte[]> { Value = payload });
        }

        producer.Flush(TimeSpan.FromSeconds(10));
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
