using System;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Collections.Concurrent;
using System.Threading;
using NativeKafkaBridge;

namespace Flink.Net.Producer;

static class Program
{
    static void Main(string[] args)
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

        var payloads = PreGeneratePayloads(messageCount);
        var handle = NativeProducer.Init(bootstrap, topic, partitions);
        if (handle == IntPtr.Zero)
        {
            Console.WriteLine("Failed to init native producer");
            return;
        }

        const int BatchSize = 10000;
        long sent = 0;
        var sw = Stopwatch.StartNew();

        Parallel.ForEach(Partitioner.Create(0L, messageCount, BatchSize * 10), new ParallelOptions { MaxDegreeOfParallelism = producers }, range =>
        {
            ReadOnlyMemory<byte>[] batch = new ReadOnlyMemory<byte>[BatchSize];
            for (long i = range.Item1; i < range.Item2; )
            {
                int n = (int)Math.Min(BatchSize, range.Item2 - i);
                for (int j = 0; j < n; j++, i++)
                {
                    batch[j] = payloads[i];
                }
                Interlocked.Add(ref sent, NativeProducer.ProduceBatch(handle, batch[..n]));
            }
        });

        NativeProducer.Flush(handle);
        NativeProducer.Destroy(handle);
        sw.Stop();
        Console.WriteLine($"[FINISH] Total: {sent:N0} Time: {sw.Elapsed.TotalSeconds:F3}s Rate: {sent / sw.Elapsed.TotalSeconds:N0} msg/sec");
    }

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
}
