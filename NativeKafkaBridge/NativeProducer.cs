using System;
using System.Runtime.InteropServices;
using System.Buffers;

namespace NativeKafkaBridge;

public static class NativeProducer
{
    [DllImport("nativekafkabridge", CallingConvention = CallingConvention.Cdecl)]
    private static extern IntPtr nk_init_producer(string brokers, string topic, int partitions);

    [DllImport("nativekafkabridge", CallingConvention = CallingConvention.Cdecl)]
    private static extern int nk_produce_batch(IntPtr handle, IntPtr[] messages, int[] lengths, int count);

    [DllImport("nativekafkabridge", CallingConvention = CallingConvention.Cdecl)]
    private static extern void nk_flush(IntPtr handle);

    [DllImport("nativekafkabridge", CallingConvention = CallingConvention.Cdecl)]
    private static extern void nk_destroy(IntPtr handle);

    public static IntPtr Init(string brokers, string topic, int partitions) => nk_init_producer(brokers, topic, partitions);

    private static IntPtr[] _msgBuffer = new IntPtr[1024];
    private static int[] _lenBuffer = new int[1024];
    private static MemoryHandle[] _handleBuffer = new MemoryHandle[1024];

    public static unsafe int ProduceBatch(IntPtr handle, ReadOnlyMemory<byte>[] payloads)
    {
        int count = payloads.Length;
        if (count > _msgBuffer.Length)
        {
            Array.Resize(ref _msgBuffer, count);
            Array.Resize(ref _lenBuffer, count);
            Array.Resize(ref _handleBuffer, count);
        }

        for (int i = 0; i < count; i++)
        {
            _handleBuffer[i] = payloads[i].Pin();
            _msgBuffer[i] = (IntPtr)_handleBuffer[i].Pointer;
            _lenBuffer[i] = payloads[i].Length;
        }
        int produced = nk_produce_batch(handle, _msgBuffer, _lenBuffer, count);
        for (int i = 0; i < count; i++)
        {
            _handleBuffer[i].Dispose();
        }
        return produced;
    }

    public static void Flush(IntPtr handle) => nk_flush(handle);

    public static void Destroy(IntPtr handle) => nk_destroy(handle);
}
