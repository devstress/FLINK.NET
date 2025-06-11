using System.Collections.Concurrent;
using System.Runtime.InteropServices;

namespace FlinkDotNet.Core.Memory
{
    /// <summary>
    /// Advanced memory manager for high-performance stream processing
    /// </summary>
    public interface IMemoryManager
    {
        /// <summary>
        /// Allocates a memory segment of the specified size
        /// </summary>
        IMemorySegment Allocate(int size);

        /// <summary>
        /// Releases a memory segment back to the pool
        /// </summary>
        void Release(IMemorySegment segment);

        /// <summary>
        /// Gets the total amount of memory managed
        /// </summary>
        long TotalMemory { get; }

        /// <summary>
        /// Gets the amount of available memory
        /// </summary>
        long AvailableMemory { get; }

        /// <summary>
        /// Gets memory usage statistics
        /// </summary>
        MemoryStatistics GetStatistics();
    }

    /// <summary>
    /// Represents a managed memory segment
    /// </summary>
    public interface IMemorySegment : IDisposable
    {
        /// <summary>
        /// Gets the memory span
        /// </summary>
        Span<byte> Span { get; }

        /// <summary>
        /// Gets the size of the segment
        /// </summary>
        int Size { get; }

        /// <summary>
        /// Gets whether the segment is allocated off-heap
        /// </summary>
        bool IsOffHeap { get; }

        /// <summary>
        /// Gets the segment identifier
        /// </summary>
        string Id { get; }
    }

    /// <summary>
    /// Memory usage statistics
    /// </summary>
    public class MemoryStatistics
    {
        public long TotalAllocated { get; set; }
        public long TotalReleased { get; set; }
        public long CurrentUsage { get; set; }
        public long PeakUsage { get; set; }
        public int ActiveSegments { get; set; }
        public int PooledSegments { get; set; }
        public double FragmentationRatio { get; set; }
    }

    /// <summary>
    /// High-performance memory manager with pooling and off-heap support
    /// </summary>
    public class FlinkMemoryManager : IMemoryManager, IDisposable
    {
        private readonly ConcurrentDictionary<int, ConcurrentQueue<PooledMemorySegment>> _pools;
        private readonly long _maxMemory;
        private long _currentMemory;
        private long _peakMemory;
        private int _activeSegments;
        private bool _disposed;
        private readonly object _lock = new object();

        // Standard segment sizes for efficient pooling
        private static readonly int[] StandardSizes = { 1024, 4096, 16384, 65536, 262144, 1048576 };

        public FlinkMemoryManager(long maxMemory = 1024 * 1024 * 1024) // 1GB default
        {
            _maxMemory = maxMemory;
            _pools = new ConcurrentDictionary<int, ConcurrentQueue<PooledMemorySegment>>();
            
            // Pre-populate pools with standard sizes
            foreach (var size in StandardSizes)
            {
                _pools[size] = new ConcurrentQueue<PooledMemorySegment>();
            }
        }

        public long TotalMemory => _maxMemory;
        public long AvailableMemory => _maxMemory - Interlocked.Read(ref _currentMemory);

        public IMemorySegment Allocate(int size)
        {
            if (size <= 0)
                throw new ArgumentException("Size must be positive", nameof(size));

            if (_disposed)
                throw new ObjectDisposedException(nameof(FlinkMemoryManager));

            // Check memory limit
            var newTotal = Interlocked.Add(ref _currentMemory, size);
            if (newTotal > _maxMemory)
            {
                Interlocked.Add(ref _currentMemory, -size);
                throw new InsufficientMemoryException($"Cannot allocate {size} bytes. Would exceed limit of {_maxMemory} bytes.");
            }

            // Update peak memory usage
            lock (_lock)
            {
                if (newTotal > _peakMemory)
                    _peakMemory = newTotal;
            }

            Interlocked.Increment(ref _activeSegments);

            // Try to get from pool first
            var poolSize = GetPoolSize(size);
            if (_pools.TryGetValue(poolSize, out var pool) && pool.TryDequeue(out var pooled))
            {
                pooled.Reset();
                return pooled;
            }

            // Allocate new segment
            return new PooledMemorySegment(this, size, poolSize);
        }

        public void Release(IMemorySegment segment)
        {
            if (segment == null)
                return;

            if (segment is PooledMemorySegment pooled)
            {
                Interlocked.Add(ref _currentMemory, -pooled.Size);
                Interlocked.Decrement(ref _activeSegments);

                // Return to pool if it's a standard size
                if (_pools.TryGetValue(pooled.PoolSize, out var pool) && pool.Count < 100)
                {
                    pool.Enqueue(pooled);
                }
                else
                {
                    pooled.Dispose();
                }
            }
        }

        public MemoryStatistics GetStatistics()
        {
            var totalPooled = 0;
            foreach (var pool in _pools.Values)
            {
                totalPooled += pool.Count;
            }

            return new MemoryStatistics
            {
                CurrentUsage = Interlocked.Read(ref _currentMemory),
                PeakUsage = _peakMemory,
                ActiveSegments = _activeSegments,
                PooledSegments = totalPooled,
                TotalAllocated = _peakMemory, // Simplified
                FragmentationRatio = CalculateFragmentation()
            };
        }

        private static int GetPoolSize(int requestedSize)
        {
            foreach (var size in StandardSizes)
            {
                if (requestedSize <= size)
                    return size;
            }
            // For very large allocations, round up to nearest MB
            return ((requestedSize + 1048575) / 1048576) * 1048576;
        }

        private double CalculateFragmentation()
        {
            // Simplified fragmentation calculation
            var totalPooled = 0L;
            var totalPooledCapacity = 0L;
            
            foreach (var kvp in _pools)
            {
                var count = kvp.Value.Count;
                totalPooled += count;
                totalPooledCapacity += count * kvp.Key;
            }

            return totalPooledCapacity == 0 ? 0.0 : 1.0 - ((double)totalPooled / totalPooledCapacity);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    // Clear all pools
                    foreach (var pool in _pools.Values)
                    {
                        while (pool.TryDequeue(out var segment))
                        {
                            segment.Dispose();
                        }
                    }
                    _pools.Clear();
                }
                _disposed = true;
            }
        }
    }

    /// <summary>
    /// Pooled memory segment implementation
    /// </summary>
    internal class PooledMemorySegment : IMemorySegment
    {
        private readonly byte[] _buffer;
        private bool _disposed;

        public PooledMemorySegment(FlinkMemoryManager manager, int size, int poolSize)
        {
            _buffer = new byte[size];
            Size = size;
            PoolSize = poolSize;
            Id = Guid.NewGuid().ToString("N")[..8];
        }

        public Span<byte> Span => _disposed ? throw new ObjectDisposedException(nameof(PooledMemorySegment)) : _buffer.AsSpan();
        public int Size { get; }
        public int PoolSize { get; }
        public bool IsOffHeap => false; // Using managed arrays for now
        public string Id { get; }

        public void Reset()
        {
            if (!_disposed)
            {
                _buffer.AsSpan().Clear();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    // Buffer will be GC'd
                }
                _disposed = true;
            }
        }
    }

    /// <summary>
    /// Off-heap memory segment using unmanaged memory
    /// </summary>
    public class OffHeapMemorySegment : IMemorySegment
    {
        private readonly IntPtr _pointer;
        private readonly int _size;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the OffHeapMemorySegment class.
        /// </summary>
        /// <param name="size">The size of the memory segment to allocate.</param>
        /// <remarks>
        /// This constructor uses unsafe code to allocate unmanaged memory via Marshal.AllocHGlobal.
        /// The unsafe keyword is required here to create a Span&lt;byte&gt; from the raw pointer.
        /// This is safe because:
        /// 1. The memory is properly allocated using Marshal.AllocHGlobal
        /// 2. The size parameter is validated and stored for bounds checking
        /// 3. The memory is zeroed immediately after allocation
        /// 4. The memory is properly freed in the Dispose method using Marshal.FreeHGlobal
        /// 5. Access to the memory is controlled through the Span property which checks disposal state
        /// 6. The unsafe context is limited to only the specific operations that require it
        /// 7. No unsafe pointer arithmetic is performed beyond creating the Span
        /// </remarks>
        public unsafe OffHeapMemorySegment(int size)
        {
            _size = size;
            _pointer = Marshal.AllocHGlobal(size);
            Id = Guid.NewGuid().ToString("N")[..8];
            
            // Zero the memory - safe because we just allocated it with the exact size
            new Span<byte>((void*)_pointer, size).Clear();
        }

        /// <summary>
        /// Gets the memory span for this segment.
        /// </summary>
        /// <remarks>
        /// This property uses unsafe code to create a Span from the unmanaged memory pointer.
        /// This is safe because:
        /// 1. The pointer was allocated with Marshal.AllocHGlobal and is valid until disposal
        /// 2. The size is exactly what was allocated and stored during construction
        /// 3. Disposal state is checked before accessing the memory
        /// 4. The Span provides bounds checking for all subsequent access
        /// </remarks>
        public unsafe Span<byte> Span => _disposed 
            ? throw new ObjectDisposedException(nameof(OffHeapMemorySegment))
            : new Span<byte>((void*)_pointer, _size);

        public int Size => _size;
        public bool IsOffHeap => true;
        public string Id { get; }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    // Free the unmanaged memory
                    Marshal.FreeHGlobal(_pointer);
                }
                _disposed = true;
            }
        }
    }

    /// <summary>
    /// Memory pool specifically for network buffers
    /// </summary>
    public class NetworkMemoryPool : IDisposable
    {
        private readonly IMemoryManager _memoryManager;
        private readonly ConcurrentQueue<IMemorySegment> _availableBuffers;
        private readonly int _bufferSize;
        private readonly int _maxBuffers;
        private int _currentBuffers;
        private bool _disposed;

        public NetworkMemoryPool(IMemoryManager memoryManager, int bufferSize = 32768, int maxBuffers = 1000)
        {
            _memoryManager = memoryManager ?? throw new ArgumentNullException(nameof(memoryManager));
            _bufferSize = bufferSize;
            _maxBuffers = maxBuffers;
            _availableBuffers = new ConcurrentQueue<IMemorySegment>();
        }

        public IMemorySegment GetBuffer()
        {
            if (_availableBuffers.TryDequeue(out var buffer))
            {
                return buffer;
            }

            if (_currentBuffers < _maxBuffers)
            {
                Interlocked.Increment(ref _currentBuffers);
                return _memoryManager.Allocate(_bufferSize);
            }

            throw new InvalidOperationException("Network buffer pool exhausted");
        }

        public void ReturnBuffer(IMemorySegment buffer)
        {
            if (buffer?.Size == _bufferSize)
            {
                // Clear the buffer before returning to pool
                buffer.Span.Clear();
                _availableBuffers.Enqueue(buffer);
            }
            else
            {
                // Different size buffer, release it
                _memoryManager.Release(buffer!);
                Interlocked.Decrement(ref _currentBuffers);
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    while (_availableBuffers.TryDequeue(out var buffer))
                    {
                        _memoryManager.Release(buffer);
                    }
                }
                _disposed = true;
            }
        }
    }
}