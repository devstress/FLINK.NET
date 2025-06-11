using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FlinkDotNet.Core.Abstractions.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RocksDbSharp;

namespace FlinkDotNet.Storage.RocksDB
{
    /// <summary>
    /// Configuration options for RocksDB state backend
    /// </summary>
    public class RocksDBOptions
    {
        public string DataDirectory { get; set; } = "rocksdb-data";
        public bool CreateIfMissing { get; set; } = true;
        public int MaxBackgroundJobs { get; set; } = 4;
        public ulong WriteBufferSize { get; set; } = 64 * 1024 * 1024; // 64MB
        public int MaxWriteBufferNumber { get; set; } = 3;
        public CompressionTypeEnum CompressionType { get; set; } = CompressionTypeEnum.Lz4;
        public bool EnableStatistics { get; set; } = true;
        public long BlockCacheSize { get; set; } = 256 * 1024 * 1024; // 256MB
    }

    /// <summary>
    /// High-performance RocksDB-based state backend for production workloads
    /// </summary>
    public class RocksDBStateBackend : IStateBackend, IDisposable
    {
        private readonly RocksDBOptions _options;
        private readonly ILogger<RocksDBStateBackend> _logger;
        private readonly RocksDb _database;
        private readonly ColumnFamilyOptions _columnFamilyOptions;
        private readonly DbOptions _dbOptions;
        private readonly Dictionary<string, ColumnFamilyHandle> _columnFamilies;
        private bool _disposed;

        public IStateSnapshotStore SnapshotStore { get; }
        public string DataDirectory => _options.DataDirectory;

        public RocksDBStateBackend(IOptions<RocksDBOptions> options, ILogger<RocksDBStateBackend> logger)
        {
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _columnFamilies = new Dictionary<string, ColumnFamilyHandle>();

            // Ensure data directory exists
            Directory.CreateDirectory(_options.DataDirectory);

            // Configure RocksDB options for optimal performance
            _dbOptions = new DbOptions()
                .SetCreateIfMissing(_options.CreateIfMissing)
                .SetMaxBackgroundJobs(_options.MaxBackgroundJobs)
                .SetStatistics(true);

            _columnFamilyOptions = new ColumnFamilyOptions()
                .SetWriteBufferSize(_options.WriteBufferSize)
                .SetMaxWriteBufferNumber(_options.MaxWriteBufferNumber)
                .SetCompression(_options.CompressionType)
                .SetBlockBasedTableFactory(new BlockBasedTableOptions()
                    .SetBlockCache(Cache.CreateLru((ulong)_options.BlockCacheSize)));

            try
            {
                // Initialize RocksDB with default column family
                var columnFamilies = new ColumnFamilies
                {
                    { "default", _columnFamilyOptions }
                };

                _database = RocksDb.Open(_dbOptions, _options.DataDirectory, columnFamilies);
                _columnFamilies["default"] = _database.GetDefaultColumnFamily();

                SnapshotStore = new RocksDBSnapshotStore(_database, _logger);
                
                _logger.LogInformation("RocksDB state backend initialized at {DataDirectory}", _options.DataDirectory);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize RocksDB state backend");
                Dispose();
                throw;
            }
        }

        /// <summary>
        /// Gets or creates a column family for the specified state name
        /// </summary>
        public ColumnFamilyHandle GetOrCreateColumnFamily(string stateName)
        {
            if (_columnFamilies.TryGetValue(stateName, out var existing))
                return existing;

            try
            {
                var columnFamily = _database.CreateColumnFamily(_columnFamilyOptions, stateName);
                _columnFamilies[stateName] = columnFamily;
                _logger.LogDebug("Created column family: {StateName}", stateName);
                return columnFamily;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to create column family: {StateName}", stateName);
                throw;
            }
        }

        /// <summary>
        /// Gets the RocksDB database instance for direct access
        /// </summary>
        public RocksDb Database => _database;

        /// <summary>
        /// Gets performance statistics from RocksDB
        /// </summary>
        public string GetStatistics()
        {
            try
            {
                return _dbOptions.GetStatistics()?.ToString() ?? "Statistics not available";
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to get RocksDB statistics");
                return "Error retrieving statistics";
            }
        }

        /// <summary>
        /// Performs database compaction to optimize storage
        /// </summary>
        public async Task CompactRangeAsync(string? columnFamilyName = null)
        {
            await Task.Run(() =>
            {
                try
                {
                    if (columnFamilyName != null && _columnFamilies.TryGetValue(columnFamilyName, out var cf))
                    {
                        _database.CompactRange(null, null, cf);
                        _logger.LogInformation("Compacted column family: {ColumnFamily}", columnFamilyName);
                    }
                    else
                    {
                        _database.CompactRange(null, null);
                        _logger.LogInformation("Compacted entire database");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to compact RocksDB range");
                    throw;
                }
            });
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                try
                {
                    // Close column families
                    foreach (var cf in _columnFamilies.Values)
                    {
                        cf?.Dispose();
                    }
                    _columnFamilies.Clear();

                    // Close database and options
                    _database?.Dispose();
                    _columnFamilyOptions?.Dispose();
                    _dbOptions?.Dispose();

                    _logger.LogInformation("RocksDB state backend disposed");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error disposing RocksDB state backend");
                }
                
                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }

    /// <summary>
    /// RocksDB-based snapshot store implementation
    /// </summary>
    internal class RocksDBSnapshotStore : IStateSnapshotStore
    {
        private readonly RocksDb _database;
        private readonly ILogger _logger;

        public RocksDBSnapshotStore(RocksDb database, ILogger logger)
        {
            _database = database ?? throw new ArgumentNullException(nameof(database));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task<IStateSnapshotWriter> CreateSnapshotWriter(string jobId, long checkpointId, string operatorId, string taskId)
        {
            var snapshotId = $"{jobId}_{checkpointId}_{operatorId}_{taskId}";
            return new RocksDBSnapshotWriter(_database, snapshotId, _logger);
        }

        public async Task<IStateSnapshotReader> CreateSnapshotReader(string handle)
        {
            return new RocksDBSnapshotReader(_database, handle, _logger);
        }

        public async Task<bool> DeleteSnapshot(string handle)
        {
            try
            {
                // In RocksDB, we can delete by prefix using a snapshot prefix
                // This is a simplified implementation - in production you might want
                // to track snapshot keys separately
                var iterator = _database.NewIterator();
                var prefix = $"snapshot_{handle}_";
                var prefixBytes = System.Text.Encoding.UTF8.GetBytes(prefix);
                
                iterator.Seek(prefixBytes);
                var batch = new WriteBatch();
                
                while (iterator.Valid())
                {
                    var key = iterator.Key();
                    var keyStr = System.Text.Encoding.UTF8.GetString(key);
                    
                    if (!keyStr.StartsWith(prefix))
                        break;
                        
                    batch.Delete(key);
                    iterator.Next();
                }
                
                _database.Write(batch);
                iterator.Dispose();
                batch.Dispose();
                
                _logger.LogInformation("Deleted snapshot: {Handle}", handle);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to delete snapshot: {Handle}", handle);
                return false;
            }
        }
    }

    /// <summary>
    /// RocksDB-based snapshot writer
    /// </summary>
    internal class RocksDBSnapshotWriter : IStateSnapshotWriter
    {
        private readonly RocksDb _database;
        private readonly string _snapshotId;
        private readonly ILogger _logger;
        private readonly WriteBatch _batch;
        private bool _disposed;

        public RocksDBSnapshotWriter(RocksDb database, string snapshotId, ILogger logger)
        {
            _database = database ?? throw new ArgumentNullException(nameof(database));
            _snapshotId = snapshotId ?? throw new ArgumentNullException(nameof(snapshotId));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _batch = new WriteBatch();
        }

        public async Task WriteKeyedState(string stateName, string key, byte[] value)
        {
            var fullKey = $"snapshot_{_snapshotId}_{stateName}_{key}";
            var keyBytes = System.Text.Encoding.UTF8.GetBytes(fullKey);
            _batch.Put(keyBytes, value);
        }

        public async Task<string> Commit()
        {
            try
            {
                await Task.Run(() => _database.Write(_batch));
                _logger.LogDebug("Committed snapshot: {SnapshotId}", _snapshotId);
                return _snapshotId;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to commit snapshot: {SnapshotId}", _snapshotId);
                throw;
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (!_disposed)
            {
                _batch?.Dispose();
                _disposed = true;
            }
        }
    }

    /// <summary>
    /// RocksDB-based snapshot reader
    /// </summary>
    internal class RocksDBSnapshotReader : IStateSnapshotReader
    {
        private readonly RocksDb _database;
        private readonly string _snapshotId;
        private readonly ILogger _logger;
        private bool _disposed;

        public RocksDBSnapshotReader(RocksDb database, string snapshotId, ILogger logger)
        {
            _database = database ?? throw new ArgumentNullException(nameof(database));
            _snapshotId = snapshotId ?? throw new ArgumentNullException(nameof(snapshotId));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task<byte[]?> ReadKeyedState(string stateName, string key)
        {
            try
            {
                var fullKey = $"snapshot_{_snapshotId}_{stateName}_{key}";
                var keyBytes = System.Text.Encoding.UTF8.GetBytes(fullKey);
                return await Task.FromResult(_database.Get(keyBytes));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read keyed state: {StateName}.{Key}", stateName, key);
                return null;
            }
        }

        public async Task<Dictionary<string, byte[]>> ReadAllKeyedState(string stateName)
        {
            var result = new Dictionary<string, byte[]>();
            
            try
            {
                var iterator = _database.NewIterator();
                var prefix = $"snapshot_{_snapshotId}_{stateName}_";
                var prefixBytes = System.Text.Encoding.UTF8.GetBytes(prefix);
                
                iterator.Seek(prefixBytes);
                
                while (iterator.Valid())
                {
                    var keyBytes = iterator.Key();
                    var keyStr = System.Text.Encoding.UTF8.GetString(keyBytes);
                    
                    if (!keyStr.StartsWith(prefix))
                        break;
                    
                    var stateKey = keyStr.Substring(prefix.Length);
                    var value = iterator.Value();
                    result[stateKey] = value;
                    
                    iterator.Next();
                }
                
                iterator.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read all keyed state: {StateName}", stateName);
            }
            
            return result;
        }

        public ValueTask DisposeAsync()
        {
            _disposed = true;
            return ValueTask.CompletedTask;
        }
    }
}