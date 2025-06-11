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
                .SetMaxBackgroundJobs(_options.MaxBackgroundJobs);

            _columnFamilyOptions = new ColumnFamilyOptions()
                .SetWriteBufferSize(_options.WriteBufferSize)
                .SetMaxWriteBufferNumber(_options.MaxWriteBufferNumber);

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
        /// Gets the RocksDB database instance for direct access
        /// </summary>
        public RocksDb Database => _database;

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
    /// RocksDB-based snapshot store implementation - simplified to match existing interfaces
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

        public async Task<SnapshotHandle> StoreSnapshot(string jobId, long checkpointId, string taskManagerId, string operatorId, byte[] snapshotData)
        {
            try
            {
                var key = $"snapshot_{jobId}_{checkpointId}_{taskManagerId}_{operatorId}";
                _database.Put(key, snapshotData);
                
                var handle = new SnapshotHandle { Path = key };
                _logger.LogDebug("Stored snapshot: {Key}, Size: {Size} bytes", key, snapshotData.Length);
                return handle;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to store snapshot");
                throw;
            }
        }

        public async Task<byte[]?> RetrieveSnapshot(SnapshotHandle handle)
        {
            try
            {
                var data = _database.Get(handle.Path);
                _logger.LogDebug("Retrieved snapshot: {Path}", handle.Path);
                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to retrieve snapshot: {Path}", handle.Path);
                return null;
            }
        }
    }
}