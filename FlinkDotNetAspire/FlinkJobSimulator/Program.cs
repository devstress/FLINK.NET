using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using StackExchange.Redis;

namespace FlinkJobSimulator
{
    /// <summary>
    /// Simplified FlinkJobSimulator that runs ONLY as a Kafka consumer group background service.
    /// This follows the new architecture where Aspire handles all infrastructure and K8s pods.
    /// The only responsibility of FlinkJobSimulator is to consume messages from Kafka.
    /// </summary>
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("🌟 === FLINKJOBSIMULATOR STARTING ===");
            Console.WriteLine($"🌟 START TIME: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
            Console.WriteLine($"🌟 PROCESS ID: {Environment.ProcessId}");
            Console.WriteLine("🌟 SIMPLIFIED TO KAFKA CONSUMER GROUP ONLY");
            
            // Enhanced startup diagnostics
            Console.WriteLine("🔍 Environment Configuration:");
            Console.WriteLine($"  SIMULATOR_KAFKA_TOPIC: {Environment.GetEnvironmentVariable("SIMULATOR_KAFKA_TOPIC")}");
            Console.WriteLine($"  SIMULATOR_REDIS_KEY_SINK_COUNTER: {Environment.GetEnvironmentVariable("SIMULATOR_REDIS_KEY_SINK_COUNTER")}");
            Console.WriteLine($"  DOTNET_KAFKA_BOOTSTRAP_SERVERS: {Environment.GetEnvironmentVariable("DOTNET_KAFKA_BOOTSTRAP_SERVERS")}");
            Console.WriteLine($"  ConnectionStrings__kafka: {Environment.GetEnvironmentVariable("ConnectionStrings__kafka")}");
            Console.WriteLine($"  ConnectionStrings__redis: {Environment.GetEnvironmentVariable("ConnectionStrings__redis")}");
            
            // Write startup log to file for stress test monitoring
            await WriteStartupLogAsync();
            
            await RunAsKafkaConsumerGroupAsync(args);
        }
        
        /// <summary>
        /// Write startup information to log file for stress test script to monitor
        /// </summary>
        private static async Task WriteStartupLogAsync()
        {
            try
            {
                var logContent = $@"FLINKJOBSIMULATOR_STARTUP_LOG
StartTime: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC
ProcessId: {Environment.ProcessId}
Status: FlinkJobSimulatorNotStarted
Phase: INITIALIZATION
Message: FlinkJobSimulator is initializing Kafka consumer group
";
                
                // Find project root (where .git directory exists) for stress test scripts
                var projectRoot = FindProjectRoot();
                var logPath = Path.Combine(projectRoot, "flinkjobsimulator_startup.log");
                await File.WriteAllTextAsync(logPath, logContent);
                Console.WriteLine($"📝 STARTUP LOG: Written to {logPath}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️ STARTUP LOG: Failed to write startup log - {ex.Message}");
                // Don't fail startup if logging fails
            }
        }
        
        /// <summary>
        /// Update state log to show FlinkJobSimulator is running
        /// </summary>
        public static async Task WriteRunningStateLogAsync()
        {
            try
            {
                var logContent = $@"FLINKJOBSIMULATOR_STATE_LOG
UpdateTime: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC
ProcessId: {Environment.ProcessId}
Status: FlinkJobSimulatorRunning
Phase: MESSAGE_PROCESSING
Message: FlinkJobSimulator is actively running and processing messages
PreviousState: FlinkJobSimulatorNotStarted
";
                
                var projectRoot = FindProjectRoot();
                var logPath = Path.Combine(projectRoot, "flinkjobsimulator_state.log");
                await File.WriteAllTextAsync(logPath, logContent);
                Console.WriteLine($"📝 STATE LOG: FlinkJobSimulator now in RUNNING state - {logPath}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️ STATE LOG: Failed to write running state log - {ex.Message}");
            }
        }
        
        /// <summary>
        /// Find the project root directory by looking for .git directory
        /// </summary>
        private static string FindProjectRoot()
        {
            var currentDir = Directory.GetCurrentDirectory();
            var directory = new DirectoryInfo(currentDir);
            
            // Walk up the directory tree to find the .git directory
            while (directory != null)
            {
                if (Directory.Exists(Path.Combine(directory.FullName, ".git")))
                {
                    return directory.FullName;
                }
                directory = directory.Parent;
            }
            
            // If we can't find .git, fall back to current directory
            Console.WriteLine($"⚠️ Could not find project root (.git directory), using current directory: {currentDir}");
            return currentDir;
        }
        
        /// <summary>
        /// Simplified FlinkJobSimulator that runs ONLY as a Kafka consumer group.
        /// This is the new architecture where Aspire handles all infrastructure.
        /// </summary>
        private static async Task RunAsKafkaConsumerGroupAsync(string[] args)
        {
            try
            {
                Console.WriteLine("🎯 KAFKA CONSUMER GROUP MODE: FlinkJobSimulator runs as background consumer");
                
                var builder = Host.CreateApplicationBuilder(args);
                
                // Configure Redis with Apache Flink 2.0 resilient startup patterns
                builder.Services.AddSingleton<IConnectionMultiplexer>(provider =>
                {
                    var configuration = provider.GetRequiredService<IConfiguration>();
                    
                    // Apache Flink 2.0 pattern: Try multiple connection string sources
                    string? connectionString = GetRedisConnectionString(configuration);
                    
                    if (string.IsNullOrEmpty(connectionString))
                    {
                        Console.WriteLine("⚠️ REDIS: No connection string found - using fallback localhost configuration");
                        connectionString = "localhost:6379"; // Fallback for standalone operation
                    }
                    
                    Console.WriteLine($"🔐 REDIS: Attempting connection to {connectionString}");
                    
                    // Apache Flink 2.0 pattern: Retry connection with exponential backoff
                    return ConnectToRedisWithRetry(connectionString);
                });
                
                builder.Services.AddSingleton<IDatabase>(provider =>
                {
                    var multiplexer = provider.GetRequiredService<IConnectionMultiplexer>();
                    return multiplexer.GetDatabase();
                });
                
                // Add the Kafka consumer as the main background service
                builder.Services.AddHostedService<TaskManagerKafkaConsumer>();
                
                var host = builder.Build();
                
                Console.WriteLine("🚀 STARTING: Kafka consumer group background service");
                await host.RunAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"💥 FATAL ERROR: {ex.Message}");
                Console.WriteLine($"💥 STACK TRACE: {ex.StackTrace}");
                
                // Keep alive for Aspire orchestration
                await KeepProcessAliveOnError();
            }
        }

        /// <summary>
        /// Get Redis connection string from multiple sources following Apache Flink 2.0 patterns
        /// </summary>
        private static string? GetRedisConnectionString(IConfiguration configuration)
        {
            // Try Aspire connection string first
            string? connectionString = configuration.GetConnectionString("redis");
            if (!string.IsNullOrEmpty(connectionString))
            {
                Console.WriteLine("🔐 REDIS: Using Aspire connection string");
                return connectionString;
            }
            
            // Try environment variable
            connectionString = Environment.GetEnvironmentVariable("DOTNET_REDIS_URL");
            if (!string.IsNullOrEmpty(connectionString))
            {
                Console.WriteLine("🔐 REDIS: Using DOTNET_REDIS_URL environment variable");
                return connectionString;
            }
            
            // Try alternative environment variable
            connectionString = Environment.GetEnvironmentVariable("ConnectionStrings__redis");
            if (!string.IsNullOrEmpty(connectionString))
            {
                Console.WriteLine("🔐 REDIS: Using ConnectionStrings__redis environment variable");
                return connectionString;
            }
            
            return null;
        }
        
        /// <summary>
        /// Connect to Redis with Apache Flink 2.0 resilient retry pattern
        /// </summary>
        private static IConnectionMultiplexer ConnectToRedisWithRetry(string connectionString)
        {
            const int maxRetries = 5;
            const int baseDelayMs = 1000; // Start with 1 second
            
            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    Console.WriteLine($"🔄 REDIS: Connection attempt {attempt}/{maxRetries} to {connectionString}");
                    
                    var options = CreateRedisConfigurationOptions(connectionString);
                    var multiplexer = ConnectionMultiplexer.Connect(options);
                    
                    Console.WriteLine("✅ REDIS: Connected successfully on attempt " + attempt);
                    return multiplexer;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ REDIS: Attempt {attempt}/{maxRetries} failed: {ex.Message}");
                    
                    if (attempt == maxRetries)
                    {
                        Console.WriteLine("💥 REDIS: All connection attempts failed - FlinkJobSimulator cannot start");
                        throw new InvalidOperationException($"Failed to connect to Redis after {maxRetries} attempts: {ex.Message}", ex);
                    }
                    
                    // Apache Flink 2.0 pattern: Exponential backoff
                    var delay = baseDelayMs * (int)Math.Pow(2, attempt - 1);
                    Console.WriteLine($"⏳ REDIS: Waiting {delay}ms before retry...");
                    Thread.Sleep(delay);
                }
            }
            
            throw new InvalidOperationException("Should never reach here");
        }

        private static StackExchange.Redis.ConfigurationOptions CreateRedisConfigurationOptions(string connectionString)
        {
            var options = new StackExchange.Redis.ConfigurationOptions();

            if (connectionString.StartsWith("redis://"))
            {
                var uri = new Uri(connectionString);
                options.EndPoints.Add(uri.Host, uri.Port);
                options.Password = ExtractPasswordFromUri(uri);
            }
            else
            {
                Console.WriteLine("🔄 REDIS CONFIG: Using standard ConfigurationOptions.Parse for non-URI connection string");
                options = StackExchange.Redis.ConfigurationOptions.Parse(connectionString);
            }

            ApplyDefaultRedisOptions(options);
            return options;
        }

        private static string? ExtractPasswordFromUri(Uri uri)
        {
            if (!string.IsNullOrEmpty(uri.UserInfo))
            {
                var userInfo = uri.UserInfo;
                if (userInfo.Contains(':'))
                {
                    var password = userInfo.Split(':')[1];
                    return string.IsNullOrEmpty(password) ? GetFallbackPassword() : password;
                }
                return userInfo;
            }
            return GetFallbackPassword();
        }

        private static string? GetFallbackPassword()
        {
            var envPassword = Environment.GetEnvironmentVariable("SIMULATOR_REDIS_PASSWORD");
            if (!string.IsNullOrEmpty(envPassword))
            {
                Console.WriteLine($"🔐 REDIS CONFIG: Using SIMULATOR_REDIS_PASSWORD environment variable (length: {envPassword.Length})");
                return envPassword;
            }
            Console.WriteLine("🔐 REDIS CONFIG: No password specified, using empty password");
            return "";
        }

        private static void ApplyDefaultRedisOptions(StackExchange.Redis.ConfigurationOptions options)
        {
            options.ConnectTimeout = 15000;
            options.SyncTimeout = 15000;
            options.AbortOnConnectFail = false;
            options.ConnectRetry = 3;
        }

        private static async Task KeepProcessAliveOnError()
        {
            Console.WriteLine("💓 KEEPALIVE: Starting error mode heartbeat");
            
            try
            {
                int heartbeatCount = 0;
                while (heartbeatCount < 1000000) // Very large limit
                {
                    await Task.Delay(TimeSpan.FromSeconds(30));
                    heartbeatCount++;
                    Console.WriteLine($"💓 ERROR HEARTBEAT {heartbeatCount}: Alive at {DateTime.UtcNow:HH:mm:ss} UTC - PID: {Environment.ProcessId}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"💓 HEARTBEAT ERROR: {ex.Message}");
                
                // Fallback manual loop
                int fallbackCount = 0;
                while (fallbackCount < 1000000)
                {
                    Thread.Sleep(60000);
                    fallbackCount++;
                    Console.WriteLine($"💓 FALLBACK HEARTBEAT {fallbackCount}: Alive at {DateTime.UtcNow:HH:mm:ss} UTC");
                }
            }
        }
    }
}
