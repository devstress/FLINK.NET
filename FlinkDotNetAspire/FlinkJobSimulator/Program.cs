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
                
                // Configure Redis
                builder.Services.AddSingleton<IConnectionMultiplexer>(provider =>
                {
                    var configuration = provider.GetRequiredService<IConfiguration>();
                    var connectionString = configuration.GetConnectionString("redis");
                    
                    if (string.IsNullOrEmpty(connectionString))
                    {
                        throw new InvalidOperationException("Redis connection string not found");
                    }
                    
                    Console.WriteLine($"🔐 REDIS: Connecting to {connectionString}");
                    
                    var options = CreateRedisConfigurationOptions(connectionString);
                    var multiplexer = ConnectionMultiplexer.Connect(options);
                    
                    Console.WriteLine("✅ REDIS: Connected successfully");
                    return multiplexer;
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
        
        private static StackExchange.Redis.ConfigurationOptions CreateRedisConfigurationOptions(string connectionString)
        {
            if (connectionString.StartsWith("redis://"))
            {
                // Parse Redis URI format manually to handle password extraction properly
                var uri = new Uri(connectionString);
                var options = new StackExchange.Redis.ConfigurationOptions();
                options.EndPoints.Add(uri.Host, uri.Port);
                
                // Extract password from URI - handle both redis://:password@host:port and redis://user:password@host:port
                if (!string.IsNullOrEmpty(uri.UserInfo))
                {
                    var userInfo = uri.UserInfo;
                    if (userInfo.Contains(':'))
                    {
                        // Format: redis://user:password@host:port or redis://:password@host:port
                        var password = userInfo.Split(':')[1];
                        if (!string.IsNullOrEmpty(password))
                        {
                            options.Password = password;
                            Console.WriteLine($"🔐 REDIS CONFIG: Extracted password from URI (length: {password.Length})");
                        }
                        else
                        {
                            // Empty password in URI - check environment variable as fallback
                            var envPassword = Environment.GetEnvironmentVariable("SIMULATOR_REDIS_PASSWORD");
                            if (!string.IsNullOrEmpty(envPassword))
                            {
                                options.Password = envPassword;
                                Console.WriteLine($"🔐 REDIS CONFIG: Empty password in URI, using SIMULATOR_REDIS_PASSWORD environment variable (length: {envPassword.Length})");
                            }
                            else
                            {
                                options.Password = ""; // Empty password
                                Console.WriteLine("🔐 REDIS CONFIG: Empty password in URI and no SIMULATOR_REDIS_PASSWORD environment variable, using empty password");
                            }
                        }
                    }
                    else
                    {
                        // Format: redis://password@host:port (no colon, treat as password)
                        options.Password = userInfo;
                        Console.WriteLine($"🔐 REDIS CONFIG: Extracted password from URI without colon (length: {userInfo.Length})");
                    }
                }
                else
                {
                    // No credentials in URI - check environment variable as fallback
                    var envPassword = Environment.GetEnvironmentVariable("SIMULATOR_REDIS_PASSWORD");
                    if (!string.IsNullOrEmpty(envPassword))
                    {
                        options.Password = envPassword;
                        Console.WriteLine($"🔐 REDIS CONFIG: No credentials in URI, using SIMULATOR_REDIS_PASSWORD environment variable (length: {envPassword.Length})");
                    }
                    else
                    {
                        options.Password = "";
                        Console.WriteLine("🔐 REDIS CONFIG: No password specified in URI and no SIMULATOR_REDIS_PASSWORD environment variable, using empty password");
                    }
                }
                
                // Set optimal connection parameters
                options.ConnectTimeout = 15000;
                options.SyncTimeout = 15000;
                options.AbortOnConnectFail = false;
                options.ConnectRetry = 3;
                
                return options;
            }
            else
            {
                // Fall back to standard parsing for non-URI formats
                Console.WriteLine("🔄 REDIS CONFIG: Using standard ConfigurationOptions.Parse for non-URI connection string");
                return StackExchange.Redis.ConfigurationOptions.Parse(connectionString);
            }
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