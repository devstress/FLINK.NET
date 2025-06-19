using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace FlinkJobSimulator
{
    /// <summary>
    /// Apache Flink 2.0 compliant FlinkJobSimulator that submits jobs to JobManager.
    /// This follows the proper Flink architecture: FlinkJobSimulator -> JobManager -> TaskManagers.
    /// The JobManager deploys tasks to registered TaskManagers for distributed execution.
    /// </summary>
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("🌟 === FLINKJOBSIMULATOR STARTING ===");
            Console.WriteLine($"🌟 START TIME: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
            Console.WriteLine($"🌟 PROCESS ID: {Environment.ProcessId}");
            Console.WriteLine("🌟 APACHE FLINK 2.0 JOB SUBMISSION MODE");
            
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
Phase: JOB_SUBMISSION_PREPARATION
Message: FlinkJobSimulator is preparing to submit job to JobManager following Apache Flink 2.0 architecture
Architecture: FlinkJobSimulator -> JobManager -> TaskManagers
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
Phase: JOB_SUBMITTED_TO_JOBMANAGER
Message: FlinkJobSimulator has successfully submitted job to JobManager - TaskManagers are now executing the job
PreviousState: FlinkJobSimulatorNotStarted
Architecture: Job is now running on 20 TaskManagers coordinated by JobManager
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
        /// Apache Flink 2.0 compliant job submission mode.
        /// FlinkJobSimulator creates JobGraph and submits it to JobManager, which then deploys tasks to TaskManagers.
        /// This follows the proper architecture: FlinkJobSimulator -> JobManager -> TaskManagers.
        /// </summary>
        private static async Task RunAsKafkaConsumerGroupAsync(string[] args)
        {
            try
            {
                Console.WriteLine("🎯 APACHE FLINK 2.0 MODE: FlinkJobSimulator submits jobs to JobManager for TaskManager execution");
                
                var builder = Host.CreateApplicationBuilder(args);
                
                // Add JobSubmissionService for proper Apache Flink 2.0 job submission
                builder.Services.AddSingleton<JobSubmissionService>();
                
                var host = builder.Build();
                
                Console.WriteLine("🚀 STARTING: Apache Flink 2.0 compliant job submission");
                
                // Get the JobSubmissionService and submit the job
                var jobSubmissionService = host.Services.GetRequiredService<JobSubmissionService>();
                
                Console.WriteLine("📤 Submitting Kafka-to-Redis streaming job to JobManager...");
                bool jobSubmitted = await jobSubmissionService.SubmitKafkaToRedisStreamingJobAsync();
                
                if (jobSubmitted)
                {
                    Console.WriteLine("✅ Job successfully submitted to JobManager! TaskManagers will now execute the job.");
                    Console.WriteLine("🔄 JobManager will deploy tasks to registered TaskManagers for distributed processing.");
                    
                    // Keep the FlinkJobSimulator alive to maintain the submitted job
                    // In a real Flink cluster, the JobManager coordinates the job lifecycle
                    Console.WriteLine("⏳ FlinkJobSimulator keeping job alive while TaskManagers process...");
                    await host.RunAsync();
                }
                else
                {
                    Console.WriteLine("❌ Failed to submit job to JobManager");
                    throw new InvalidOperationException("Job submission failed");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"💥 FATAL ERROR: {ex.Message}");
                Console.WriteLine($"💥 STACK TRACE: {ex.StackTrace}");
                
                // Keep alive for Aspire orchestration
                await KeepProcessAliveOnError();
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
