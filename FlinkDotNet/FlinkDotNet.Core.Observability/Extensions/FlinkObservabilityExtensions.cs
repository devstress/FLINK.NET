using FlinkDotNet.Core.Abstractions.Observability;
using FlinkDotNet.Core.Observability;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using System.Diagnostics;

namespace FlinkDotNet.Core.Observability.Extensions
{
    /// <summary>
    /// Extension methods for configuring Flink observability following Apache Flink 2.0 standards.
    /// Provides comprehensive setup for metrics, tracing, logging, and health monitoring.
    /// </summary>
    public static class FlinkObservabilityExtensions
    {
        /// <summary>
        /// Adds comprehensive Flink observability services to the service collection.
        /// Configures all observability components following Apache Flink 2.0 patterns.
        /// </summary>
        public static IServiceCollection AddFlinkObservability(this IServiceCollection services, 
            string serviceName = "FlinkDotNet", Action<FlinkObservabilityOptions>? configure = null)
        {
            var options = new FlinkObservabilityOptions();
            configure?.Invoke(options);

            // Register core observability services
            services.AddSingleton<IFlinkMetrics>(serviceProvider =>
            {
                var logger = serviceProvider.GetRequiredService<ILogger<FlinkMetricsCollector>>();
                return new FlinkMetricsCollector(logger);
            });

            services.AddSingleton<IFlinkTracing>(serviceProvider =>
            {
                var logger = serviceProvider.GetRequiredService<ILogger<FlinkTracingCollector>>();
                return new FlinkTracingCollector(logger, serviceName);
            });

            services.AddSingleton<IFlinkLogger>(serviceProvider =>
            {
                var logger = serviceProvider.GetRequiredService<ILogger<FlinkStructuredLogger>>();
                var baseContext = new Dictionary<string, object>
                {
                    ["service.name"] = serviceName,
                    ["service.version"] = "1.0.0",
                    ["flink.version"] = "2.0"
                };
                return new FlinkStructuredLogger(logger, baseContext);
            });

            services.AddSingleton<IFlinkHealthMonitor>(serviceProvider =>
            {
                var logger = serviceProvider.GetRequiredService<ILogger<FlinkHealthMonitor>>();
                return new FlinkHealthMonitor(logger);
            });

            // Configure OpenTelemetry for metrics and tracing
            services.AddOpenTelemetry()
                .WithMetrics(metrics =>
                {
                    metrics.AddMeter("FlinkDotNet.Core")
                        .AddRuntimeInstrumentation();

                    if (options.EnablePrometheusMetrics)
                    {
                        // Add Prometheus metrics export if configured
                        // metrics.AddPrometheusExporter();
                    }

                    if (options.EnableConsoleMetrics)
                    {
                        metrics.AddConsoleExporter();
                    }
                })
                .WithTracing(tracing =>
                {
                    tracing.AddSource($"{serviceName}.Tracing")
                        .SetSampler(new AlwaysOnSampler());

                    if (options.EnableConsoleTracing)
                    {
                        tracing.AddConsoleExporter();
                    }

                    if (options.EnableJaegerTracing && !string.IsNullOrEmpty(options.JaegerEndpoint))
                    {
                        // Add Jaeger tracing export if configured
                        // tracing.AddJaegerExporter(jaeger => jaeger.Endpoint = new Uri(options.JaegerEndpoint));
                    }
                });

            // Add health checks for Flink components
            services.AddHealthChecks()
                .AddCheck<FlinkComponentHealthCheck>("flink_components")
                .AddCheck<FlinkJobHealthCheck>("flink_jobs");

            return services;
        }

        /// <summary>
        /// Adds Flink observability to a host builder with default configuration.
        /// Simplifies setup for applications using the generic host.
        /// </summary>
        public static IHostBuilder AddFlinkObservability(this IHostBuilder hostBuilder, 
            string serviceName = "FlinkDotNet", Action<FlinkObservabilityOptions>? configure = null)
        {
            return hostBuilder.ConfigureServices((context, services) =>
            {
                services.AddFlinkObservability(serviceName, configure);
            });
        }

        /// <summary>
        /// Creates a scoped Flink logger with additional context.
        /// Useful for adding operation-specific context to logs.
        /// </summary>
        public static IFlinkLogger CreateScopedLogger(this IServiceProvider services,
            Dictionary<string, object> context)
        {
            var baseLogger = services.GetRequiredService<IFlinkLogger>();
            return baseLogger.WithContext(context);
        }

        /// <summary>
        /// Starts an operator activity with proper Flink context.
        /// Enables distributed tracing across operator boundaries.
        /// </summary>
        public static Activity? StartFlinkOperatorActivity(this IServiceProvider services,
            string operatorName, string taskId, string? parentSpanId = null)
        {
            var tracing = services.GetRequiredService<IFlinkTracing>();
            return tracing.StartOperatorSpan(operatorName, taskId, parentSpanId);
        }

        /// <summary>
        /// Records Flink metrics using the registered metrics collector.
        /// Provides convenient access to metrics recording.
        /// </summary>
        public static void RecordFlinkMetric(this IServiceProvider services,
            string metricType, string operatorName, string taskId, object value)
        {
            var metrics = services.GetRequiredService<IFlinkMetrics>();
            
            switch (metricType.ToLowerInvariant())
            {
                case "records_in":
                    if (value is long count)
                        for (int i = 0; i < count; i++)
                            metrics.RecordIncomingRecord(operatorName, taskId);
                    break;
                case "records_out":
                    if (value is long outCount)
                        for (int i = 0; i < outCount; i++)
                            metrics.RecordOutgoingRecord(operatorName, taskId);
                    break;
                case "latency":
                    if (value is TimeSpan latency)
                        metrics.RecordLatency(operatorName, taskId, latency);
                    break;
                case "error":
                    if (value is string errorType)
                        metrics.RecordError(operatorName, taskId, errorType);
                    break;
            }
        }
    }

    /// <summary>
    /// Configuration options for Flink observability.
    /// Allows customization of observability features and exporters.
    /// </summary>
    public class FlinkObservabilityOptions
    {
        /// <summary>Enable Prometheus metrics export</summary>
        public bool EnablePrometheusMetrics { get; set; } = false;

        /// <summary>Enable console metrics export for development</summary>
        public bool EnableConsoleMetrics { get; set; } = true;

        /// <summary>Enable console tracing export for development</summary>
        public bool EnableConsoleTracing { get; set; } = true;

        /// <summary>Enable Jaeger tracing export</summary>
        public bool EnableJaegerTracing { get; set; } = false;

        /// <summary>Jaeger endpoint for trace export</summary>
        public string? JaegerEndpoint { get; set; }

        /// <summary>Enable detailed operator-level monitoring</summary>
        public bool EnableOperatorMonitoring { get; set; } = true;

        /// <summary>Enable comprehensive job-level monitoring</summary>
        public bool EnableJobMonitoring { get; set; } = true;

        /// <summary>Enable network performance monitoring</summary>
        public bool EnableNetworkMonitoring { get; set; } = true;

        /// <summary>Enable state backend monitoring</summary>
        public bool EnableStateBackendMonitoring { get; set; } = true;

        /// <summary>Health check interval in seconds</summary>
        public int HealthCheckIntervalSeconds { get; set; } = 30;

        /// <summary>Metrics collection interval in seconds</summary>
        public int MetricsIntervalSeconds { get; set; } = 10;
    }
}