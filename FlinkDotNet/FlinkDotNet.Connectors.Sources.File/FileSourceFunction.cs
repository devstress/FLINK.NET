using System;
using System.IO;
using System.Text; // For a simple string deserialization if no serializer is provided for string
using System.Threading;
using FlinkDotNet.Core.Abstractions.Sources;
using FlinkDotNet.Core.Abstractions.Serializers; // For ITypeSerializer

namespace FlinkDotNet.Connectors.Sources.File
{
    public class FileSourceFunction<TOut> : ISourceFunction<TOut>
    {
        private readonly string _filePath;
        private readonly ITypeSerializer<TOut> _serializer;
        private volatile bool _isRunning = true;

        public FileSourceFunction(string filePath, ITypeSerializer<TOut> serializer)
        {
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentNullException(nameof(filePath));
            _filePath = filePath;
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        }

        public void Run(ISourceContext<TOut> ctx)
        {
            Console.WriteLine($"FileSourceFunction: Starting to read from {_filePath}");
            try
            {
                using var reader = new StreamReader(_filePath, Encoding.UTF8);
                string? line;
                while (_isRunning && (line = reader.ReadLine()) != null)
                {
                    if (!_isRunning) break;

                    try
                    {
                        // For this basic version, we assume the serializer can handle raw UTF-8 bytes of a line.
                        // This might need adjustment based on how ITypeSerializer is intended to be used
                        // (e.g., if it expects a structured format rather than just a line of text).
                        // If TOut is string and StringSerializer is used, this should be fine.
                        var recordBytes = Encoding.UTF8.GetBytes(line);
                        TOut record = _serializer.Deserialize(recordBytes);
                        ctx.Collect(record);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"FileSourceFunction: Error deserializing line '{line}': {ex.Message}");
                        // Decide on error handling: skip, fail, DLQ (later)
                    }
                }
            }
            catch (FileNotFoundException)
            {
                Console.WriteLine($"FileSourceFunction: Error - File not found: {_filePath}");
                // Consider whether to throw or just complete if file not found.
                // For now, it will complete the source if file isn't there.
            }
            catch (Exception ex)
            {
                Console.WriteLine($"FileSourceFunction: Error reading file {_filePath}: {ex.Message}");
                // Depending on the exception, may need to rethrow to fail the task.
            }
            finally
            {
                 Console.WriteLine($"FileSourceFunction: Finished reading from {_filePath}");
            }
        }

        public void Cancel()
        {
            Console.WriteLine($"FileSourceFunction: Cancel called for {_filePath}");
            _isRunning = false;
        }
    }
}
