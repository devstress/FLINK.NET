namespace FlinkDotNet.Core.Abstractions.Windowing
{
    /// <summary>
    /// Represents the progress of event time in a stream.
    /// </summary>
    public readonly record struct Watermark(long Timestamp);
}
