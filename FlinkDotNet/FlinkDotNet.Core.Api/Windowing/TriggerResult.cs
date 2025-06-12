namespace FlinkDotNet.Core.Api.Windowing
{
    /// <summary>
    /// Result type for trigger methods. It tells the system how to act on the window,
    /// e.g., fire, purge, or do nothing.
    /// </summary>
    [Flags]
    public enum TriggerResult : byte
    {
        /// <summary>Do nothing.</summary>
        None = 0, // 0000

        /// <summary>Fire the window function and emit results.</summary>
        Fire = 1,     // 0001

        /// <summary>Purge the elements in the window.</summary>
        Purge = 2,    // 0010

        /// <summary>Fire the window function and then purge elements.</summary>
        FireAndPurge = Fire | Purge // 0011
    }

    public static class TriggerResultExtensions
    {
        public static bool IsFire(this TriggerResult result) => (result & TriggerResult.Fire) != 0;
        public static bool IsPurge(this TriggerResult result) => (result & TriggerResult.Purge) != 0;
    }
}
