namespace FlinkDotNet.Core.Abstractions.Operators
{
    public enum ChainingStrategy
    {
        /// <summary>
        /// Always chain this operator with the preceding and succeeding operators if possible.
        /// </summary>
        ALWAYS,

        /// <summary>
        /// Never chain this operator with any other operator.
        /// </summary>
        NEVER,

        /// <summary>
        /// Chain this operator with the preceding operator, but do not chain with the succeeding operator.
        /// This operator will be the head of a new chain (if chained with previous) or start a new chain itself.
        /// </summary>
        HEAD
    }
}
