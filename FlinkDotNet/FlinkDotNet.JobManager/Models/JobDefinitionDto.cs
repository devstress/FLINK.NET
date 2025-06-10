namespace FlinkDotNet.JobManager.Models
{
    public class JobDefinitionDto
    {
        public string JobName { get; set; } = "Flink Job";

        /// <summary>
        /// List of operator definitions that form the job graph.
        /// Each OperatorDefinitionDto can be the head of an operator chain.
        /// </summary>
        public List<OperatorDefinitionDto> Steps { get; set; } = new List<OperatorDefinitionDto>();

    }
}
#nullable disable
