using System.Collections.Generic;

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

        // Potential job-level configurations can be added here
        // public JobExecutionConfigDto ExecutionConfig { get; set; }
    }
}
#nullable disable
