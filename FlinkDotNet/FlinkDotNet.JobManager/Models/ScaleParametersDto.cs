namespace FlinkDotNet.JobManager.Models
{
    public class ScaleParametersDto
    {
        public string? OperatorName { get; set; } // Optional: to scale a specific operator
        public int DesiredParallelism { get; set; }
        // Add other relevant scaling parameters
    }
}
