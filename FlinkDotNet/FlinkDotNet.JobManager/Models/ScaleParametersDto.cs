namespace FlinkDotNet.JobManager.Models
{
    public class ScaleParametersDto
    {
        public string? OperatorName { get; set; } // Optional: to scale a specific operator
        [System.ComponentModel.DataAnnotations.Required]
        public int? DesiredParallelism { get; set; }
        // Add other relevant scaling parameters
    }
}
