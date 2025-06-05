using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using FlinkDotNet.JobManager.Models; // Now references the actual DTOs

namespace FlinkDotNet.JobManager.Interfaces
{
    /// <summary>
    /// Defines the contract for the Job Management API.
    /// These methods correspond to the REST/gRPC endpoints for managing jobs.
    /// </summary>
    public interface IJobManagerApi
    {
        Task<IActionResult> SubmitJob([FromBody] JobDefinitionDto jobDefinition);
        Task<IActionResult> GetJobStatus(string jobId);
        Task<IActionResult> ScaleJob(string jobId, [FromBody] ScaleParametersDto scaleParameters);
        Task<IActionResult> StopJob(string jobId);
        Task<IActionResult> CancelJob(string jobId);
        Task<IActionResult> GetJobCheckpoints(string jobId);
        Task<IActionResult> RestartJob(string jobId);
        Task<IActionResult> ResubmitDlqMessages(string jobId);
        Task<IActionResult> ModifyDlqMessage(string jobId, string messageId, [FromBody] DlqMessageDto messageData);
    }
}
