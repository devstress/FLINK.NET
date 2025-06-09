using System;
using System.Threading.Tasks;
using Grpc.Core;
using FlinkDotNet.Proto.Internal;

namespace FlinkDotNet.TaskManager.Services
{
    public class DataExchangeServiceImpl : DataExchangeService.DataExchangeServiceBase
    {
        private readonly string _taskManagerId;

        public DataExchangeServiceImpl(string taskManagerId)
        {
            _taskManagerId = taskManagerId;
        }

        public override Task ExchangeData(IAsyncStreamReader<UpstreamPayload> requestStream,
            IServerStreamWriter<DownstreamPayload> responseStream,
            ServerCallContext context)
        {
            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] ExchangeData not implemented.");
            return Task.CompletedTask;
        }
    }
}
