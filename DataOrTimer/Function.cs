using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;

namespace DataOrTimer
{
    public static class Function
    {
        const string DataEventName = "NewData";

        [FunctionName(nameof(RunOrchestrator))]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            ILogger logger)
        {
            logger = context.CreateReplaySafeLogger(logger);

            var expiration = context.GetInput<DateTime>();

            var minDelay = TimeSpan.FromSeconds(10);
            var maxDelay = TimeSpan.FromSeconds(30);
            var delayDelta = expiration - context.CurrentUtcDateTime;
            var waitTime = TimeSpan.FromSeconds(Math.Clamp(delayDelta.TotalSeconds, minDelay.TotalSeconds, maxDelay.TotalSeconds));
            var wakeupTime = context.CurrentUtcDateTime + waitTime;

            logger.LogInformation($">>>>>>      [ORC] Orchestration begins with expiration at { expiration }, waitTime { waitTime }, next wakeup at { wakeupTime }");

            using (var cts = new CancellationTokenSource())
            {
                var timerTask = context.CreateTimer(wakeupTime, cts.Token);
                var dataTask = context.WaitForExternalEvent<string>(DataEventName);

                var winner = await Task.WhenAny(dataTask, timerTask);
                if (winner == dataTask)
                {
                    cts.Cancel();

                    var data = await dataTask;

                    logger.LogInformation($">>>>>>      [ORC] Data Event Triggered, data = { data }");

                    var result = await context.CallActivityAsync<string>(nameof(SayHello), data);

                    logger.LogInformation($">>>>>>      [ORC] Activity result { result }");
                }
                else
                {
                    logger.LogInformation(">>>>>>      [ORC] Timer Triggered");

                    if (context.CurrentUtcDateTime >= expiration)
                    {
                        logger.LogInformation(">>>>>>      [ORC] Orchestration expired");
                        return;
                    }
                }

                logger.LogInformation(">>>>>>      [ORC] Restarting for another round");
                context.ContinueAsNew(expiration, true);
            }
        }

        [FunctionName(nameof(SayHello))]
        public static async Task<string> SayHello([ActivityTrigger] string name, ILogger log)
        {
            log.LogInformation($">>>>>>      [ACT] Saying hello to {name}, STARTED.");

            await Task.Delay(TimeSpan.FromSeconds(10));

            log.LogInformation($">>>>>>      [ACT] Saying hello to {name}, DONE.");

            return $"Hello {name}!";
        }

        [FunctionName("Function_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]HttpRequestMessage req,
            [DurableClient]
        IDurableOrchestrationClient starter,
            ILogger logger)
        {
            var orchestrationId = "SingletonId";

            var orchestration = await starter.GetStatusAsync(orchestrationId);
            if (orchestration == null)
            {
                logger.LogInformation(">>>>>>      [CLI] Starting new orchestration, expiring in 10 minutes");
                await starter.StartNewAsync(nameof(RunOrchestrator), orchestrationId, DateTime.UtcNow.AddMinutes(10));
            }
            else
            {
                logger.LogInformation(">>>>>>      [CLI] Orchestration already exists");
            }

            var data = $"DATA:{Guid.NewGuid()}";
            logger.LogInformation($">>>>>>      [CLI] Sending data { data }");
            await starter.RaiseEventAsync(orchestrationId, DataEventName, data);

            return starter.CreateCheckStatusResponse(req, orchestrationId);
        }
    }
}