using System;
using System.IO;
using System.Reflection;
using System.Runtime.Loader;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace SBCanary
{
    class Program
    {
        private static ILoggerFactory LoggerFactory;

        static int Main(string[] args)
        {
            var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "Development";
            var configBuilder = new ConfigurationBuilder()
                .SetBasePath(Path.Combine(AppContext.BaseDirectory))
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddJsonFile($"appsettings.{environment}.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables();

            var config = configBuilder.Build();

            LoggerFactory = new LoggerFactory().AddConsole(config.GetSection("Logging"));
            var logger = LoggerFactory.CreateLogger("Main");

            logger.LogInformation("Starting...");
            try
            {
                var app = new Program(config["ServiceBus:ConnectionString"], config["ServiceBus:TopicName"], config["ServiceBus:SubscriptionName"]);

                Console.CancelKeyPress += (s, e) =>
                {
                    e.Cancel = true;
                    logger.LogInformation("Stopping...");
                    app.Stop();
                };

                app.Run()
                    .GetAwaiter()
                    .GetResult();
            }
            catch (Exception e)
            {
                logger.LogCritical(e.ToString());
                return 1;
            }
            return 0;
        }

        private readonly string connectionString;
        private readonly string topicName;
        private readonly string subscriptionName;

        public Program(string connectionString, string topicName, string subscriptionName)
        {
            this.connectionString = connectionString;
            this.topicName = topicName;
            this.subscriptionName = subscriptionName;
        }

        private CancellationTokenSource cts;
        private ManualResetEventSlim stopped;
        public async Task Run()
        {
            if (cts != null) throw new InvalidOperationException("Already running!");

            cts = new CancellationTokenSource();
            stopped = new ManualResetEventSlim();

            var client = new SubscriptionClient(connectionString, topicName, subscriptionName);
            StartSubscriber(client);

            var publisher = StartPublisher(cts.Token);
            var watcher = StartWatcher(cts.Token);

            await Task.WhenAny(new[] { publisher, watcher });
            await client.CloseAsync();

            cts = null;
        }

        public void Stop()
        {
            cts.Cancel();
        }

        private async Task StartPublisher(CancellationToken token)
        {
            var logger = LoggerFactory.CreateLogger("Publisher");
            var client = new TopicClient(connectionString, topicName);
            while (!token.IsCancellationRequested)
            {
                var body = DateTimeOffset.UtcNow;
                await client.SendAsync(new Message()
                {
                    Body = Encoding.Unicode.GetBytes(body.ToString("o"))
                });
                var took = (DateTimeOffset.UtcNow - body).TotalMilliseconds;
                logger.LogDebug($"Sent {body:o}, took {took}ms.");
                await Task.Delay(100);
            }
            logger.LogInformation("Publisher stopped");
        }

        DateTimeOffset lastReceived = DateTimeOffset.MinValue;
        private void StartSubscriber(SubscriptionClient client)
        {
            var logger = LoggerFactory.CreateLogger("Subscriber");
            lastReceived = DateTimeOffset.UtcNow;
            logger.LogDebug("Starting subscriber...");
            client.RegisterMessageHandler(
                (message, cancellationToken) =>
                {
                    var time = DateTimeOffset.UtcNow;
                    logger.LogTrace(Newtonsoft.Json.JsonConvert.SerializeObject(message));
                    var body = Encoding.Unicode.GetString(message.Body);
                    var sent = DateTimeOffset.Parse(body);
                    var totalMs = (time - sent).TotalMilliseconds;
                    logger.LogDebug($"{totalMs}ms");
                    if (sent > lastReceived) lastReceived = sent;
                    return Task.CompletedTask;
                },
                new MessageHandlerOptions((e) => LogMessageHandlerException(logger, e))
                {
                    AutoComplete = true,
                    MaxConcurrentCalls = 1
                }
            );
        }

        private Task LogMessageHandlerException(ILogger l, ExceptionReceivedEventArgs e)
        {
            l.LogError("Exception: \"{0}\" {0}", e.Exception.Message, e.ExceptionReceivedContext.EntityPath);
            return Task.CompletedTask;
        }

        TimeSpan warningTime = TimeSpan.FromSeconds(5);
        TimeSpan errorTime = TimeSpan.FromMinutes(1);
        TimeSpan criticalTime = TimeSpan.FromMinutes(10);
        TimeSpan watchSleepTime = TimeSpan.FromSeconds(5);
        private async Task StartWatcher(CancellationToken token)
        {
            var logger = LoggerFactory.CreateLogger("Watcher");
            var lastThreshold = TimeSpan.Zero;

            await Task.Delay(watchSleepTime);

            while (!token.IsCancellationRequested)
            {
                var delay = DateTimeOffset.UtcNow - lastReceived;
                logger.LogDebug($"Delay = {delay.TotalSeconds}s");

                if (delay >= criticalTime && lastThreshold != criticalTime)
                {
                    logger.LogCritical($"{criticalTime} threshold exceeded - {delay.TotalSeconds}s");
                    lastThreshold = criticalTime;
                }
                else if (criticalTime > delay && delay >= errorTime && lastThreshold != errorTime)
                {
                    logger.LogError($"{errorTime} threshold exceeded - {delay.TotalSeconds}s");
                    lastThreshold = errorTime;
                }
                else if (errorTime > delay && delay >= warningTime && lastThreshold != warningTime)
                {
                    logger.LogWarning($"{warningTime} threshold exceeded - {delay.TotalSeconds}s");
                    lastThreshold = warningTime;
                }
                else if (warningTime > delay && lastThreshold != TimeSpan.Zero)
                {
                    logger.LogInformation($"OK - {delay.TotalMilliseconds}ms");
                    lastThreshold = TimeSpan.Zero;
                }

                await Task.Delay(watchSleepTime);
            }
        }
    }
}
