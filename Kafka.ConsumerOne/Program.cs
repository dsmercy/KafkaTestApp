using Confluent.Kafka;
using System;
using System.Threading;

namespace Kafka.ConsumerOne
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine($"Consumer 1 started");
            string brokerList = "localhost:9092";
            string topicName = "test-topic";

            var config = new ConsumerConfig
            {
                GroupId = "consumer-group-1",
                BootstrapServers = brokerList,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Null, string>(config).Build())
            {
                consumer.Subscribe(topicName);
                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        var consumeResult = consumer.Consume(cts.Token);
                        Console.WriteLine($"Consumer 1: {consumeResult.Message.Value}");
                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                }
                Console.WriteLine($"Consumer 1 completed");
            }
        }
    }
}
