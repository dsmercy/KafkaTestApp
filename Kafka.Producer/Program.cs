using Confluent.Kafka;
using System;

namespace Kafka.Producer
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Producer started.");
            string brokerList = "localhost:9092";
            string topicName = "test-topic";

            var config = new ProducerConfig { BootstrapServers = brokerList };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                while (true)
                {
                    Console.WriteLine("Enter a message to produce (or type 'exit' to quit):");
                    string inputMessage = Console.ReadLine();

                    if (inputMessage.ToLower() == "exit")
                    {
                        break;
                    }

                    producer.Produce(topicName, new Message<Null, string> { Value = inputMessage });
                    Console.WriteLine($"Produced: {inputMessage}");
                }

                // Ensure all outstanding messages are delivered
                producer.Flush(TimeSpan.FromSeconds(10));
            }

            Console.WriteLine("Producer completed.");
        }
    }
}
