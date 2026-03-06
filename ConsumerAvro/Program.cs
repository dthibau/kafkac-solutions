using System;
using System.Collections.Generic;
using System.Threading;

namespace KafkaConsumerAvro
{
    internal class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 1 || !int.TryParse(args[0], out int nbThreads))
            {
                Console.WriteLine("Usage: dotnet run <nbThreads>");
                return;
            }

            string bootstrapServers = "localhost:19092,localhost:19093,localhost:19094";
            string schemaRegistryUrl = "http://localhost:8081";
            string groupId = "position-avro-consumer";
            string topic = "position-avro";

            List<Thread> threads = new List<Thread>();
            CancellationTokenSource cts = new CancellationTokenSource();

            Console.CancelKeyPress += (sender, e) =>
            {
                Console.WriteLine("Annulation demandée...");
                cts.Cancel();
                e.Cancel = true;
            };

            for (int i = 0; i < nbThreads; i++)
            {
                KafkaConsumerThread consumerThread = new KafkaConsumerThread(bootstrapServers, schemaRegistryUrl, groupId, topic);

                Thread thread = new Thread(() => consumerThread.StartConsuming(cts.Token));
                threads.Add(thread);
                thread.Start();
            }

            foreach (var thread in threads)
            {
                thread.Join();
            }
        }
    }
}
