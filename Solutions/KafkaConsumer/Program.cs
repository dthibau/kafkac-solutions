using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConsumer
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

            string bootstrapServers = "localhost:19092";
            string groupId = "position-consumer";
            string topic = "position";
            string connectionString = "Host=localhost;Username=postgres;Password=postgres;Database=consumer";

            List<Thread> threads = new List<Thread>();
            CancellationTokenSource cts = new CancellationTokenSource();

            // Gestion du Ctrl+C pour demander l'annulation
            Console.CancelKeyPress += (sender, e) =>
            {
                Console.WriteLine("Annulation demandée...");
                cts.Cancel();
                e.Cancel = true; // Empêche l'application de se fermer immédiatement
            };

            for (int i = 0; i < nbThreads; i++)
            {
                // Créer une nouvelle instance de KafkaConsumerService pour chaque thread
                KafkaConsumerThread consumerThread = new KafkaConsumerThread(bootstrapServers, groupId, topic, connectionString);

                Thread thread = new Thread(() => consumerThread.StartConsuming(cts.Token));
                threads.Add(thread);
                thread.Start();
            }

            // Attendre que tous les threads terminent (pour un exemple simplifié)
            foreach (var thread in threads)
            {
                thread.Join();
            }
        }
    }
}
