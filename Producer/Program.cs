using KafkaProducer.Formation.model;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

class Program
{
    static async Task Main(string[] args)
    {
        if (args.Length != 3)
        {
            Console.WriteLine("Usage: dotnet run <nbThreads> <nbMessages> <mode>");
            Console.WriteLine("Modes: fire-and-forget=0, sync=1, async=2");
            return;
        }

        int nbThreads = int.Parse(args[0]);
        int nbMessages = int.Parse(args[1]);
        SendMode sendMode = (SendMode)int.Parse(args[2]);

        string bootstrapServers = "localhost:19092";
        string topic = "position";

        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();

        Console.WriteLine($"Starting {nbThreads} threads producing each {nbMessages}. SendMode is {sendMode}");
        // Crée et démarre les threads
        var tasks = new Task[nbThreads];
        for (int i = 0; i < nbThreads; i++)
        {
            int threadIndex = i;
            tasks[i] = Task.Run(async () =>
            {
                var producer = new ProducerThread(bootstrapServers, topic, sendMode);

                Console.WriteLine($"Starting thread {threadIndex}");
                await producer.StartProducing(threadIndex, nbMessages);
                Console.WriteLine($"Finished thread {threadIndex}");
            });
        }

        await Task.WhenAll(tasks);

        stopwatch.Stop();
        Console.WriteLine($"Tous les messages ont été envoyés. Temps total d'exécution : {stopwatch.Elapsed.TotalSeconds} secondes.");
    }
}
