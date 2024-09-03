using KafkaProducer.Formation.model;
using System;
using System.Diagnostics;
using System.Threading;
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

        string bootstrapServers = "localhost:19092"; // Remplacez par l'adresse de votre serveur Kafka
        string topic = "position"; // Remplacez par le nom de votre topic Kafka

        ProducerThread producer = new ProducerThread(bootstrapServers, topic, sendMode);

        // Initialiser le chronomètre
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();

        // Crée et démarre les threads
        var tasks = new Task[nbThreads];
        for (int i = 0; i < nbThreads; i++)
        {
            int threadIndex = i;
            switch (sendMode)
            {
                case SendMode.FIRE_AND_FORGET:
                       tasks[i] = Task.Run(() =>
                            {
                                Coursier coursier = new Coursier(threadIndex, new Position(45, 45));
                                for (int j = 0; j < nbMessages; j++)
                                {
                                    coursier.move();
                                    producer.ProduceFireAndForget(coursier);
                                    Thread.Sleep(100);
                                }
                        });
                    break;
                case SendMode.SYNCHRONE:
                    tasks[i] = Task.Run(() =>
                    {
                        Coursier coursier = new Coursier(threadIndex, new Position(45, 45));
                        for (int j = 0; j < nbMessages; j++)
                        {
                            coursier.move();
                            producer.ProduceSynchronously(coursier);
                            Thread.Sleep(100);
                        }
                    });
                    break;
                case SendMode.ASYNCHRONE:
                    tasks[i] = Task.Run(async () =>
                    {
                        Coursier coursier = new Coursier(threadIndex, new Position(45, 45));
                        for (int j = 0; j < nbMessages; j++)
                        {
                            coursier.move();
                            await producer.ProduceAsynchronously(coursier);
                            Thread.Sleep(100);
                        }
                     });
                    break;
                default:
                    Console.WriteLine($"Mode inconnu: {sendMode}");
                    break; ;
            }
        }

        // Attendre que tous les threads aient terminé
        await Task.WhenAll(tasks);

        // Arrêter le chronomètre
        stopwatch.Stop();

        // Afficher le temps total d'exécution
        Console.WriteLine($"Tous les messages ont été envoyés. Temps total d'exécution : {stopwatch.Elapsed.TotalSeconds} secondes.");
    }
}
