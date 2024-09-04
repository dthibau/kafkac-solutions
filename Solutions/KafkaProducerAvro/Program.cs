using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json;
using Confluent.SchemaRegistry;
using Avro.IO;
using model;


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
        string topic = "position-avro"; // Remplacez par le nom de votre topic Kafka
        string schemaRegistryUrl = "http://localhost:8081";

        var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = schemaRegistryUrl });
        string avroSchemaString = File.ReadAllText("..\\..\\Coursier.avsc");
        schemaRegistry.RegisterSchemaAsync($"{topic}-value", avroSchemaString).Wait();

        ProducerThread producer = new ProducerThread(bootstrapServers, topic, sendMode, schemaRegistry);

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
                                Position position = new Position();
                                position.latitude = 45;
                                position.longitude = 45;
                                Coursier coursier = new Coursier();
                                coursier.id = threadIndex;
                                coursier.position = position;
                                Random random = new Random();

                                for (int j = 0; j < nbMessages; j++)
                                {
                                    position.latitude += random.NextDouble() - 0.5;
                                    position.longitude += random.NextDouble() - 0.5;
                                    coursier.position = position;
                                    producer.ProduceFireAndForget(coursier);
                                    Thread.Sleep(100);
                                }
                        });
                    break;
                case SendMode.SYNCHRONE:
                    tasks[i] = Task.Run(() =>
                    {
                        Position position = new Position();
                        position.latitude = 45;
                        position.longitude = 45;
                        Coursier coursier = new Coursier();
                        coursier.id = threadIndex;
                        coursier.position = position;
                        Random random = new Random();

                        for (int j = 0; j < nbMessages; j++)
                        {
                            position.latitude += random.NextDouble() - 0.5;
                            position.longitude += random.NextDouble() - 0.5;
                            coursier.position = position;
                            producer.ProduceSynchronously(coursier);
                            Thread.Sleep(100);
                        }
                    });
                    break;
                case SendMode.ASYNCHRONE:
                    tasks[i] = Task.Run(async () =>
                    {
                        Position position = new Position();
                        position.latitude = 45;
                        position.longitude = 45;
                        Coursier coursier = new Coursier();
                        coursier.id = threadIndex;
                        coursier.position = position;
                        Random random = new Random();

                        for (int j = 0; j < nbMessages; j++)
                        {
                            position.latitude += random.NextDouble() - 0.5;
                            position.longitude += random.NextDouble() - 0.5;
                            coursier.position = position;
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
