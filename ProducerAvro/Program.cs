using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Confluent.SchemaRegistry;

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

        string bootstrapServers = "localhost:19092,localhost:19093,localhost:19094";
        string schemaRegistryUrl = "http://localhost:8081";
        string topic = "position-avro";

        // Enregistrement du schéma Avro dans le Schema Registry
        string avroSchema = File.ReadAllText("avro/Coursier.avsc");
        using var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
        {
            Url = schemaRegistryUrl
        });
        var schema = new Schema(avroSchema, SchemaType.Avro);
        int schemaId = await schemaRegistry.RegisterSchemaAsync($"{topic}-value", schema);
        Console.WriteLine($"Schéma enregistré avec l'id : {schemaId}");

        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();

        Console.WriteLine($"Starting {nbThreads} threads producing each {nbMessages}. SendMode is {sendMode}");
        var tasks = new Task[nbThreads];
        for (int i = 0; i < nbThreads; i++)
        {
            int threadIndex = i;
            tasks[i] = Task.Run(async () =>
            {
                using var producer = new ProducerThread(bootstrapServers, schemaRegistryUrl, topic, sendMode);

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
