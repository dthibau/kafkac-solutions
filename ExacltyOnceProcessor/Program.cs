using System;
using System.Threading;
using Confluent.Kafka;

class Program
{
    static void Main(string[] args)
    {
        if (args.Length != 1 || !int.TryParse(args[0], out int nbMessages))
        {
            Console.WriteLine("Usage: dotnet run <nbMessages>");
            return;
        }

        string bootstrapServers = "localhost:19092,localhost:19093,localhost:19094";
        string inputTopic = "position";
        string outputTopic = "position-output";
        string groupId = "exactly-once-group";

        var processor = new ExactlyOnceProcessor(bootstrapServers, groupId, inputTopic, outputTopic);

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (sender, e) =>
        {
            Console.WriteLine("Annulation demandée...");
            cts.Cancel();
            e.Cancel = true;
        };

        processor.Process(nbMessages, cts.Token);
    }
}
