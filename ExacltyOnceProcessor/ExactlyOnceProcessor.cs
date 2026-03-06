using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using Confluent.Kafka;

internal class ExactlyOnceProcessor
{
    private readonly string _bootstrapServers;
    private readonly string _groupId;
    private readonly string _inputTopic;
    private readonly string _outputTopic;

    public ExactlyOnceProcessor(string bootstrapServers, string groupId, string inputTopic, string outputTopic)
    {
        _bootstrapServers = bootstrapServers;
        _groupId = groupId;
        _inputTopic = inputTopic;
        _outputTopic = outputTopic;
    }

    public void Process(int nbMessages, CancellationToken cancellationToken)
    {
        // Configuration du Producer transactionnel
        // A compléter :
        //   - BootstrapServers
        //   - EnableIdempotence = true
        //   - TransactionalId = "exactly-once-processor"
        var producerConfig = new ProducerConfig
        {
            // A compléter
        };

        // Configuration du Consumer
        // A compléter :
        //   - BootstrapServers
        //   - GroupId
        //   - EnableAutoCommit = false (les offsets sont committés via la transaction)
        //   - AutoOffsetReset = AutoOffsetReset.Earliest
        //   - IsolationLevel = IsolationLevel.ReadCommitted
        var consumerConfig = new ConsumerConfig
        {
            // A compléter
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();
        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

        // A compléter : Initialisation des transactions

        consumer.Subscribe(_inputTopic);

        int messagesTraites = 0;

        try
        {
            while (messagesTraites < nbMessages && !cancellationToken.IsCancellationRequested)
            {
                // A compléter : Récupération d'un lot de messages via ConsumeBatch
                // Puis pour chaque batch :
                //   1. BeginTransaction
                //   2. Pour chaque message du batch : Transform + Produce vers le topic de sortie
                //   3. SendOffsetsToTransaction (offsets max par partition du batch + consumer group metadata)
                //   4. CommitTransaction
                //   En cas d'erreur : AbortTransaction
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Traitement interrompu.");
        }
        finally
        {
            consumer.Close();
        }

        Console.WriteLine($"Traitement terminé. {messagesTraites} messages traités.");
    }

    /// <summary>
    /// Consomme un lot de messages (jusqu'à maxBatchSize).
    /// Le premier appel bloque en attente d'un message, les suivants ont un timeout court.
    /// </summary>
    private List<ConsumeResult<string, string>> ConsumeBatch(
        IConsumer<string, string> consumer, CancellationToken cancellationToken, int maxBatchSize)
    {
        var batch = new List<ConsumeResult<string, string>>();

        while (batch.Count < maxBatchSize)
        {
            var result = consumer.Consume(batch.Count == 0 ? cancellationToken : new CancellationTokenSource(500).Token);

            if (result == null)
                break;

            if (result.IsPartitionEOF)
                continue;

            batch.Add(result);
        }

        return batch;
    }

    /// <summary>
    /// Transforme un message JSON position en ajoutant la distance au point d'origine (0,0).
    /// </summary>
    private string Transform(string jsonValue)
    {
        using var doc = JsonDocument.Parse(jsonValue);
        var root = doc.RootElement;

        double latitude = 0, longitude = 0;

        // Le message peut contenir un objet Position directement ou un Coursier avec une Position
        if (root.TryGetProperty("Position", out var posElement))
        {
            latitude = posElement.GetProperty("Latitude").GetDouble();
            longitude = posElement.GetProperty("Longitude").GetDouble();
        }
        else if (root.TryGetProperty("Latitude", out _))
        {
            latitude = root.GetProperty("Latitude").GetDouble();
            longitude = root.GetProperty("Longitude").GetDouble();
        }

        double distance = Math.Sqrt(latitude * latitude + longitude * longitude);

        return JsonSerializer.Serialize(new
        {
            latitude,
            longitude,
            distanceFromOrigin = Math.Round(distance, 2)
        });
    }
}
