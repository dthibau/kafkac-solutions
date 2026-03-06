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
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            EnableIdempotence = true,
            TransactionalId = "exactly-once-processor"
        };

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = _groupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            IsolationLevel = IsolationLevel.ReadCommitted
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();
        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

        producer.InitTransactions(TimeSpan.FromSeconds(10));

        consumer.Subscribe(_inputTopic);

        int messagesTraites = 0;

        try
        {
            while (messagesTraites < nbMessages && !cancellationToken.IsCancellationRequested)
            {
                // Récupération d'un lot de messages
                var batch = ConsumeBatch(consumer, cancellationToken, Math.Min(10, nbMessages - messagesTraites));

                if (batch.Count == 0)
                    continue;

                producer.BeginTransaction();

                try
                {
                    // Transformation et envoi de chaque message du lot
                    foreach (var consumeResult in batch)
                    {
                        string transformedValue = Transform(consumeResult.Message.Value);

                        producer.Produce(_outputTopic, new Message<string, string>
                        {
                            Key = consumeResult.Message.Key,
                            Value = transformedValue
                        });
                    }

                    // Commit atomique : offsets consommés + messages produits
                    var lastByPartition = new List<TopicPartitionOffset>();
                    foreach (var cr in batch)
                    {
                        var existing = lastByPartition.FindIndex(tp => tp.TopicPartition == cr.TopicPartition);
                        if (existing >= 0)
                        {
                            if (cr.Offset > lastByPartition[existing].Offset)
                                lastByPartition[existing] = new TopicPartitionOffset(cr.TopicPartition, cr.Offset + 1);
                        }
                        else
                        {
                            lastByPartition.Add(new TopicPartitionOffset(cr.TopicPartition, cr.Offset + 1));
                        }
                    }

                    producer.SendOffsetsToTransaction(
                        lastByPartition,
                        consumer.ConsumerGroupMetadata,
                        TimeSpan.FromSeconds(10)
                    );

                    producer.CommitTransaction();

                    messagesTraites += batch.Count;
                    Console.WriteLine($"{messagesTraites}/{nbMessages} messages traités (batch de {batch.Count})");
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Erreur lors de la transaction, abort : {e.Message}");
                    producer.AbortTransaction();
                }
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
