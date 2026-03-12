using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using model;

internal class ProducerThread : IDisposable
{
    private readonly IProducer<string, Coursier> _producer;
    private readonly ISchemaRegistryClient _schemaRegistry;
    private readonly string _topic;
    private readonly SendMode _sendMode;

    public ProducerThread(string bootstrapServers, string schemaRegistryUrl, string topic, SendMode sendMode)
    {
        _topic = topic;
        _sendMode = sendMode;

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        };

        if (sendMode == SendMode.FIRE_AND_FORGET)
        {
            config.EnableDeliveryReports = false;
        }

        _schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
        {
            Url = schemaRegistryUrl
        });

        ISerializer<Coursier> serializer;

        var baseAvroSerializer = new AvroSerializer<Coursier>(_schemaRegistry);

        if ( sendMode == SendMode.ASYNCHRONE )
        {
            _producer = new ProducerBuilder<string, Coursier>(config)
                        .SetValueSerializer(baseAvroSerializer.AsSyncOverAsync())
                        .Build();
        } else
        {
            _producer = new ProducerBuilder<string, Coursier>(config)
                       .SetValueSerializer(baseAvroSerializer)
                       .Build();
        }




    }

    public async Task StartProducing(int threadIndex, int nbMessages)
    {
        var coursier = new Coursier
        {
            id = threadIndex,
            first_name = threadIndex == 0 ? null : $"Coursier-{threadIndex}",
            position = new Position { latitude = 45, longitude = 45 }
        };

        for (int i = 0; i < nbMessages; i++)
        {
            Move(coursier);

            var message = new Message<string, Coursier>
            {
                Key = coursier.id.ToString(),
                Value = coursier
            };

            try
            {
                switch (_sendMode)
                {
                    case SendMode.FIRE_AND_FORGET:
                        await _producer.ProduceAsync(_topic, message);
                        break;

                    case SendMode.SYNCHRONE:
                        var result = _producer.ProduceAsync(_topic, message).GetAwaiter().GetResult();
                        Console.WriteLine($"[Thread {threadIndex}] Message envoyé: partition={result.Partition}, offset={result.Offset}");
                        break;

                    case SendMode.ASYNCHRONE:
                         _producer.Produce(_topic, message, deliveryReport =>
                        {
                            if (deliveryReport.Error.Code != ErrorCode.NoError)
                            {
                                Console.WriteLine($"[Thread {threadIndex}] Erreur: {deliveryReport.Error.Reason}");
                            }
                            else
                            {
                            Console.WriteLine($"[Thread {threadIndex}] Message envoyÃ©: partition={deliveryReport.Partition}, offset={deliveryReport.Offset}");
                                                        }
                            });
                        break;

                    default:
                        throw new InvalidOperationException($"Mode inconnu : {_sendMode}");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Erreur lors de l'envoi du message '{message}': {e.Message}");
            }

            Thread.Sleep(100);
        }

        _producer.Flush(TimeSpan.FromSeconds(10));
    }

    private static void Move(Coursier coursier)
    {
        var random = new Random();
        coursier.position.latitude += random.NextDouble() - 0.5;
        coursier.position.longitude += random.NextDouble() - 0.5;
    }

    public void Dispose()
    {
        _producer?.Dispose();
        _schemaRegistry?.Dispose();
    }
}
