using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaProducer.Formation.model;

internal class ProducerThread : IDisposable
{
    private readonly IProducer<string, Coursier> _producer;
    private readonly string _topic;
    private readonly SendMode _sendMode;

    public ProducerThread(string bootstrapServers, string topic, SendMode sendMode)
    {
        _topic = topic;
        _sendMode = sendMode;

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            EnableIdempotence = true,
            TransactionalId = $"producer-{Guid.NewGuid()}",
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SslCaLocation = @"C:\Users\PLB\kafka\TPsC\9_securite\9.2.2_OAuth\ssl\mount\ca-cert.pem",
            SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.None,
            SaslMechanism = SaslMechanism.OAuthBearer,
            SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc,
            SaslOauthbearerClientId = "kafka-producer-client",
            SaslOauthbearerClientSecret = "producer-secret",
            SaslOauthbearerTokenEndpointUrl = "http://localhost:9090/realms/kafka/protocol/openid-connect/token",
            StatisticsIntervalMs = 5000
        };

        if (sendMode == SendMode.FIRE_AND_FORGET)
        {
            config.EnableDeliveryReports = false;
        }

        _producer = new ProducerBuilder<string, Coursier>(config)
            .SetValueSerializer(new CustomSerializer<Coursier>())
            .SetStatisticsHandler((_, json) =>
            {
                try
                {
                    using var doc = JsonDocument.Parse(json);
                    var root = doc.RootElement;
                    if (root.TryGetProperty("brokers", out var brokers))
                    {
                        foreach (var broker in brokers.EnumerateObject())
                        {
                            var b = broker.Value;
                            if (b.TryGetProperty("throttle", out var throttle))
                            {
                                var cnt = throttle.GetProperty("cnt").GetInt64();
                                var sum = throttle.GetProperty("sum").GetInt64();
                                if (cnt > 0)
                                {
                                    Console.WriteLine($"[THROTTLE] Broker {broker.Name} : {cnt} requêtes throttlées, temps total = {sum} ms");
                                }
                            }
                        }
                    }
                }
                catch { /* ignore parsing errors */ }
            })
            .Build();

        _producer.InitTransactions(TimeSpan.FromSeconds(10));
    }

    public async Task StartProducing(int threadIndex, int nbMessages)
    {
        Coursier coursier = new Coursier(threadIndex, new Position(45, 45));

        _producer.BeginTransaction();

        for (int i = 0; i < nbMessages; i++)
        {
            coursier.move();

            var message = new Message<string, Coursier>
            {
                Key = coursier.Id.ToString(),
                Value = coursier
            };

            try
            {
                switch (_sendMode)
                {
                    case SendMode.FIRE_AND_FORGET:
                        _producer.Produce(_topic, message);
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
                           /* else
                            {
                                Console.WriteLine($"[Thread {threadIndex}] Message envoyé: partition={deliveryReport.Partition}, offset={deliveryReport.Offset}");
                            }*/
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

            if ((i + 1) % 10 == 0)
            {
                _producer.CommitTransaction();
                Console.WriteLine($"[Thread {threadIndex}] Transaction committée après {i + 1} messages");
                _producer.BeginTransaction();
            }

            Thread.Sleep(1);
        }

        // Les messages restants (non multiple de 10) ne sont pas committés
        _producer.Flush(TimeSpan.FromSeconds(10));
    }

    public void Dispose()
    {
        _producer?.Dispose();
    }
}
