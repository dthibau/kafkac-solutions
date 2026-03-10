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
            // --- Sécurité (décommenter pour labs 9.x) ---
            // SecurityProtocol = SecurityProtocol.SaslSsl,
            // SslCaLocation = @"C:\Users\PLB\kafka\TPsC\9_securite\9.2.2_OAuth\ssl\mount\ca-cert.pem",
            // SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.None,
            // SaslMechanism = SaslMechanism.OAuthBearer,
            // SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc,
            // SaslOauthbearerClientId = "kafka-producer-client",
            // SaslOauthbearerClientSecret = "producer-secret",
            // SaslOauthbearerTokenEndpointUrl = "http://localhost:9090/realms/kafka/protocol/openid-connect/token",
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

                    Console.WriteLine("=== [PRODUCER STATS] ===");

                    // Métriques globales
                    var msgCnt = root.GetProperty("msg_cnt").GetInt64();
                    var msgSize = root.GetProperty("msg_size").GetInt64();
                    var tx = root.GetProperty("tx").GetInt64();
                    Console.WriteLine($"  Global : msg_cnt={msgCnt}, msg_size={msgSize}, tx={tx}");

                    // Métriques par broker
                    if (root.TryGetProperty("brokers", out var brokers))
                    {
                        foreach (var broker in brokers.EnumerateObject())
                        {
                            var b = broker.Value;
                            var outbufCnt = b.GetProperty("outbuf_cnt").GetInt64();
                            var txmsgs = b.GetProperty("txmsgs").GetInt64();
                            var txbytes = b.GetProperty("txbytes").GetInt64();
                            var rttAvg = b.GetProperty("rtt").GetProperty("avg").GetInt64();
                            var throttleCnt = b.GetProperty("throttle").GetProperty("cnt").GetInt64();
                            var throttleSum = b.GetProperty("throttle").GetProperty("sum").GetInt64();

                            Console.WriteLine($"  Broker {broker.Name} : outbuf_cnt={outbufCnt}, txmsgs={txmsgs}, txbytes={txbytes}, rtt.avg={rttAvg} µs, throttle(cnt={throttleCnt}, sum={throttleSum})");
                        }
                    }

                    Console.WriteLine("========================");
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
