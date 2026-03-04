using System;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaProducer.Formation.model;
using static Confluent.Kafka.ConfigPropertyNames;

internal class ProducerThread
{
    private readonly IProducer<string, Coursier> _producer;
    private readonly string _topic;
    private readonly SendMode _sendMode;

    public ProducerThread(string bootstrapServers, string topic, SendMode sendMode)
    {
       // A compléter
    }

    public async Task StartProducing(int threadIndex, int nbMessages)
    {
        Coursier coursier = new Coursier(threadIndex, new Position(45, 45));

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
                       // A compléter
                        break;

                    case SendMode.SYNCHRONE:
                       // A compléter
                        break;

                    case SendMode.ASYNCHRONE:
                       // A compléter
                        break;

                    default:
                        throw new InvalidOperationException($"Mode inconnu : {_sendMode}");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Erreur lors de l'envoi du message '{message}': {e.Message}");
            }

            Thread.Sleep(100); // Simule une pause entre les envois
        }
    }

    public void Dispose()
    {
      // A compléter
    }
}