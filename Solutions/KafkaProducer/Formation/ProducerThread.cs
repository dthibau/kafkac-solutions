using System;
using System.Numerics;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaProducer.Formation.model;
using static Confluent.Kafka.ConfigPropertyNames;

internal class ProducerThread : IDisposable
{
    private readonly ProducerConfig _config;
    private readonly string _topic;
    private readonly IProducer<long, Coursier> _producer;

    public ProducerThread(string bootstrapServers, string topic, SendMode sendMode)
    {
        _topic = topic;
        if (sendMode == SendMode.FIRE_AND_FORGET)
        {
            _config = new ProducerConfig { BootstrapServers = bootstrapServers, 
                EnableDeliveryReports = false };
        }
        else
        {
            _config = new ProducerConfig { BootstrapServers = bootstrapServers };
        }
        _producer = new ProducerBuilder<long, Coursier>(_config)
            .SetValueSerializer(new CustomSerializer<Coursier>())
            .Build();
    }

    public void ProduceFireAndForget(Coursier coursier)
    {
            Message<long, Coursier> message = new Message<long, Coursier>
            {
                Value = coursier,
                Key = coursier.Id
            };
            try
            {

                _producer.Produce(_topic,message);
               // Console.WriteLine($"Fire-and-Forget: Message '{message}' envoyé.");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Erreur lors de l'envoi du message '{message}': {e.Message}");
            }
    }
    public void ProduceSynchronously(Coursier coursier)
    {

            Message<long, Coursier> message = new Message<long, Coursier>
            {
                Value = coursier,
                Key = coursier.Id
            };
            try
            {
            var task = _producer.ProduceAsync(_topic, message);
             var deliveryResult =task .Wait(10000) ? task.Result  : null;
               // Console.WriteLine($"Synchronously: Message '{message}' envoyé à partition {deliveryResult.Partition} [offset {deliveryResult.Offset}]");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Erreur lors de l'envoi synchrone du message '{message}': {e.Message}");
            }

    }

    public void  ProduceAsynchronously(Coursier coursier)
    {

            Message<long, Coursier> message = new Message<long, Coursier>
            {
                Value = coursier,
                Key = coursier.Id
            };
            try
            {
                 _producer.Produce(_topic, message,
                    (deliveryResult =>
                    {
                        if (deliveryResult.Error.IsError)
                            Console.WriteLine($"Asynchronously: Erreur");

                    }));
            }
            catch (Exception e)
            {
                Console.WriteLine($"Erreur lors de l'envoi asynchrone du message '{message}': {e.Message}");
            }
     }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }


}
