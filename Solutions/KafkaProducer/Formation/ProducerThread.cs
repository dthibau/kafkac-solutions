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
<<<<<<< HEAD
    private readonly IProducer<string, Coursier> _producer;
=======
    private readonly IProducer<long, Coursier> _producer;
    private int nbSend = 0;
>>>>>>> Producteur transactionnel

    public ProducerThread(string bootstrapServers, string topic, SendMode sendMode)
    {
        _topic = topic;
        if (sendMode == SendMode.FIRE_AND_FORGET)
        {
            _config = new ProducerConfig { BootstrapServers = bootstrapServers, EnableDeliveryReports = false, TransactionalId = "tx-position" };
        }
        else
        {
            _config = new ProducerConfig { BootstrapServers = bootstrapServers, TransactionalId = "tx-position" };
        }
        _producer = new ProducerBuilder<string, Coursier>(_config)
            .SetValueSerializer(new CustomSerializer<Coursier>())
            .Build();
        _producer.InitTransactions(TimeSpan.FromSeconds(30));
        _producer.BeginTransaction();
    }

    public void ProduceFireAndForget(Coursier coursier)
    {
            Message<string, Coursier> message = new Message<string, Coursier>
            {
                Value = coursier,
                Key = coursier.Id.ToString()
            };
            try
            {

                _producer.Produce(_topic,message);
                // Console.WriteLine($"Fire-and-Forget: Message '{message}' envoyé.");
                nbSend++;
                if (nbSend % 10 == 0)
                {
                    _producer.CommitTransaction();
                    _producer.BeginTransaction();
                    nbSend = 0;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Erreur lors de l'envoi du message '{message}': {e.Message}");
            }
       
    }
    public void ProduceSynchronously(Coursier coursier)
    {

        Message<string, Coursier> message = new Message<string, Coursier>
        {
            Value = coursier,
            Key = coursier.Id.ToString()
        };
        try
            {

            var task = _producer.ProduceAsync(_topic, message);
             var deliveryResult =task .Wait(10000) ? task.Result  : null;
               // Console.WriteLine($"Synchronously: Message '{message}' envoyé à partition {deliveryResult.Partition} [offset {deliveryResult.Offset}]");
               nbSend++;
                if (nbSend % 10 == 0)
                {
                    _producer.CommitTransaction();
                    _producer.BeginTransaction();
                    nbSend = 0;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Erreur lors de l'envoi synchrone du message '{message}': {e.Message}");
            }

    }

    public void  ProduceAsynchronously(Coursier coursier)
    {

        Message<string, Coursier> message = new Message<string, Coursier>
        {
            Value = coursier,
            Key = coursier.Id.ToString()
        };
        try
            {
                 _producer.Produce(_topic, message,
                    (deliveryResult =>
                    {
                        if (deliveryResult.Error.IsError)
                            Console.WriteLine($"Asynchronously: Erreur");

                    }));

            //   Console.WriteLine($"Asynchronously: Message '{message}' envoyé à partition {deliveryResult.Partition} [offset {deliveryResult.Offset}]");
                 nbSend++;
                if (nbSend % 10 == 0)
                {
                    _producer.CommitTransaction();
                    _producer.BeginTransaction();
                    nbSend = 0;
                }
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
