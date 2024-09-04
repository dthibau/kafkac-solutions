using System;
using System.Numerics;
using System.Threading.Tasks;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using model;
using static Confluent.Kafka.ConfigPropertyNames;

internal class ProducerThread
{
    private readonly ProducerConfig _config;
    private readonly string _topic;
    private readonly IProducer<long, Coursier> _producer;

    public ProducerThread(string bootstrapServers, string topic, SendMode sendMode, CachedSchemaRegistryClient schemaRegistry)
    {
        _topic = topic;
        if (sendMode == SendMode.FIRE_AND_FORGET)
        {
            _config = new ProducerConfig { BootstrapServers = bootstrapServers, EnableDeliveryReports = false };
        }
        else
        {
            _config = new ProducerConfig { BootstrapServers = bootstrapServers };
        }
        _producer = new ProducerBuilder<long, Coursier>(_config)
            .SetKeySerializer(Serializers.Int64)
           .SetValueSerializer(new AvroSerializer<Coursier>(schemaRegistry))
            .Build();
    }

    public void ProduceFireAndForget(Coursier coursier)
    {
            Message<long, Coursier> message = new Message<long, Coursier>
            {
                Value = coursier,
                Key = coursier.id
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
                Key = coursier.id
            };
            try
            {
                var deliveryResult = _producer.ProduceAsync(_topic, message).GetAwaiter().GetResult();
               // Console.WriteLine($"Synchronously: Message '{message}' envoyé à partition {deliveryResult.Partition} [offset {deliveryResult.Offset}]");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Erreur lors de l'envoi synchrone du message '{message}': {e.Message}");
            }

    }

    public async Task ProduceAsynchronously(Coursier coursier)
    {

            Message<long, Coursier> message = new Message<long, Coursier>
            {
                Value = coursier,
                Key = coursier.id
            };
            try
            {
                var deliveryResult = await _producer.ProduceAsync(_topic, message);
             //   Console.WriteLine($"Asynchronously: Message '{message}' envoyé à partition {deliveryResult.Partition} [offset {deliveryResult.Offset}]");
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
