using System;
using System.Threading;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;

namespace KafkaConsumerAvro
{
    internal class KafkaConsumerThread
    {
        private readonly string _bootstrapServers;
        private readonly string _schemaRegistryUrl;
        private readonly string _groupId;
        private readonly string _topic;

        public KafkaConsumerThread(string bootstrapServers, string schemaRegistryUrl, string groupId, string topic)
        {
            _bootstrapServers = bootstrapServers;
            _schemaRegistryUrl = schemaRegistryUrl;
            _groupId = groupId;
            _topic = topic;
        }

        public void StartConsuming(CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = _groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
            };

            using var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
            {
                Url = _schemaRegistryUrl
            });

            using var consumer = new ConsumerBuilder<string, GenericRecord>(config)
                .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync())
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    Console.WriteLine($"Partitions assignées : {string.Join(", ", partitions)}");
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    Console.WriteLine($"Partitions révoquées : {string.Join(", ", partitions)}");
                })
                .Build();

            consumer.Subscribe(_topic);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(cancellationToken);
                    var record = consumeResult.Message.Value;

                    long id = (long)record["id"];
                    var position = (GenericRecord)record["position"];
                    double latitude = (double)position["latitude"];
                    double longitude = (double)position["longitude"];

                    Console.WriteLine($"Message reçu : clé={consumeResult.Message.Key}, id={id}, lat={latitude}, lng={longitude}, partition={consumeResult.Partition}, offset={consumeResult.Offset}");
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Consommation annulée.");
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}
