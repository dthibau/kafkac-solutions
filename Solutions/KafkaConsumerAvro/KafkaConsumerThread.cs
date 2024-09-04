using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Npgsql;
using System;
using System.Threading;

namespace KafkaConsumer
{
    internal class KafkaConsumerThread
    {
        private readonly string _bootstrapServers;
        private readonly string _groupId;
        private readonly string _topic;
        private readonly string _connectionString;

        public KafkaConsumerThread(string bootstrapServers, string groupId, string topic, string connectionString)
        {
            _bootstrapServers = bootstrapServers;
            _groupId = groupId;
            _topic = topic;
            _connectionString = connectionString;
        }

        public void StartConsuming(string schemaRegistryUrl, CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = _groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = schemaRegistryUrl });


            using (var consumer = new ConsumerBuilder<long, GenericRecord>(config)
                .SetKeyDeserializer(Deserializers.Int64)
                .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync())
                .SetPartitionsRevokedHandler((c,partitions) =>
                {
                    Console.WriteLine($"Partitions revoked : {partitions}");
                })
                .SetPartitionsAssignedHandler((c, partitions) => { Console.WriteLine($"Partitions assigned : {partitions}"); })
                .Build())
            {
                consumer.Subscribe(_topic);

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);

                        Console.WriteLine($"Message consommé : clé = {consumeResult.Message.Key}, offset = {consumeResult.Offset}");

                        GenericRecord record = consumeResult.Message.Value;
                        Console.WriteLine($"Position :  {record["position"]}");

                        // Insérer dans PostgreSQL
                        InsertIntoPostgres(consumeResult.Message.Key, consumeResult.Offset.Value);
                    }
                    catch (ConsumeException ex)
                    {
                        Console.WriteLine($"Erreur de consommation : {ex.Error.Reason}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Erreur inattendue : {ex.Message}");
                    }
                }
            }
        }

        private void InsertIntoPostgres(long key, long offset)
        {
            using (var conn = new NpgsqlConnection(_connectionString))
            {
                conn.Open();

                using (var cmd = new NpgsqlCommand("INSERT INTO coursier (coursierId, kafkaOffset) VALUES (@key, @offset)", conn))
                {
                    cmd.Parameters.AddWithValue("key", key);
                    cmd.Parameters.AddWithValue("offset", offset);

                    try
                    {
                        cmd.ExecuteNonQuery();
                        Console.WriteLine($"Insertion réussie : clé = {key}, offset = {offset}");
                    }
                    catch (PostgresException ex)
                    {
                        Console.WriteLine($"Erreur lors de l'insertion dans PostgreSQL : {ex.Message}");
                    }
                }
            }
        }
    }
}
