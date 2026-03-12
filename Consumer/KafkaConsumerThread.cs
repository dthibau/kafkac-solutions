using Confluent.Kafka;
using KafkaConsumer.model;
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

        public void StartConsuming(CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = _groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false
            };

            using var consumer = new ConsumerBuilder<string, Coursier>(config)
                .SetValueDeserializer(new CustomDeserializer<Coursier>())
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    Console.WriteLine($"Partitions assignées : {string.Join(", ", partitions)}");
                    var offsets = partitions.Select(topicPartition =>
                    {
                        long? lastOffset = GetOffsetByPartitionId(topicPartition.Partition.Value);

                        if (lastOffset.HasValue)
                        {
                            Console.WriteLine($"Reprise partition {topicPartition.Partition} à l'offset {lastOffset.Value + 1}");
                            return new TopicPartitionOffset(
                                topicPartition.Topic,
                                topicPartition.Partition,
                                new Offset(lastOffset.Value + 1)
                            );
                        }
                        else
                        {
                            Console.WriteLine($"Aucun offset trouvé pour partition {topicPartition.Partition}, début par défaut.");
                            return new TopicPartitionOffset(
                                topicPartition.Topic,
                                topicPartition.Partition,
                                Offset.Unset // laisse AutoOffsetReset décider
                            );
                        }
                    }).ToList();

                    return offsets; // ← clé de la solution




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

                    Console.WriteLine($"Message reçu : clé = {consumeResult.Message.Key}, partition = {consumeResult.Partition}, offset = {consumeResult.Offset}");

                    long coursierId = long.Parse(consumeResult.Message.Key);
                    InsertIntoPostgres(coursierId, consumeResult.Partition.Value, consumeResult.Offset.Value);

                    consumer.StoreOffset(consumeResult);
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

        private void InsertIntoPostgres(long key, long partitionId, long offset)
        {
            using (var conn = new NpgsqlConnection(_connectionString))
            {
                conn.Open();

                using (var cmd = new NpgsqlCommand("INSERT INTO coursier (coursierId, partitionId, kafkaOffset) VALUES (@key, @partitionId, @offset)", conn))
                {
                    cmd.Parameters.AddWithValue("key", key);
                    cmd.Parameters.AddWithValue("partitionId", partitionId);
                    cmd.Parameters.AddWithValue("offset", offset);

                    try
                    {
                        cmd.ExecuteNonQuery();
                        Console.WriteLine($"Insertion réussie : clé = {key}, partition = {partitionId},  offset = {offset}");
                    }
                    catch (PostgresException ex)
                    {
                        Console.WriteLine($"Erreur lors de l'insertion dans PostgreSQL : {ex.Message}");
                    }
                }
            }
        }
        private long? GetOffsetByPartitionId(int partitionId)
        {
            using (var conn = new NpgsqlConnection(_connectionString))
            {
                conn.Open();

                // On suppose que vous avez une colonne partitionId dans votre table
                string sql = "SELECT kafkaOffset FROM coursier WHERE partitionId = @partitionId ORDER BY kafkaOffset desc LIMIT 1";

                using (var cmd = new NpgsqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("partitionId", partitionId);

                    try
                    {
                        // ExecuteScalar renvoie le premier élément de la première ligne
                        object result = cmd.ExecuteScalar();

                        // Si aucune ligne n'est trouvée, result sera null
                        if (result != null && result != DBNull.Value)
                        {
                            return Convert.ToInt64(result);
                        }

                        return null; // Pas d'offset trouvé pour cette partition
                    }
                    catch (PostgresException ex)
                    {
                        Console.WriteLine($"Erreur lors de la lecture PostgreSQL : {ex.Message}");
                        return null;
                    }
                }
            }
        }
    }
}
