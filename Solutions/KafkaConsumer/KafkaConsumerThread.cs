using Confluent.Kafka;
using KafkaConsumer.model;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = _groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = _bootstrapServers,
                TransactionalId = "tx-tostring"
            };
            using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
            using (var consumer = new ConsumerBuilder<long, Coursier>(consumerConfig)
                .SetKeyDeserializer(Deserializers.Int64)
                .SetValueDeserializer(new CustomDeserializer<Coursier>())
                .SetPartitionsRevokedHandler((c,partitions) =>
                {
                    Console.WriteLine($"Partitions revoked : {partitions}");
                })
                .SetPartitionsAssignedHandler((c, partitions) => { Console.WriteLine($"Partitions assigned : {partitions}"); })
                .Build())
            {
                consumer.Subscribe(_topic);
                producer.InitTransactions(TimeSpan.FromSeconds(30));
                producer.BeginTransaction();
                var lastTxnCommit = DateTime.Now;
                var txnCommitPeriod = TimeSpan.FromSeconds(10);

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));

                        if ( consumeResult !=  null )
                        {
                            Console.WriteLine($"Message consommé : clé = {consumeResult.Message.Key}, offset = {consumeResult.Offset}");

                            // Insérer dans PostgreSQL
                           // InsertIntoPostgres(consumeResult.Message.Key, consumeResult.Offset.Value);

                            producer.Produce("position-tostring", new Message<string, string>
                            {
                                Key = consumeResult.Message.Key.ToString(),
                                Value = consumeResult.Message.Value.ToString()
                            });

                            if (DateTime.Now > lastTxnCommit + txnCommitPeriod)
                            {
                                producer.SendOffsetsToTransaction(
                                    // Note: committed offsets reflect the next message to consume, not last
                                    // message consumed. consumer.Position returns the last consumed offset
                                    // values + 1, as required.
                                    consumer.Assignment.Select(a => new TopicPartitionOffset(a, consumer.Position(a))),
                                    consumer.ConsumerGroupMetadata,
                                     TimeSpan.FromSeconds(30));
                                producer.CommitTransaction();
                                producer.BeginTransaction();
                                lastTxnCommit = DateTime.Now;
                            }
                        }
                        
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
