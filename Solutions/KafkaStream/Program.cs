using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

using model;
using Streamiz.Kafka.Net.State;

namespace PositionStream
{
    internal class Program
    {
        // Map pour stocker la dernière région de chaque coursier
        private static Dictionary<long, Position> dernieresPosition= new Dictionary<Int64, Position>();

        static async Task Main(string[] args)
        {
            var config = new StreamConfig<Int64SerDes, SchemaAvroSerDes<Coursier>>();
            config.ApplicationId = "position-stream";
            config.BootstrapServers = "localhost:19092";
            config.SchemaRegistryUrl = "http://localhost:8081"; // URL of your Schema Registry
            config.AutoOffsetReset = AutoOffsetReset.Earliest;

            StreamBuilder builder = new StreamBuilder();

            var branches = builder.Stream<long, Coursier>("position-avro")
                .MapValues(c =>
                {
                    Position position = (Position)c.position;
                    position.latitude = Math.Round(position.latitude, 1);
                    position.longitude = Math.Round(position.longitude, 1);
                    return c;
                })
                .SelectKey((k, v) => (Position)v.position)
                .MapValues(c => c.id)
                .Branch((k, v) => k.latitude >= 45.0,
                (k, v) => true);

            branches[0].GroupByKey<SchemaAvroSerDes<Position>, Int64SerDes>()
                .Aggregate(() => new List<Int64>(), 
                (position, coursierId, aggregate) =>
                {
                    if (!aggregate.Contains(coursierId))
                    {
                        aggregate.Add(coursierId);
                    }
                    return aggregate;
                },
                InMemory.As<Position, List<long>>("CountStore1")
                        .WithValueSerdes<JsonSerDes<List<long>>>()
                        .WithKeySerdes<SchemaAvroSerDes<Position>>())
                .ToStream()
                .FlatMap((k, v) =>
                {
                    List<KeyValuePair<Position, List<long>>> results = new List<KeyValuePair<Position, List<long>>>();
                    results.Add(new KeyValuePair<Position, List<long>>(k, v));
                    Console.WriteLine("Position : " + k.latitude + ":" + k.longitude + " Couriers " + String.Join(", ", v));
                    Console.WriteLine("Dernières positions :");
                    foreach (KeyValuePair<long, Position> entry in dernieresPosition)
                    {
                        Console.WriteLine($"Clé : {entry.Key}, Valeur : {entry.Value}");
                    }
                    for (int i = 0; i < v.Count; i++)
                    {
                        if (dernieresPosition.TryGetValue(v[i], out var position))
                        {
                           
                            if (!(position.latitude == k.latitude && position.longitude == k.longitude))
                            {
                                Console.WriteLine("Coursier   : " + v[i] + " has Changed new is " + k.latitude + ":" + k.longitude + " / old is" + position.latitude + ":" +position.longitude);
                                Position oldPosition = dernieresPosition[v[i]];
                                dernieresPosition[v[i]] = k;
                                List<long> otherCoursiers = new List<long>();
                                foreach (var item in dernieresPosition)
                                {
                                    if (item.Value.latitude == oldPosition.latitude && item.Value.longitude == oldPosition.longitude)
                                    {
                                        otherCoursiers.Add(item.Key);
                                    }
                                }
                                results.Add(new KeyValuePair<Position, List<long>>(oldPosition, otherCoursiers));
                            }
                        }
                    }
                    for (int i = 0; i < v.Count; i++)
                    {
                        dernieresPosition[v[i]] = k;
                    }
                    Console.WriteLine("Dernières positions :");
                    foreach (KeyValuePair<long, Position> entry in dernieresPosition)
                    {
                        Console.WriteLine($"Clé : {entry.Key}, Valeur : {entry.Value}");
                    }


                    return results;
                })
                .To<SchemaAvroSerDes<Position>, JsonSerDes<List<long>>>("position-coursiers");




            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);

            // Subscribe CTRL + C to quit stream application
            Console.CancelKeyPress += (o, e) =>
            {
                stream.Dispose();
            };

            // Start stream instance with cancellable token
            await stream.StartAsync();

        }
    }
}
