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
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromMinutes(5)))
                .Count(InMemoryWindows.As<Position, long>("CountStore1").WithValueSerdes<Int64SerDes>().WithKeySerdes<SchemaAvroSerDes<Position>>()).ToStream()
                .Map((windowKey, count) => new KeyValuePair<Position, long>(windowKey.Key, count))
                .To<SchemaAvroSerDes<Position>, SchemaAvroSerDes<long>>("position-output-count-window-more");

            branches[1].GroupByKey<SchemaAvroSerDes<Position>, Int64SerDes>()
    .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromMinutes(5)))
    .Count(InMemoryWindows.As<Position, long>("CountStore2").WithValueSerdes<Int64SerDes>().WithKeySerdes<SchemaAvroSerDes<Position>>()).ToStream()
    .Map((windowKey, count) => new KeyValuePair<Position, long>(windowKey.Key, count))
    .To<SchemaAvroSerDes<Position>, SchemaAvroSerDes<long>>("position-output-count-window-less");


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
