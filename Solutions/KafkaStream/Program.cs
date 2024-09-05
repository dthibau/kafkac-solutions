﻿using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Stream;
using model;

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

            builder.Stream<long, Coursier>("position-avro")
                .MapValues(c =>
                {
                Position position = (Position)c.position;
                position.latitude = Math.Round(position.latitude, 1);
                position.longitude = Math.Round(position.longitude, 1);
                    return c;
                })
                .SelectKey((k, v) => (Position)v.position)
                .MapValues(c => c.id)
                .To<SchemaAvroSerDes<Position>, Int64SerDes>("position-output"); ;

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
