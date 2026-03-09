using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Confluent.SchemaRegistry;
using model;

var config = new StreamConfig
{
    ApplicationId = "position-stream",
    BootstrapServers = "localhost:19092,localhost:19093,localhost:19094",
    SchemaRegistryUrl = "http://localhost:8081",
    AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest
};

var builder = new StreamBuilder();

builder.Stream<string, Coursier, StringSerDes, SchemaAvroSerDes<Coursier>>("position-avro")
    .MapValues<Coursier>((coursier, ctx) =>
    {
        var rounded = new Coursier
        {
            id = coursier.id,
            first_name = coursier.first_name,
            vehicle_id = coursier.vehicle_id,
            position = new Position
            {
                latitude = Math.Round(coursier.position.latitude),
                longitude = Math.Round(coursier.position.longitude)
            }
        };
        Console.WriteLine($"Coursier {rounded.id} : lat={rounded.position.latitude}, lng={rounded.position.longitude}");
        return rounded;
    })
    .Map<string, string>((key, coursier, ctx) =>
        KeyValuePair.Create($"{coursier.position.latitude},{coursier.position.longitude}", coursier.id.ToString()))
    .To<StringSerDes, StringSerDes>("position-by-location");

var topology = builder.Build();
var stream = new KafkaStream(topology, config);

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    stream.Dispose();
};

await stream.StartAsync();
