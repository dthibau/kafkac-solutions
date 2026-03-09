using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
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

var rounded = builder.Stream<string, Coursier, StringSerDes, SchemaAvroSerDes<Coursier>>("position-avro")
    .MapValues<Coursier>((coursier, ctx) =>
    {
        var r = new Coursier
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
        Console.WriteLine($"Coursier {r.id} : lat={r.position.latitude}, lng={r.position.longitude}");
        return r;
    });

// Inversion clé/valeur
rounded
    .Map<string, string>((key, coursier, ctx) =>
        KeyValuePair.Create($"{coursier.position.latitude},{coursier.position.longitude}", coursier.id.ToString()))
    .To<StringSerDes, StringSerDes>("position-by-location");

// Branch : séparer nord (lat > 45) et sud (lat <= 45)
var branches = rounded.Branch((key, coursier, ctx) => coursier.position.latitude > 45,
                              (key, coursier, ctx) => coursier.position.latitude <= 45);

branches[0].To<StringSerDes, SchemaAvroSerDes<Coursier>>("position-nord");
branches[1].To<StringSerDes, SchemaAvroSerDes<Coursier>>("position-sud");

// Count : nombre de messages par position
var positionStream = rounded
    .Map<string, string>((key, coursier, ctx) =>
        KeyValuePair.Create($"{coursier.position.latitude},{coursier.position.longitude}", coursier.id.ToString()));

positionStream
    .GroupByKey<StringSerDes, StringSerDes>()
    .Count()
    .ToStream()
    .Peek((key, count, ctx) => Console.WriteLine($"Position {key} : {count} messages"))
    .To<StringSerDes, Int64SerDes>("position-count");

// Windowed Count : nombre de messages par position sur une fenêtre de 30 secondes
positionStream
    .GroupByKey<StringSerDes, StringSerDes>()
    .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromSeconds(30)))
    .Count()
    .ToStream()
    .Peek((key, count, ctx) => Console.WriteLine($"Position {key.Key} [{key.Window.StartTime:HH:mm:ss} - {key.Window.EndTime:HH:mm:ss}] : {count} messages"))
    .Map<string, long>((key, count, ctx) =>
        KeyValuePair.Create($"{key.Key}|{key.Window.StartTime:HH:mm:ss}-{key.Window.EndTime:HH:mm:ss}", count))
    .To<StringSerDes, Int64SerDes>("position-count-windowed");

// GroupBy + Aggregate : nombre de coursiers actuellement présents à chaque position
// 1. KTable<coursierId, position> : dernière position connue de chaque coursier
var coursiersTable = rounded
    .Map<string, string>((key, coursier, ctx) =>
        KeyValuePair.Create(coursier.id.ToString(), $"{coursier.position.latitude},{coursier.position.longitude}"))
    .GroupByKey<StringSerDes, StringSerDes>()
    .Reduce((oldValue, newValue) => newValue);

// 2. GroupBy position → Aggregate avec adder/subtractor
coursiersTable
    .GroupBy<string, string, StringSerDes, StringSerDes>((coursierId, position, ctx) =>
        KeyValuePair.Create(position, coursierId))
    .Aggregate<long, Int64SerDes>(
        () => 0L,
        (position, coursierId, count) => count + 1,   // adder : un coursier arrive
        (position, coursierId, count) => count - 1)   // subtractor : un coursier part
    .ToStream()
    .Peek((position, count, ctx) => Console.WriteLine($"Position {position} : {count} coursier(s) présent(s)"))
    .To<StringSerDes, Int64SerDes>("coursiers-par-position");

var topology = builder.Build();
var stream = new KafkaStream(topology, config);

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    stream.Dispose();
};

await stream.StartAsync();
