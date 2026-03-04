using Confluent.Kafka;
using System;
using System.Text.Json;

internal class CustomSerializer<T> : ISerializer<T>
{
    public byte[] Serialize(T data, SerializationContext context)
    {
        if (data == null)
            return Array.Empty<byte>();

        // Sérialiser l'objet en JSON
        return JsonSerializer.SerializeToUtf8Bytes(data);
    }
}


