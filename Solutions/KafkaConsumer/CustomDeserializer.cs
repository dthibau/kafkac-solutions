using Confluent.Kafka;
using System;
using System.Text.Json;

internal class CustomDeserializer<T> : IDeserializer<T>
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull || data.Length == 0)
        {
            // Si les données sont nulles ou vides, retourner la valeur par défaut de T
            return default;
        }

        // Sérialiser l'objet en JSON
        return JsonSerializer.Deserialize<T>(data);
    }
}


