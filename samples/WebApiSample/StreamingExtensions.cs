using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using JsonStreaming;

static class StreamingExtensions
{
    public static async Task<int> ProjectTypedAsync<TIn, TOut>(
        this PipeReader reader,
        JsonPath path,
        Utf8JsonWriter writer,
        JsonTypeInfo<TIn> inputType,
        JsonTypeInfo<TOut> outputType,
        Func<TIn, IEnumerable<TOut>> transform,
        CancellationToken ct = default)
    {
        int count = 0;
        await reader.TransformItemsAsync(
            PipeWriter.Create(Stream.Null),
            path,
            (itemBytes, _) =>
            {
                var r = new Utf8JsonReader(itemBytes);
                var input = JsonSerializer.Deserialize(ref r, inputType);
                if (input is null) return;
                foreach (var result in transform(input))
                {
                    JsonSerializer.Serialize(writer, result, outputType);
                    count++;
                }
            },
            ct: ct);
        return count;
    }

    public static async Task<int> ForEachItemAsync(
        this PipeReader reader,
        JsonPath path,
        Action<ReadOnlySequence<byte>> processItem,
        CancellationToken ct = default)
    {
        int count = 0;
        await reader.TransformItemsAsync(
            PipeWriter.Create(Stream.Null),
            path,
            (itemBytes, _) =>
            {
                processItem(itemBytes);
                count++;
            },
            ct: ct);
        return count;
    }
}
