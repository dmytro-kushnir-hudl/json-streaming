using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using JsonStreaming;

static class StreamingExtensions
{
    public static async Task<int> ProjectTypedAsync<TIn, TOut>(
        this PipeReader reader,
        NdJsonPath path,
        Utf8JsonWriter writer,
        JsonTypeInfo<TIn> inputType,
        JsonTypeInfo<TOut> outputType,
        Func<TIn, IEnumerable<TOut>> transform,
        CancellationToken ct = default)
    {
        int count = 0;
        await reader.ProjectItemsAsync(
            path,
            PipeWriter.Create(Stream.Null),
            (itemBytes, _) =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                var input = JsonSerializer.Deserialize(ref reader, inputType);
                if (input is null) return ValueTask.CompletedTask;
                foreach (var result in transform(input))
                {
                    JsonSerializer.Serialize(writer, result, outputType);
                    count++;
                }
                return ValueTask.CompletedTask;
            },
            ct: ct);
        return count;
    }

    public static async Task<int> ForEachItemAsync(
        this PipeReader reader,
        NdJsonPath path,
        Action<ReadOnlySequence<byte>> processItem,
        CancellationToken ct = default)
    {
        int count = 0;
        await reader.ProjectItemsAsync(
            path,
            PipeWriter.Create(Stream.Null),
            (itemBytes, _) =>
            {
                processItem(itemBytes);
                count++;
                return ValueTask.CompletedTask;
            },
            ct: ct);
        return count;
    }
}
