using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;

namespace JsonStreaming.Benchmarks;

/// <summary>
/// End-to-end streaming benchmarks: callback → write-through → typed transform.
/// Single category, sorted fastest-to-slowest, easy to compare.
/// </summary>
[Config(typeof(InProcessConfig))]
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class StreamingBenchmarks
{
    [Params(200_000)]
    public int ItemCount { get; set; }

    private byte[] _json = [];

    private const long PipeFlushThreshold = 16_384;

    private static readonly NdJsonPath ProjectTitlesPath = NdJsonPath.At("items").Each().Key("title");
    private static readonly NdJsonPath ProjectAllItemsPath = NdJsonPath.At("items").Each();

    [GlobalSetup]
    public void Setup()
    {
        _json = MakeJson(ItemCount);
    }

    // ── 1. Baseline: what a dev writes without this library ───────────────
    // Deserialize into List<T>, LINQ Select, serialize back.
    // This is the normal .NET pattern — no streaming, full buffer.

    [Benchmark(Baseline = true, Description = "Baseline: Deserialize → LINQ → Serialize")]
    public void Baseline_DeserializeLinqSerialize()
    {
        var wrapper = JsonSerializer.Deserialize(
            _json,
            BenchJsonContext.Default.ItemWrapper
        )!;

        var results = wrapper
            .Items!.Select(item => new BenchItemSlim { Id = item.Id, Title = item.Title })
            .ToList();

        JsonSerializer.Serialize(Stream.Null, results, BenchJsonContext.Default.ListBenchItemSlim);
    }

    // ── 3. ProjectItemsAsync: verbatim passthrough ─────────────────────────

    [Benchmark(Description = "ProjectItems: verbatim")]
    public async Task Write_Verbatim()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        await pipe.ProjectItemsAsync(
            NdJsonPath.At("items").Each(),
            PipeWriter.Create(Stream.Null),
            (itemBytes, _) =>
            {
                if (itemBytes.IsSingleSegment)
                    writer.WriteRawValue(itemBytes.FirstSpan, skipInputValidation: true);
                else
                    writer.WriteRawValue(itemBytes.ToArray(), skipInputValidation: true);
                return ValueTask.CompletedTask;
            });
        writer.WriteEndArray();
    }

    // ── 4. ProjectItemsAsync: transform via JsonDocument ─────────────────

    [Benchmark(Description = "ProjectItems: transform (JsonDocument)")]
    public async Task Write_TransformJsonDocument()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        await pipe.ProjectItemsAsync(
            NdJsonPath.At("items").Each(),
            PipeWriter.Create(Stream.Null),
            (itemBytes, _) =>
            {
                using var doc = JsonDocument.Parse(itemBytes);
                var root = doc.RootElement;
                writer.WriteStartObject();
                writer.WriteNumber("id"u8, root.GetProperty("id").GetInt32());
                writer.WriteString("title"u8, root.GetProperty("title").GetString());
                writer.WriteEndObject();
                return ValueTask.CompletedTask;
            });
        writer.WriteEndArray();
    }

    // ── 6. ProjectItemsAsync: zero-alloc Utf8JsonReader transform ────────

    [Benchmark(Description = "ProjectItems: transform (Utf8JsonReader, zero-alloc)")]
    public async Task Write_TransformUtf8Reader()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        await pipe.ProjectItemsAsync(
            NdJsonPath.At("items").Each(),
            PipeWriter.Create(Stream.Null),
            (itemBytes, _) =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                reader.Read(); // StartObject
                writer.WriteStartObject();
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    if (reader.ValueTextEquals("id"u8))
                    {
                        reader.Read();
                        writer.WriteNumber("id"u8, reader.GetInt32());
                    }
                    else if (reader.ValueTextEquals("title"u8))
                    {
                        reader.Read();
                        writer.WritePropertyName("title"u8);
                        if (!reader.HasValueSequence && !reader.ValueIsEscaped)
                            writer.WriteStringValue(reader.ValueSpan);
                        else
                            writer.WriteStringValue(reader.GetString());
                    }
                    else
                    {
                        reader.Skip();
                    }
                }
                writer.WriteEndObject();
                return ValueTask.CompletedTask;
            });
        writer.WriteEndArray();
    }

    // ── 7. ProjectItemsAsync: typed direct-write ─────────────────────────

    [Benchmark(Description = "ProjectItems: typed direct-write (no TOut alloc)")]
    public async Task Write_TypedDirectWrite()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        await pipe.ProjectItemsAsync(
            NdJsonPath.At("items").Each(),
            PipeWriter.Create(Stream.Null),
            (itemBytes, _) =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                var item = JsonSerializer.Deserialize(ref reader, BenchJsonContext.Default.BenchItem);
                if (item is not null)
                {
                    writer.WriteStartObject();
                    writer.WriteNumber("id"u8, item.Id);
                    writer.WriteString("title"u8, item.Title);
                    writer.WriteEndObject();
                }
                return ValueTask.CompletedTask;
            });
        writer.WriteEndArray();
    }

    // ── 8. ProjectItemsAsync: typed transform (TIn → TOut) ──────────────

    [Benchmark(Description = "ProjectItems: typed transform (TIn → TOut)")]
    public async Task Write_TypedTransform()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        await pipe.ProjectItemsAsync(
            NdJsonPath.At("items").Each(),
            PipeWriter.Create(Stream.Null),
            (itemBytes, _) =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                var item = JsonSerializer.Deserialize(ref reader, BenchJsonContext.Default.BenchItem);
                if (item is not null)
                {
                    var slim = new BenchItemSlim { Id = item.Id, Title = item.Title };
                    JsonSerializer.Serialize(writer, slim, BenchJsonContext.Default.BenchItemSlim);
                }
                return ValueTask.CompletedTask;
            });
        writer.WriteEndArray();
    }

    // ── 10. NDJSON: callback approach vs transcoder ──────────────────────

    [Benchmark(Description = "NDJSON: ProjectItemsAsync titles")]
    public async Task Ndjson_ProjectTitles_Manual()
    {
        var pipe = ToPipe(_json);
        var output = PipeWriter.Create(Stream.Null);
        using var writer = new Utf8JsonWriter(output, SkipValidation);

        await pipe.ProjectItemsAsync(
            NdJsonPath.At("items").Each(),
            output,
            (itemBytes, _) =>
            {
                WriteTitleNdjsonLine(itemBytes, writer, output);
                return ValueTask.CompletedTask;
            });

        writer.Flush();
        await output.FlushAsync();
        await output.CompleteAsync();
    }

    [Benchmark(Description = "NDJSON: transcoder projection titles (Utf8JsonWriter)")]
    public async Task Ndjson_ProjectTitles_Transcoder()
    {
        var pipe = ToPipe(_json);
        var writer = PipeWriter.Create(Stream.Null);
        await pipe.ProjectNdJsonAsync(ProjectTitlesPath, writer);
        await writer.CompleteAsync();
    }

    [Benchmark(Description = "NDJSON: transcoder projection titles (direct copy)")]
    public async Task Ndjson_ProjectTitles_TranscoderDirect()
    {
        var pipe = ToPipe(_json);
        var writer = PipeWriter.Create(Stream.Null);
        await pipe.ProjectNdJsonVerbatimAsync(ProjectTitlesPath, writer);
        await writer.CompleteAsync();
    }

    [Benchmark(Description = "NDJSON: ProjectItemsAsync all items")]
    public async Task Ndjson_ProjectAllItems_Manual()
    {
        var pipe = ToPipe(_json);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            NdJsonPath.At("items").Each(),
            output,
            (itemBytes, pw) =>
            {
                WriteSequence(pw, itemBytes);
                pw.Write("\n"u8);
                return ValueTask.CompletedTask;
            });

        await output.FlushAsync();
        await output.CompleteAsync();
    }

    [Benchmark(Description = "NDJSON: transcoder all items (Utf8JsonWriter)")]
    public async Task Ndjson_ProjectAllItems_Transcoder()
    {
        var pipe = ToPipe(_json);
        var writer = PipeWriter.Create(Stream.Null);
        await pipe.ProjectNdJsonAsync(ProjectAllItemsPath, writer);
        await writer.CompleteAsync();
    }

    [Benchmark(Description = "NDJSON: transcoder all items (direct copy)")]
    public async Task Ndjson_ProjectAllItems_TranscoderDirect()
    {
        var pipe = ToPipe(_json);
        var writer = PipeWriter.Create(Stream.Null);
        await pipe.ProjectNdJsonVerbatimAsync(ProjectAllItemsPath, writer);
        await writer.CompleteAsync();
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static readonly JsonWriterOptions SkipValidation = new() { SkipValidation = true };

    private static PipeReader ToPipe(byte[] data) =>
        PipeReader.Create(new MemoryStream(data), new StreamPipeReaderOptions(bufferSize: 8192));

    private static void WriteTitleNdjsonLine(
        ReadOnlySequence<byte> itemBytes,
        Utf8JsonWriter writer,
        PipeWriter output
    )
    {
        var reader = new Utf8JsonReader(itemBytes);

        while (reader.Read())
        {
            if (reader.TokenType != JsonTokenType.PropertyName)
                continue;

            bool isTitle = reader.ValueTextEquals("title"u8);
            reader.Read();

            if (!isTitle)
            {
                reader.Skip();
                continue;
            }

            if (!reader.HasValueSequence && !reader.ValueIsEscaped)
                writer.WriteStringValue(reader.ValueSpan);
            else
                writer.WriteStringValue(reader.GetString());

            writer.Flush();
            output.Write("\n"u8);
            writer.Reset();
            FlushPipeWriterIfNeeded(output);
            return;
        }
    }

    private static void WriteSequence(PipeWriter output, ReadOnlySequence<byte> sequence)
    {
        if (sequence.IsSingleSegment)
        {
            output.Write(sequence.FirstSpan);
            return;
        }

        foreach (var segment in sequence)
            output.Write(segment.Span);
    }

    private static void FlushPipeWriterIfNeeded(PipeWriter output)
    {
        if (
            output is { CanGetUnflushedBytes: true, UnflushedBytes: >= PipeFlushThreshold }
        )
            output.FlushAsync().GetAwaiter().GetResult();
    }

    private static byte[] MakeJson(int count)
    {
        var sb = new StringBuilder();
        sb.Append("""{"items":[""");
        for (int i = 0; i < count; i++)
        {
            if (i > 0)
                sb.Append(',');
            sb.Append(
                $$"""{"id":{{i}},"title":"Product {{i}}","brand":"Brand{{i % 10}}","price":{{9.99 + i}},"rating":{{(i % 50) / 10.0}},"stock":{{i % 200}}}"""
            );
        }
        sb.Append("]}");
        return Encoding.UTF8.GetBytes(sb.ToString());
    }
}

// ── Source-gen types for benchmarks ─────────────────────────────────────

public sealed record BenchItem
{
    public int Id { get; init; }
    public string Title { get; init; } = "";
    public string? Brand { get; init; }
    public double Price { get; init; }
    public double Rating { get; init; }
    public int Stock { get; init; }
}

public sealed record BenchItemSlim
{
    public int Id { get; init; }
    public string Title { get; init; } = "";
}

public sealed record ItemWrapper
{
    public List<BenchItem>? Items { get; init; }
}

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(BenchItem))]
[JsonSerializable(typeof(BenchItemSlim))]
[JsonSerializable(typeof(List<BenchItemSlim>))]
[JsonSerializable(typeof(ItemWrapper))]
internal partial class BenchJsonContext : JsonSerializerContext;
