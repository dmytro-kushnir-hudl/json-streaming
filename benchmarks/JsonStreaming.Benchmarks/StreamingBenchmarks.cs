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

    // ── 3. WriteArray: verbatim ───────────────────────────────────────────

    [Benchmark(Description = "WriteArray: verbatim")]
    public async Task<int> Write_Verbatim()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(pipe, "items", writer, FlushOptions);
        writer.WriteEndArray();
        return count;
    }

    // ── 4. WriteArray: transform via JsonDocument ──────────────────────────

    [Benchmark(Description = "WriteArray: transform (JsonDocument)")]
    public async Task<int> Write_TransformJsonDocument()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(
            pipe,
            "items",
            writer,
            (itemBytes, w) =>
            {
                using var doc = JsonDocument.Parse(itemBytes);
                var root = doc.RootElement;
                w.WriteStartObject();
                w.WriteNumber("id"u8, root.GetProperty("id").GetInt32());
                w.WriteString("title"u8, root.GetProperty("title").GetString());
                w.WriteEndObject();
            },
            FlushOptions
        );
        writer.WriteEndArray();
        return count;
    }

    // ── 6. Zero-alloc transform via Utf8JsonReader → Utf8JsonWriter ──────

    [Benchmark(Description = "WriteArray: transform (Utf8JsonReader, zero-alloc)")]
    public async Task<int> Write_TransformUtf8Reader()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(
            pipe,
            "items",
            writer,
            (itemBytes, w) =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                reader.Read(); // StartObject
                w.WriteStartObject();
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    if (reader.ValueTextEquals("id"u8))
                    {
                        reader.Read();
                        w.WriteNumber("id"u8, reader.GetInt32());
                    }
                    else if (reader.ValueTextEquals("title"u8))
                    {
                        reader.Read();
                        w.WritePropertyName("title"u8);
                        // Copy raw UTF-8 bytes — no string allocation
                        if (!reader.HasValueSequence && !reader.ValueIsEscaped)
                            w.WriteStringValue(reader.ValueSpan);
                        else
                            w.WriteStringValue(reader.GetString());
                    }
                    else
                    {
                        reader.Skip();
                    }
                }
                w.WriteEndObject();
            },
            FlushOptions
        );
        writer.WriteEndArray();
        return count;
    }

    // ── 7. Typed direct-write (deserialize TIn, write directly, no TOut) ──

    [Benchmark(Description = "WriteArray: typed direct-write (no TOut alloc)")]
    public async Task<int> Write_TypedDirectWrite()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        var count = await JsonStreamReaderTyped.WriteArrayAsync(
            pipe,
            "items",
            writer,
            BenchJsonContext.Default.BenchItem,
            (item, w) =>
            {
                w.WriteStartObject();
                w.WriteNumber("id"u8, item.Id);
                w.WriteString("title"u8, item.Title);
                w.WriteEndObject();
            },
            FlushOptions
        );
        writer.WriteEndArray();
        return count;
    }

    // ── 8. Typed transform (TIn → TOut, both allocated) ────────────────────

    [Benchmark(Description = "WriteArray: typed transform (TIn → TOut)")]
    public async Task<int> Write_TypedTransform()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        var count = await JsonStreamReaderTyped.WriteArrayAsync(
            pipe,
            "items",
            writer,
            BenchJsonContext.Default.BenchItem,
            BenchJsonContext.Default.BenchItemSlim,
            item => new BenchItemSlim { Id = item.Id, Title = item.Title },
            FlushOptions
        );
        writer.WriteEndArray();
        return count;
    }

    // ── 9. Typed verbatim (raw passthrough, no deserialize) ────────────────

    [Benchmark(Description = "WriteArray: typed verbatim (raw passthrough)")]
    public async Task<int> Write_TypedVerbatim()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(pipe, "items", writer, FlushOptions);
        writer.WriteEndArray();
        return count;
    }

    // ── 10. NDJSON: old callback approach vs transcoder ───────────────────

    [Benchmark(Description = "NDJSON: old projection titles (ProcessArray + Utf8JsonReader)")]
    public async Task Ndjson_ProjectTitles_Manual()
    {
        var pipe = ToPipe(_json);
        var output = PipeWriter.Create(Stream.Null);
        using var writer = new Utf8JsonWriter(output, SkipValidation);

        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            "items",
            itemBytes => WriteTitleNdjsonLine(itemBytes, writer, output)
        );

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

    [Benchmark(Description = "NDJSON: old passthrough all items (ProcessArray callback)")]
    public async Task Ndjson_ProjectAllItems_Manual()
    {
        var pipe = ToPipe(_json);
        var output = PipeWriter.Create(Stream.Null);

        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            "items",
            itemBytes =>
            {
                WriteSequence(output, itemBytes);
                output.Write("\n"u8);
                FlushPipeWriterIfNeeded(output);
            }
        );

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

    private static readonly WriteOptions FlushOptions = new()
    {
        FlushThreshold = 16_384,
        AsyncFlush = _ => ValueTask.CompletedTask,
    };

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
