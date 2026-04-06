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
    [Params(1_000)]
    public int ItemCount { get; set; }

    private byte[] _json = [];

    [GlobalSetup]
    public void Setup()
    {
        _json = MakeJson(ItemCount);
    }

    // ── 1. Baseline: what a dev writes without this library ───────────────
    // Full-buffer JsonDocument.Parse, iterate items, extract same 2 fields,
    // write to same Utf8JsonWriter. Same output, no streaming.

    [Benchmark(Baseline = true, Description = "Baseline: JsonDocument full-buffer transform")]
    public int Baseline_JsonDocumentFullBuffer()
    {
        using var doc = JsonDocument.Parse(_json);
        using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        int count = 0;
        foreach (var item in doc.RootElement.GetProperty("items").EnumerateArray())
        {
            writer.WriteStartObject();
            writer.WriteNumber("id"u8, item.GetProperty("id").GetInt32());
            writer.WriteString("title"u8, item.GetProperty("title").GetString());
            writer.WriteEndObject();
            count++;
        }
        writer.WriteEndArray();
        return count;
    }

    // ── 3. WriteArray: verbatim (JsonDocument per item) ────────────────────

    [Benchmark(Description = "WriteArray: verbatim")]
    public async Task<int> Write_Verbatim()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(pipe, "items", writer);
        writer.WriteEndArray();
        return count;
    }

    // ── 4. WriteArray: verbatim + flush ────────────────────────────────────

    [Benchmark(Description = "WriteArray: verbatim + flush (16KB)")]
    public async Task<int> Write_VerbatimFlush()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(
            pipe,
            "items",
            writer,
            new WriteOptions
            {
                FlushThreshold = 16_384,
                AsyncFlush = _ => ValueTask.CompletedTask,
            }
        );
        writer.WriteEndArray();
        return count;
    }

    // ── 5. WriteArray: transform via JsonDocument ──────────────────────────

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
            }
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
            }
        );
        writer.WriteEndArray();
        return count;
    }

    // ── 7. Typed transform via source-gen ─────────────────────────────────

    [Benchmark(Description = "WriteArray: typed transform (source-gen)")]
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
            item => new BenchItemSlim { Id = item.Id, Title = item.Title }
        );
        writer.WriteEndArray();
        return count;
    }

    // ── 8. Typed verbatim via source-gen (deserialize + serialize same type)

    [Benchmark(Description = "WriteArray: typed verbatim (source-gen)")]
    public async Task<int> Write_TypedVerbatim()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        var count = await JsonStreamReaderTyped.WriteArrayAsync(
            pipe,
            "items",
            writer,
            BenchJsonContext.Default.BenchItem
        );
        writer.WriteEndArray();
        return count;
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static readonly JsonWriterOptions SkipValidation = new() { SkipValidation = true };

    private static PipeReader ToPipe(byte[] data) =>
        PipeReader.Create(new MemoryStream(data), new StreamPipeReaderOptions(bufferSize: 8192));

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
[JsonSerializable(typeof(ItemWrapper))]
internal partial class BenchJsonContext : JsonSerializerContext;
