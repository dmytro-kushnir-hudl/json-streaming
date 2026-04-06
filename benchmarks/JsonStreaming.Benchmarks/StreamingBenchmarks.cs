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
    [Params(1_000, 100_000)]
    public int ItemCount { get; set; }

    private byte[] _json = [];

    [GlobalSetup]
    public void Setup()
    {
        _json = MakeJson(ItemCount);
    }

    // ── 1. Callback only (zero-copy baseline) ──────────────────────────────

    [Benchmark(Baseline = true, Description = "ProcessArray: callback (zero-copy)")]
    public async Task<int> Callback_ZeroCopy()
    {
        var pipe = ToPipe(_json);
        return await JsonStreamReader.ProcessArrayAsync(pipe, "items", _ => { });
    }

    // ── 2. STJ baselines ───────────────────────────────────────────────────

    [Benchmark(Description = "Baseline: JsonDocument.Parse")]
    public int Baseline_JsonDocument()
    {
        using var doc = JsonDocument.Parse(_json);
        int count = 0;
        foreach (var _ in doc.RootElement.GetProperty("items").EnumerateArray())
            count++;
        return count;
    }

    [Benchmark(Description = "Baseline: JsonSerializer.Deserialize<List<T>>")]
    public int Baseline_Deserialize()
    {
        var wrapper = JsonSerializer.Deserialize(
            _json,
            BenchJsonContext.Default.ItemWrapper
        );
        return wrapper?.Items?.Count ?? 0;
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

    // ── 6. Typed transform via source-gen (no JsonDocument) ────────────────

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

    // ── 7. Typed verbatim via source-gen (deserialize + serialize same type)

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
