using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;

namespace JsonStreaming.Benchmarks;

/// <summary>
/// Benchmarks for WriteArrayAsync (PipeReader → Utf8JsonWriter).
/// Measures write-through overhead including flush behavior.
/// </summary>
[Config(typeof(InProcessConfig))]
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkDotNet.Configs.BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class WriteArrayBenchmarks
{
    [Params(1_000, 10_000, 100_000)]
    public int ItemCount { get; set; }

    private byte[] _json = [];

    [GlobalSetup]
    public void Setup()
    {
        _json = MakeJson(ItemCount);
    }

    // ── Write modes ────────────────────────────────────────────────────────

    [BenchmarkCategory("WriteMode")]
    [Benchmark(Baseline = true, Description = "WriteArray: verbatim")]
    public async Task<int> Write_Verbatim()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(pipe, "messages", writer);
        writer.WriteEndArray();
        writer.Flush();
        return count;
    }

    [BenchmarkCategory("WriteMode")]
    [Benchmark(Description = "WriteArray: transform (select fields)")]
    public async Task<int> Write_Transform()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(
            pipe,
            "messages",
            writer,
            (itemBytes, w) =>
            {
                using var doc = JsonDocument.Parse(itemBytes);
                w.WriteStartObject();
                w.WriteString("_raw"u8, doc.RootElement.GetProperty("_raw").GetString());
                w.WriteString("_loglevel"u8, doc.RootElement.GetProperty("_loglevel").GetString());
                w.WriteEndObject();
            }
        );
        writer.WriteEndArray();
        writer.Flush();
        return count;
    }

    [BenchmarkCategory("WriteMode")]
    [Benchmark(Description = "WriteArray: verbatim with flush (16KB)")]
    public async Task<int> Write_VerbatimWithFlush()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(
            pipe,
            "messages",
            writer,
            new WriteOptions
            {
                FlushThreshold = 16_384,
                AsyncFlush = _ => ValueTask.CompletedTask,
            }
        );
        writer.WriteEndArray();
        writer.Flush();
        return count;
    }

    [BenchmarkCategory("WriteMode")]
    [Benchmark(Description = "Baseline: ProcessArray (callback only)")]
    public async Task<int> Baseline_ProcessArray()
    {
        var pipe = ToPipe(_json);
        return await JsonStreamReader.ProcessArrayAsync(pipe, "messages", _ => { });
    }

    // ── Flush overhead ─────────────────────────────────────────────────────

    [BenchmarkCategory("Flush")]
    [Benchmark(Baseline = true, Description = "Flush: disabled")]
    public async Task<int> Flush_Disabled()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(
            pipe,
            "messages",
            writer,
            new WriteOptions { FlushThreshold = 0 }
        );
        writer.WriteEndArray();
        writer.Flush();
        return count;
    }

    [BenchmarkCategory("Flush")]
    [Benchmark(Description = "Flush: 4KB threshold")]
    public async Task<int> Flush_4KB()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(
            pipe,
            "messages",
            writer,
            new WriteOptions
            {
                FlushThreshold = 4096,
                AsyncFlush = _ => ValueTask.CompletedTask,
            }
        );
        writer.WriteEndArray();
        writer.Flush();
        return count;
    }

    [BenchmarkCategory("Flush")]
    [Benchmark(Description = "Flush: 16KB threshold")]
    public async Task<int> Flush_16KB()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(
            pipe,
            "messages",
            writer,
            new WriteOptions
            {
                FlushThreshold = 16_384,
                AsyncFlush = _ => ValueTask.CompletedTask,
            }
        );
        writer.WriteEndArray();
        writer.Flush();
        return count;
    }

    [BenchmarkCategory("Flush")]
    [Benchmark(Description = "Flush: 64KB threshold")]
    public async Task<int> Flush_64KB()
    {
        var pipe = ToPipe(_json);
        await using var writer = new Utf8JsonWriter(Stream.Null, SkipValidation);
        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(
            pipe,
            "messages",
            writer,
            new WriteOptions
            {
                FlushThreshold = 65_536,
                AsyncFlush = _ => ValueTask.CompletedTask,
            }
        );
        writer.WriteEndArray();
        writer.Flush();
        return count;
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static readonly JsonWriterOptions SkipValidation = new() { SkipValidation = true };

    private static PipeReader ToPipe(byte[] data, int bufferSize = 8192) =>
        PipeReader.Create(new MemoryStream(data), new StreamPipeReaderOptions(bufferSize: bufferSize));

    private static byte[] MakeJson(int count)
    {
        var sb = new StringBuilder();
        sb.Append("""{"messages":[""");
        for (int i = 0; i < count; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append($$"""{"_raw":"log entry {{i}}","_loglevel":"INFO","_messagetime":"17754264{{i:D5}}","_sourcehost":"prod-web","_sourcecategory":"app"}""");
        }
        sb.Append("]}");
        return Encoding.UTF8.GetBytes(sb.ToString());
    }
}
