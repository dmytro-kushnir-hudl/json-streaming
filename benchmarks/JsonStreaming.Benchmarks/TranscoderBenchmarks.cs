using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;

namespace JsonStreaming.Benchmarks;

/// <summary>
/// Benchmarks for <see cref="JsonTranscoder"/>: format, minify, and NDJSON projection.
///
/// Input is pre-generated and kept in memory; output is discarded (<see cref="Stream.Null"/>)
/// so measurements reflect only transcoding throughput.
/// </summary>
[Config(typeof(InProcessConfig))]
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class TranscoderBenchmarks
{
    [Params(50_000)]
    public int ItemCount { get; set; }

    private byte[] _minified = [];
    private byte[] _formatted = [];

    private static readonly JsonPath ProjectTitles = JsonPath.At("items").Each().Key("title");
    private static readonly JsonPath ProjectAllItems = JsonPath.At("items").Each();

    [GlobalSetup]
    public void Setup()
    {
        _minified = MakeMinifiedJson(ItemCount);

        // formatted = pretty-print the minified input via the transcoder
        var ms = new MemoryStream();
        var pipe = ToPipe(_minified);
        var writer = PipeWriter.Create(ms);
        pipe.ProxyFormattedJsonAsync(writer).GetAwaiter().GetResult();
        writer.CompleteAsync().GetAwaiter().GetResult();
        _formatted = ms.ToArray();
    }

    // ── Baseline ──────────────────────────────────────────────────────────────

    [Benchmark(Baseline = true, Description = "Baseline: JsonSerializer format (allocates full DOM)")]
    public void Baseline_Format()
    {
        var doc = JsonDocument.Parse(_minified);
        JsonSerializer.Serialize(Stream.Null, doc.RootElement,
            new JsonSerializerOptions { WriteIndented = true });
    }

    [Benchmark(Description = "Baseline: JsonSerializer minify (allocates full DOM)")]
    public void Baseline_Minify()
    {
        var doc = JsonDocument.Parse(_formatted);
        JsonSerializer.Serialize(Stream.Null, doc.RootElement,
            new JsonSerializerOptions { WriteIndented = false });
    }

    // ── Streaming ─────────────────────────────────────────────────────────────

    [Benchmark(Description = "Transcoder: format (streaming, bounded memory)")]
    public async Task Format_Streaming()
    {
        var pipe = ToPipe(_minified);
        var writer = PipeWriter.Create(Stream.Null);
        await pipe.ProxyFormattedJsonAsync(writer);
        await writer.CompleteAsync();
    }

    [Benchmark(Description = "Transcoder: minify (streaming, bounded memory)")]
    public async Task Minify_Streaming()
    {
        var pipe = ToPipe(_formatted);
        var writer = PipeWriter.Create(Stream.Null);
        await pipe.ProxyMinifiedJsonAsync(writer);
        await writer.CompleteAsync();
    }

    [Benchmark(Description = "Transcoder: project titles (items[*].title → NDJSON)")]
    public async Task Project_Titles()
    {
        var pipe = ToPipe(_minified);
        var writer = PipeWriter.Create(Stream.Null);
        await pipe.TransformItemsAsync(writer, ProjectTitles, (bytes, w) => w.Write(bytes));
        await writer.CompleteAsync();
    }

    [Benchmark(Description = "Transcoder: project all items (items[*] → NDJSON)")]
    public async Task Project_AllItems()
    {
        var pipe = ToPipe(_minified);
        var writer = PipeWriter.Create(Stream.Null);
        await pipe.TransformItemsAsync(writer, ProjectAllItems, (bytes, w) => w.Write(bytes));
        await writer.CompleteAsync();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static PipeReader ToPipe(byte[] data) =>
        PipeReader.Create(new MemoryStream(data), new StreamPipeReaderOptions(bufferSize: 8192));

    private static byte[] MakeMinifiedJson(int count)
    {
        var sb = new StringBuilder();
        sb.Append("""{"items":[""");
        for (int i = 0; i < count; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append(
                $$"""{"id":{{i}},"title":"Product {{i}}","brand":"Brand{{i % 10}}","price":{{9.99 + i}},"rating":{{(i % 50) / 10.0}},"stock":{{i % 200}}}"""
            );
        }
        sb.Append("]}");
        return Encoding.UTF8.GetBytes(sb.ToString());
    }
}
