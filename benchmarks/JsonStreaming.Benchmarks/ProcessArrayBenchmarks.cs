using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;

namespace JsonStreaming.Benchmarks;

/// <summary>
/// Benchmarks for ProcessArrayAsync (zero-copy callback).
/// Measures navigation + iteration overhead at various scales.
/// </summary>
[Config(typeof(InProcessConfig))]
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkDotNet.Configs.BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class ProcessArrayBenchmarks
{
    [Params(100, 100_000)]
    public int ItemCount { get; set; }

    private byte[] _flatJson = [];
    private byte[] _nestedJson = [];
    private byte[] _selectManyJson = [];

    [GlobalSetup]
    public void Setup()
    {
        _flatJson = MakeJson(ItemCount);
        _nestedJson = MakeNestedJson(ItemCount);
        _selectManyJson = MakeSelectManyJson(ItemCount, groupSize: 100);
    }

    // ── Scale: flat path ───────────────────────────────────────────────────

    [BenchmarkCategory("Scale")]
    [Benchmark(Baseline = true, Description = "ProcessArray: flat path")]
    public async Task<int> Process_FlatPath()
    {
        var pipe = ToPipe(_flatJson);
        return await JsonStreamReader.ProcessArrayAsync(pipe, "messages", _ => { });
    }

    [BenchmarkCategory("Scale")]
    [Benchmark(Description = "ProcessArray: nested path")]
    public async Task<int> Process_NestedPath()
    {
        var pipe = ToPipe(_nestedJson);
        return await JsonStreamReader.ProcessArrayAsync(pipe, "response.data.items", _ => { });
    }

    [BenchmarkCategory("Scale")]
    [Benchmark(Description = "ProcessArray: select-many")]
    public async Task<int> Process_SelectMany()
    {
        var pipe = ToPipe(_selectManyJson);
        var path = JsonPath.Root.Property("groups"u8).Each().Property("items"u8);
        return await JsonStreamReader.ProcessArrayAsync(pipe, path, _ => { });
    }

    // ── Baselines: STJ ─────────────────────────────────────────────────────

    [BenchmarkCategory("Scale")]
    [Benchmark(Description = "Baseline: JsonDocument.Parse")]
    public int Baseline_JsonDocument()
    {
        using var doc = JsonDocument.Parse(_flatJson);
        int count = 0;
        foreach (var _ in doc.RootElement.GetProperty("messages").EnumerateArray())
            count++;
        return count;
    }

    [BenchmarkCategory("Scale")]
    [Benchmark(Description = "Baseline: JsonSerializer.Deserialize")]
    public int Baseline_Deserialize()
    {
        var wrapper = JsonSerializer.Deserialize<MessageWrapper>(_flatJson);
        return wrapper?.Messages?.Count ?? 0;
    }

    // ── Buffer size effect ─────────────────────────────────────────────────

    [BenchmarkCategory("BufSize")]
    [Benchmark(Description = "BufSize: 64B")]
    public async Task<int> BufSize_64B()
    {
        var pipe = ToPipe(_flatJson, 64);
        return await JsonStreamReader.ProcessArrayAsync(pipe, "messages", _ => { });
    }

    [BenchmarkCategory("BufSize")]
    [Benchmark(Baseline = true, Description = "BufSize: 8KB")]
    public async Task<int> BufSize_8KB()
    {
        var pipe = ToPipe(_flatJson, 8192);
        return await JsonStreamReader.ProcessArrayAsync(pipe, "messages", _ => { });
    }

    [BenchmarkCategory("BufSize")]
    [Benchmark(Description = "BufSize: 64KB")]
    public async Task<int> BufSize_64KB()
    {
        var pipe = ToPipe(_flatJson, 65536);
        return await JsonStreamReader.ProcessArrayAsync(pipe, "messages", _ => { });
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

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

    private static byte[] MakeNestedJson(int count)
    {
        var flat = Encoding.UTF8.GetString(MakeJson(count));
        var arrayStart = flat.IndexOf('[');
        var arrayJson = flat[arrayStart..^1]; // strip outer }
        return Encoding.UTF8.GetBytes("""{"response":{"data":{"items":""" + arrayJson + "}}}");
    }

    private static byte[] MakeSelectManyJson(int totalItems, int groupSize)
    {
        var sb = new StringBuilder();
        sb.Append("""{"groups":[""");
        int remaining = totalItems;
        bool first = true;
        while (remaining > 0)
        {
            if (!first) sb.Append(',');
            first = false;
            int batchSize = Math.Min(remaining, groupSize);
            sb.Append("""{"items":[""");
            for (int i = 0; i < batchSize; i++)
            {
                if (i > 0) sb.Append(',');
                int idx = totalItems - remaining + i;
                sb.Append($$"""{"id":{{idx}}}""");
            }
            sb.Append("]}");
            remaining -= batchSize;
        }
        sb.Append("]}");
        return Encoding.UTF8.GetBytes(sb.ToString());
    }

    private sealed class MessageWrapper
    {
        public List<Dictionary<string, string>>? Messages { get; set; }
    }
}
