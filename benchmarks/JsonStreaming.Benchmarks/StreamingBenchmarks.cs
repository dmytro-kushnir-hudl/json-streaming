using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;

namespace JsonStreaming.Benchmarks;

/// <summary>
///     End-to-end streaming benchmarks: callback → write-through → typed transform.
///     Single category, sorted fastest-to-slowest, easy to compare.
/// </summary>
[Config(typeof(InProcessConfig))]
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class StreamingBenchmarks
{
    // ── Helpers ─────────────────────────────────────────────────────────────

    private static readonly JsonWriterOptions SkipValidation = new() { SkipValidation = true };

    private byte[] _json = [];

    [Params(200_000)] public int ItemCount { get; set; }

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

    // ── 3. TransformItemsAsync: verbatim passthrough ─────────────────────────

    [Benchmark(Description = "ProjectItems: verbatim")]
    public async Task Write_Verbatim()
    {
        var pipeReader = ToPipe(_json);
        var pipeWriter = PipeWriter.Create(Stream.Null);
        await pipeReader.TransformItemsAsync(
            pipeWriter,
            JsonPath.At("items").Each(),
            (itemBytes, writer) => { writer.Write(itemBytes); });
    }

    // ── 4. TransformItemsAsync: transform via JsonDocument ─────────────────

    [Benchmark(Description = "ProjectItems: transform (JsonDocument)")]
    public async Task Write_TransformJsonDocument()
    {
        var pipe = ToPipe(_json);
        var output = PipeWriter.Create(Stream.Null);
        await pipe.TransformItemsAsync(
            output,
            JsonPath.At("items").Each(),
            (itemBytes, writers) =>
            {
                using var doc = JsonDocument.Parse(itemBytes);
                var root = doc.RootElement;
                writers.Json.WriteStartObject();
                writers.Json.WriteNumber("id"u8, root.GetProperty("id").GetInt32());
                writers.Json.WriteString("title"u8, root.GetProperty("title").GetString());
                writers.Json.WriteEndObject();
                writers.Json.Flush();
                writers.Json.Reset();
            });
    }

    // ── 6. TransformItemsAsync: zero-alloc Utf8JsonReader transform ────────

    [Benchmark(Description = "ProjectItems: transform (Utf8JsonReader, zero-alloc)")]
    public async Task Write_TransformUtf8Reader()
    {
        await ToPipe(_json).TransformItemsAsync(
            PipeWriter.Create(Stream.Null),
            JsonPath.At("items").Each(),
            (itemBytes, writers) =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                var writer = writers.Json;
                reader.Read(); // StartObject

                writer.WriteStartObject();
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
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
                        {
                            writer.WriteStringValue(reader.ValueSpan);
                        }
                        else if (reader.HasValueSequence && !reader.ValueIsEscaped)
                        {
                            // Multi-segment, no escaping: stream segments directly — zero alloc.
                            var e = reader.ValueSequence.GetEnumerator();
                            var hasNext = e.MoveNext();
                            while (hasNext)
                            {
                                var seg = e.Current;
                                hasNext = e.MoveNext();
                                writer.WriteStringValueSegment(seg.Span, !hasNext);
                            }
                        }
                        else
                        {
                            // Escaped (and possibly multi-segment): CopyString unescapes into a buffer.
                            var maxLen = reader.HasValueSequence
                                ? (int)reader.ValueSequence.Length
                                : reader.ValueSpan.Length;
                            if (maxLen <= 256)
                            {
                                Span<byte> buf = stackalloc byte[maxLen];
                                writer.WriteStringValue(buf[..reader.CopyString(buf)]);
                            }
                            else
                            {
                                var rented = ArrayPool<byte>.Shared.Rent(maxLen);
                                try
                                {
                                    writer.WriteStringValue(rented.AsSpan(0, reader.CopyString(rented)));
                                }
                                finally
                                {
                                    ArrayPool<byte>.Shared.Return(rented);
                                }
                            }
                        }
                    }
                    else
                    {
                        reader.Skip();
                    }

                writer.WriteEndObject();
                writer.Flush();
                writer.Reset();
            });
    }

    // ── 7. TransformItemsAsync: typed direct-write ─────────────────────────

    [Benchmark(Description = "ProjectItems: typed direct-write (no TOut alloc)")]
    public async Task Write_TypedDirectWrite()
    {
        var pipe = ToPipe(_json);
        var output = PipeWriter.Create(Stream.Null);
        await pipe.TransformItemsAsync(
            output,
            JsonPath.At("items").Each(),
            (itemBytes, writers) =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                var item = JsonSerializer.Deserialize(ref reader, BenchJsonContext.Default.BenchItem);
                if (item is not null)
                {
                    writers.Json.WriteStartObject();
                    writers.Json.WriteNumber("id"u8, item.Id);
                    writers.Json.WriteString("title"u8, item.Title);
                    writers.Json.WriteEndObject();
                    writers.Json.Flush();
                    writers.Json.Reset();
                }
            });
    }

    // ── 8. TransformItemsAsync: typed transform (TIn → TOut) ──────────────

    [Benchmark(Description = "ProjectItems: typed transform (TIn → TOut)")]
    public async Task Write_TypedTransform()
    {
        var pipe = ToPipe(_json);
        var output = PipeWriter.Create(Stream.Null);
        await pipe.TransformItemsAsync(
            output,
            JsonPath.At("items").Each(),
            (itemBytes, writers) =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                var item = JsonSerializer.Deserialize(ref reader, BenchJsonContext.Default.BenchItem);
                if (item is not null)
                {
                    var slim = new BenchItemSlim { Id = item.Id, Title = item.Title };
                    using var writer = new Utf8JsonWriter(writers.Bytes, SkipValidation);
                    JsonSerializer.Serialize(writer, slim, BenchJsonContext.Default.BenchItemSlim);
                    writer.Flush();
                }
            });
    }

    // ── 9a. Manual loop + POCO reuse: read only needed fields, GetString() for strings ──
    // One BenchItemMutable allocated at setup, reused for all 200K items — no per-item POCO.
    // Skips Brand/Price/Rating/Stock entirely (unlike Write_TypedDirectWrite which deserializes all).
    // Title still calls GetString() → one string allocation per item remains.
    // Demonstrates: object graph + unused-field eliminations are "free";
    // the remaining ~8MB comes entirely from GetString() on the one needed string field.

    [Benchmark(Description = "ProjectItems: manual reuse, GetString() (string still allocs)")]
    public async Task Write_PopulateReuse()
    {
        var instance = new BenchItemMutable();
        await ToPipe(_json).TransformItemsAsync(
            PipeWriter.Create(Stream.Null),
            JsonPath.At("items").Each(),
            (itemBytes, writers) =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                reader.Read(); // StartObject

                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    if (reader.ValueTextEquals("id"u8))
                    {
                        reader.Read();
                        instance.Id = reader.GetInt32();
                    }
                    else if (reader.ValueTextEquals("title"u8))
                    {
                        reader.Read();
                        instance.Title = reader.GetString()!; // ← string allocation
                    }
                    else
                    {
                        reader.Skip();
                    }
                }

                writers.Json.WriteStartObject();
                writers.Json.WriteNumber("id"u8, instance.Id);
                writers.Json.WriteString("title"u8, instance.Title);
                writers.Json.WriteEndObject();
                writers.Json.Flush();
                writers.Json.Reset();
            });
    }

    // ── 9b. CopyString: zero-alloc strings via UTF-8 buffer ─────────────────
    // No managed string at any point: CopyString decodes into stackalloc/ArrayPool,
    // WriteStringValue(ReadOnlySpan<byte>) writes UTF-8 bytes directly.
    // Handles unescaped, escaped (\n, \uXXXX), and multi-segment values in one API call.
    // Trade-off vs ValueSpan path: one extra copy for unescaped single-segment strings;
    // benefit: uniform code, no 3-way branch.

    [Benchmark(Description = "ProjectItems: CopyString (zero-alloc, handles escaped)")]
    public async Task Write_CopyStringZeroAlloc()
    {
        await ToPipe(_json).TransformItemsAsync(
            PipeWriter.Create(Stream.Null),
            JsonPath.At("items").Each(),
            (itemBytes, writers) =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                var writer = writers.Json;
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
                        WriteStringNoAlloc(ref reader, writer);
                    }
                    else
                    {
                        reader.Skip();
                    }
                }

                writer.WriteEndObject();
                writer.Flush();
                writer.Reset();
            });
    }

    // CopyString(Span<byte>) handles all cases in one call:
    // – unescaped single-segment: copies ValueSpan bytes
    // – multi-segment: copies across segments
    // – escaped (\n, \uXXXX): unescapes into destination
    // No managed string created. stackalloc covers the common case; ArrayPool for large values.
    private static void WriteStringNoAlloc(ref Utf8JsonReader reader, Utf8JsonWriter writer)
    {
        var maxBytes = reader.HasValueSequence
            ? checked((int)reader.ValueSequence.Length)
            : reader.ValueSpan.Length;

        if (maxBytes <= 512)
        {
            Span<byte> buf = stackalloc byte[maxBytes == 0 ? 1 : maxBytes];
            writer.WriteStringValue(buf[..reader.CopyString(buf)]);
        }
        else
        {
            var rented = ArrayPool<byte>.Shared.Rent(maxBytes);
            try
            {
                writer.WriteStringValue(rented.AsSpan(0, reader.CopyString(rented)));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    // ── 10. NDJSON: callback approach vs transcoder ──────────────────────

    [Benchmark(Description = "NDJSON: TransformItemsAsync titles")]
    public async Task Ndjson_ProjectTitles_Manual()
    {
        var pipe = ToPipe(_json);

        await pipe.TransformItemsAsync(
            PipeWriter.Create(Stream.Null),
            JsonPath.At("items").Each(),
            (itemBytes, writers) => { writers.Write(itemBytes); });
    }

    [Benchmark(Description = "NDJSON: TransformItemsAsync all items")]
    public async Task Ndjson_ProjectAllItems_Manual()
    {
        var pipe = ToPipe(_json);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.TransformItemsAsync(
            PipeWriter.Create(Stream.Null),
            JsonPath.At("items").Each(),
            (itemBytes, writers) =>
            {
                writers.Write(itemBytes);
                writers.Write("\n"u8);
            });

        await output.FlushAsync();
        await output.CompleteAsync();
    }

    private static PipeReader ToPipe(byte[] data)
    {
        return PipeReader.Create(new MemoryStream(data), new StreamPipeReaderOptions(bufferSize: 8192));
    }

    private static byte[] MakeJson(int count)
    {
        var sb = new StringBuilder();
        sb.Append("""{"items":[""");
        for (var i = 0; i < count; i++)
        {
            if (i > 0)
                sb.Append(',');
            sb.Append(
                $$"""{"id":{{i}},"title":"Product {{i}}","brand":"Brand{{i % 10}}","price":{{9.99 + i}},"rating":{{i % 50 / 10.0}},"stock":{{i % 200}}}"""
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

/// <summary>
/// Mutable version of <see cref="BenchItem"/> for use with <see cref="JsonSerializer.Populate"/>.
/// A single instance is allocated once and reused across all items in the benchmark,
/// eliminating per-item POCO allocations (string fields still allocate per item).
/// </summary>
public sealed class BenchItemMutable
{
    public int Id { get; set; }
    public string Title { get; set; } = "";
    public string? Brand { get; set; }
    public double Price { get; set; }
    public double Rating { get; set; }
    public int Stock { get; set; }
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