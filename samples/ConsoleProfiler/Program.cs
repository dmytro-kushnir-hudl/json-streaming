using System.Diagnostics;
using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using JsonStreaming;

// ── Generate test data: 200K items, ~30MB JSON ─────────────────────────

const int itemCount = 200_000;
Console.Error.WriteLine($"Generating {itemCount:N0} items...");

var sb = new StringBuilder();
sb.Append("""{"items":[""");
for (int i = 0; i < itemCount; i++)
{
    if (i > 0)
        sb.Append(',');
    sb.Append(
        $$"""{"id":{{i}},"title":"Product {{i}}","brand":"Brand{{i % 10}}","price":{{9.99 + i}},"rating":{{(i % 50) / 10.0}},"stock":{{i % 200}}}"""
    );
}
sb.Append("]}");
var json = Encoding.UTF8.GetBytes(sb.ToString());
Console.Error.WriteLine($"JSON size: {json.Length / 1024.0 / 1024.0:F1} MB");

var pid = Environment.ProcessId;
var threads = Environment.ProcessorCount;

Console.Error.WriteLine($"PID: {pid}");
Console.Error.WriteLine($"Threads: {threads}");
Console.Error.WriteLine();
Console.Error.WriteLine($"  dotnet-gcdump collect -p {pid}");
Console.Error.WriteLine();
Console.Error.WriteLine("Spinning until ENTER is pressed...");

// ── Spin on all cores until stdin gets a line ──────────────────────────

using var cts = new CancellationTokenSource();
var ct = cts.Token;
long totalItems = 0;

var workers = Enumerable
    .Range(0, threads)
    .Select(id => Task.Run(async () =>
    {
        int local = 0;
        while (!ct.IsCancellationRequested)
        {
            local += await RunOnce(json);
        }
        Interlocked.Add(ref totalItems, local);
    }))
    .ToArray();

// Wait for stdin
Console.ReadLine();
cts.Cancel();

try { await Task.WhenAll(workers); }
catch (OperationCanceledException) { }

Console.Error.WriteLine();
Console.Error.WriteLine($"Total items: {Interlocked.Read(ref totalItems):N0}");
Console.Error.WriteLine($"GC Gen0:     {GC.CollectionCount(0)}");
Console.Error.WriteLine($"GC Gen1:     {GC.CollectionCount(1)}");
Console.Error.WriteLine($"GC Gen2:     {GC.CollectionCount(2)}");
Console.Error.WriteLine($"Heap:        {GC.GetTotalMemory(false) / 1024.0 / 1024.0:F1} MB");

static async Task<int> RunOnce(byte[] json)
{
    // PipeReader.Create(Stream) returns a StreamPipeReader that rents from ArrayPool.
    // Must dispose to return the rental — CompleteAsync alone doesn't do it.
    var pipe = PipeReader.Create(
        new MemoryStream(json),
        new StreamPipeReaderOptions(bufferSize: 8192)
    );

    await using var writer = new Utf8JsonWriter(
        Stream.Null,
        new JsonWriterOptions { SkipValidation = true }
    );
    var options = new WriteOptions
    {
        FlushThreshold = 16_384,
        AsyncFlush = _ => ValueTask.CompletedTask,
    };

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
        options
    );

    writer.WriteEndArray();
    await pipe.CompleteAsync(); // returns all rented BufferSegments to ArrayPool
    return count;
}
