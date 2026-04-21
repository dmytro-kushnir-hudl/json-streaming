namespace JsonStreaming.Benchmarks;

using BenchmarkDotNet.Attributes;
using System.IO.Pipelines;

[Config(typeof(InProcessConfig))]
[MemoryDiagnoser]
public class AsyncOverheadBenchmark
{
    private byte[] _jsonData;
    private PipeReader _reader;
    private PipeWriter _writer;

    private Stream _stream = Stream.Null;

    [Params(10000)] // Number of items to process
    public int ItemCount;

    [GlobalSetup]
    public void Setup()
    {
        // Generate a simple NDJSON stream: {"id":1}\n{"id":2}...
        using var ms = new MemoryStream();
        using var sw = new StreamWriter(ms);
        for (int i = 0; i < ItemCount; i++)
        {
            sw.WriteLine($"{{\"id\":{i}}}");
        }
        sw.Flush();
        _jsonData = ms.ToArray();
    }

    [Benchmark]
    public async Task TestAsyncSuspensionOverhead()
    {
        var pipe = new Pipe();
        _reader = pipe.Reader;
        _writer = pipe.Writer;

        // Start a background producer
        var producer = Task.Run(async () =>
        {
            await _writer.WriteAsync(_jsonData);
            await _writer.CompleteAsync();
        });

        // The Consumer: This is where we measure Runtime-Async
        // We simulate an async transformation for every single item
        await TransformWithForcedYieldsAsync(_reader, _writer);

        await producer;
    }

    private async Task TransformWithForcedYieldsAsync(PipeReader input, PipeWriter output)
    {
        while (true)
        {
            var result = await input.ReadAsync();
            var buffer = result.Buffer;

            // Simulate processing each byte as an "item" to force max suspensions
            // In a real scenario, this would be your YieldValue logic
            foreach (var segment in buffer)
            {
                for (int i = 0; i < segment.Length; i++)
                {
                    await _stream.WriteAsync(segment);
                }
            }

            input.AdvanceTo(buffer.End);
            if (result.IsCompleted) break;
        }
    }
}