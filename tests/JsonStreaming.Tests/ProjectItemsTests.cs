using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using FluentAssertions;

namespace JsonStreaming.Tests;

public class ProjectItemsTests
{
    // language=JSON
    private const string OrderJson = """
                                     { "name": "Alice", "price": 199.95,
                                       "shipTo": { "city": "Pretendville", "zip": "98999" } }
                                     """;

    // language=JSON
    private const string PeopleJson = """
                                      [
                                        { "name": "Adeel Solangi",  "language": "Sindhi", "version": 6.1  },
                                        { "name": "Afzal Ghaffar",  "language": "Sindhi", "version": 1.88 },
                                        { "name": "Aamir Solangi",  "language": "Sindhi", "version": 7.27 }
                                      ]
                                      """;

    // language=JSON
    private const string NestedArraysJson = """
                                            { "data": { "pages": [
                                                { "todos": [{"id":1},{"id":2}] },
                                                { "todos": [{"id":3}] }
                                            ] } }
                                            """;

    private static PipeReader ToPipe(string json, int bufferSize = 64)
    {
        return PipeReader.Create(
            new MemoryStream(Encoding.UTF8.GetBytes(json)),
            new StreamPipeReaderOptions(bufferSize: bufferSize)
        );
    }

    [Fact]
    public async Task ProjectItems_PrimitiveValue_CallbackReceivesBytes()
    {
        var items = new List<string>();
        var pipe = ToPipe(OrderJson);
        var output = PipeWriter.Create(Stream.Null);

        var currentCancellationToken = TestContext.Current.CancellationToken;
        await pipe.TransformItemsAsync(output, JsonPath.At("price"),
            (itemBytes, writer) => { items.Add(Encoding.UTF8.GetString(itemBytes)); }, ct: currentCancellationToken);

        items.Should().Equal("199.95");
    }

    [Fact]
    public async Task ProjectItems_ObjectValue_CallbackReceivesCompleteJson()
    {
        var items = new List<string>();
        var pipe = ToPipe(OrderJson);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.TransformItemsAsync(output, JsonPath.At("shipTo"),
            (itemBytes, _) => { items.Add(Encoding.UTF8.GetString(itemBytes)); },
            ct: TestContext.Current.CancellationToken);

        items.Should().HaveCount(1);
        var doc = JsonDocument.Parse(items[0]);
        doc.RootElement.GetProperty("city").GetString().Should().Be("Pretendville");
    }

    [Fact]
    public async Task ProjectItems_ArrayElements_CallbackPerElement()
    {
        var items = new List<string>();
        var pipe = ToPipe(PeopleJson);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.TransformItemsAsync(output, JsonPath.Each(),
            (itemBytes, writer) => { items.Add(Encoding.UTF8.GetString(itemBytes)); },
            ct: TestContext.Current.CancellationToken);

        items.Should().HaveCount(3);
        JsonDocument.Parse(items[0]).RootElement
            .GetProperty("name").GetString().Should().Be("Adeel Solangi");
    }

    [Fact]
    public async Task ProjectItems_NestedProperty_ExtractsFromEachElement()
    {
        var items = new List<string>();
        var pipe = ToPipe(PeopleJson);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.TransformItemsAsync(output, JsonPath.Each().Key("name"),
            (itemBytes, writer) => { items.Add(Encoding.UTF8.GetString(itemBytes)); },
            ct: TestContext.Current.CancellationToken);

        items.Should().Equal("\"Adeel Solangi\"", "\"Afzal Ghaffar\"", "\"Aamir Solangi\"");
    }

    [Fact]
    public async Task ProjectItems_NoMatch_CallbackNeverCalled()
    {
        var items = new List<string>();
        var pipe = ToPipe(OrderJson);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.TransformItemsAsync(output, JsonPath.At("nonexistent"),
            (itemBytes, writer) => { items.Add(Encoding.UTF8.GetString(itemBytes)); },
            ct: TestContext.Current.CancellationToken);

        items.Should().BeEmpty();
    }

    [Theory]
    [InlineData(16)]
    [InlineData(37)]
    [InlineData(64)]
    [InlineData(4096)]
    public async Task ProjectItems_SmallBuffers_SameResults(int bufferSize)
    {
        var items = new List<string>();
        var pipe = ToPipe(PeopleJson, bufferSize);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.TransformItemsAsync(output, JsonPath.Each(),
            (itemBytes, _) => { items.Add(Encoding.UTF8.GetString(itemBytes)); },
            ct: TestContext.Current.CancellationToken);

        items.Should().HaveCount(3);
        foreach (var item in items)
            JsonDocument.Parse(item); // all valid JSON
    }

    [Fact]
    public async Task ProjectItems_LargeItemSpanningBuffers()
    {
        var ct = TestContext.Current.CancellationToken;

        var json = $$"""
                     { "items": [
                         { "id": 1, "payload": "{{new string('x', 20_000)}}" },
                         { "id": 2, "payload": "{{new string('y', 20_000)}}" }
                     ] }
                     """;

        var items = new List<string>();
        var pipe = ToPipe(json, 64);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.TransformItemsAsync(
            output,
            JsonPath.At("items").Each(),
            (itemBytes, writer) => { items.Add(Encoding.UTF8.GetString(itemBytes)); }, ct: ct);

        items.Should().HaveCount(2);
        JsonDocument.Parse(items[0]).RootElement
            .GetProperty("id").GetInt32().Should().Be(1);
        JsonDocument.Parse(items[1]).RootElement
            .GetProperty("id").GetInt32().Should().Be(2);
    }

    [Fact]
    public async Task ProjectItems_WritesToOutputPipeWriter()
    {
        var ct = TestContext.Current.CancellationToken;

        var pipe = ToPipe(PeopleJson);
        await using var outputStream = new MemoryStream();
        var output = PipeWriter.Create(outputStream);

        await pipe.TransformItemsAsync(output, JsonPath.Each().Key("name"),
            (itemBytes, writer) =>
            {
                foreach (var segment in itemBytes)
                    writer.Write(segment.Span);
                writer.Write("\n"u8);
            }, ct: ct);

        var result = Encoding.UTF8.GetString(outputStream.ToArray());
        var lines = result.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        lines.Should().Equal("\"Adeel Solangi\"", "\"Afzal Ghaffar\"", "\"Aamir Solangi\"");
    }

    [Fact]
    public async Task ProjectItems_SelectMany_FlattensNestedArrays()
    {
        var items = new List<string>();
        var pipe = ToPipe(NestedArraysJson);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.TransformItemsAsync(output, JsonPath.At("data").Key("pages").Each().Key("todos").Each(),
            (itemBytes, writer) => { items.Add(Encoding.UTF8.GetString(itemBytes)); },
            ct: TestContext.Current.CancellationToken);

        items.Should().HaveCount(3);
    }

    [Fact]
    public async Task ProjectItems_EmptyArray_NoCallbacks()
    {
        var json = """{"items":[]}""";
        var items = new List<string>();
        var pipe = ToPipe(json);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.TransformItemsAsync(output, JsonPath.At("items").Each(),
            (itemBytes, writer) => { items.Add(Encoding.UTF8.GetString(itemBytes)); },
            ct: TestContext.Current.CancellationToken);

        items.Should().BeEmpty();
    }

    // ── CalculateValueSlice tests ────────────────────────────────────────────

    // Verify exact bytes for YieldValue (primitive) — no quoting/unquoting, raw JSON bytes.
    [Theory]
    [InlineData("""{"x":42}""", "x", "42")]
    [InlineData("""{"x":3.14}""", "x", "3.14")]
    [InlineData("""{"x":true}""", "x", "true")]
    [InlineData("""{"x":false}""", "x", "false")]
    [InlineData("""{"x":null}""", "x", "null")]
    [InlineData("""{"x":"hello"}""", "x", "\"hello\"")]
    public async Task Slice_Primitive_ExactBytes(string json, string key, string expected)
    {
        var captured = new List<string>();
        await ToPipe(json).TransformItemsAsync(
            PipeWriter.Create(Stream.Null),
            JsonPath.At(key),
            (bytes, _) => captured.Add(Encoding.UTF8.GetString(bytes)),
            ct: TestContext.Current.CancellationToken);

        captured.Should().Equal(expected);
    }

    // Verify exact bytes for EndCapture (object/array) in a single buffer.
    [Theory]
    [InlineData("""{"x":{}}""", "x", """{}""")]
    [InlineData("""{"x":{"a":1}}""", "x", """{"a":1}""")]
    [InlineData("""{"x":[1,2,3]}""", "x", """[1,2,3]""")]
    [InlineData("""{"x":{"a":{"b":1}}}""", "x", """{"a":{"b":1}}""")]
    public async Task Slice_Object_ExactBytes_SingleBuffer(string json, string key, string expected)
    {
        var captured = new List<string>();
        await ToPipe(json, json.Length + 1).TransformItemsAsync(
            PipeWriter.Create(Stream.Null),
            JsonPath.At(key),
            (bytes, _) => captured.Add(Encoding.UTF8.GetString(bytes)),
            ct: TestContext.Current.CancellationToken);

        captured.Should().Equal(expected);
    }

    // Same JSON, but buffer sizes that force the object to span multiple pipe reads.
    // This exercises the BeginSegment() re-anchor and cross-buffer _valueEnd computation.
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(7)]
    [InlineData(13)]
    public async Task Slice_Object_ExactBytes_CrossBuffer(int bufferSize)
    {
        const string json = """{"x":{"a":1,"b":2}}""";
        const string expected = """{"a":1,"b":2}""";

        var captured = new List<string>();
        await ToPipe(json, bufferSize).TransformItemsAsync(
            PipeWriter.Create(Stream.Null),
            JsonPath.At("x"),
            (bytes, _) => captured.Add(Encoding.UTF8.GetString(bytes)),
            ct: TestContext.Current.CancellationToken);

        captured.Should().Equal(expected);
    }

    // Multiple items across buffers — each slice must be independent and correct.
    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(16)]
    public async Task Slice_MultipleObjects_CrossBuffer_AllCorrect(int bufferSize)
    {
        const string json = """[{"id":1},{"id":2},{"id":3}]""";
        var expected = new[] { """{"id":1}""", """{"id":2}""", """{"id":3}""" };

        var captured = new List<string>();
        await ToPipe(json, bufferSize).TransformItemsAsync(
            PipeWriter.Create(Stream.Null),
            JsonPath.Each(),
            (bytes, _) => captured.Add(Encoding.UTF8.GetString(bytes)),
            ct: TestContext.Current.CancellationToken);

        captured.Should().Equal(expected);
    }

    // Empty object is the minimal EndCapture case: BeginCapture immediately followed by EndCapture.
    [Fact]
    public async Task Slice_EmptyObject_ExactBytes()
    {
        var captured = new List<string>();
        await ToPipe("""{"x":{}}""").TransformItemsAsync(
            PipeWriter.Create(Stream.Null),
            JsonPath.At("x"),
            (bytes, _) => captured.Add(Encoding.UTF8.GetString(bytes)),
            ct: TestContext.Current.CancellationToken);

        captured.Should().Equal("{}");
    }
}