using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using JsonStreaming;

namespace JsonStreaming.Tests;

public class ProjectItemsTests
{
    private static PipeReader ToPipe(string json, int bufferSize = 64) =>
        PipeReader.Create(
            new MemoryStream(Encoding.UTF8.GetBytes(json)),
            new StreamPipeReaderOptions(bufferSize: bufferSize)
        );

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

    [Fact]
    public async Task ProjectItems_PrimitiveValue_CallbackReceivesBytes()
    {
        var items = new List<string>();
        var pipe = ToPipe(OrderJson);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            JsonPath.At("price"),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

        items.Should().Equal("199.95");
    }

    [Fact]
    public async Task ProjectItems_ObjectValue_CallbackReceivesCompleteJson()
    {
        var items = new List<string>();
        var pipe = ToPipe(OrderJson);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            JsonPath.At("shipTo"),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

        items.Should().HaveCount(1);
        var doc = System.Text.Json.JsonDocument.Parse(items[0]);
        doc.RootElement.GetProperty("city").GetString().Should().Be("Pretendville");
    }

    [Fact]
    public async Task ProjectItems_ArrayElements_CallbackPerElement()
    {
        var items = new List<string>();
        var pipe = ToPipe(PeopleJson);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            JsonPath.Each(),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

        items.Should().HaveCount(3);
        System.Text.Json.JsonDocument.Parse(items[0]).RootElement
            .GetProperty("name").GetString().Should().Be("Adeel Solangi");
    }

    [Fact]
    public async Task ProjectItems_NestedProperty_ExtractsFromEachElement()
    {
        var items = new List<string>();
        var pipe = ToPipe(PeopleJson);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            JsonPath.Each().Key("name"),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

        items.Should().Equal("\"Adeel Solangi\"", "\"Afzal Ghaffar\"", "\"Aamir Solangi\"");
    }

    [Fact]
    public async Task ProjectItems_NoMatch_CallbackNeverCalled()
    {
        var items = new List<string>();
        var pipe = ToPipe(OrderJson);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            JsonPath.At("nonexistent"),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

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

        await pipe.ProjectItemsAsync(
            JsonPath.Each(),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

        items.Should().HaveCount(3);
        foreach (var item in items)
            System.Text.Json.JsonDocument.Parse(item); // all valid JSON
    }

    [Fact]
    public async Task ProjectItems_LargeItemSpanningBuffers()
    {
        var json = $$"""
            { "items": [
                { "id": 1, "payload": "{{new string('x', 20_000)}}" },
                { "id": 2, "payload": "{{new string('y', 20_000)}}" }
            ] }
            """;

        var items = new List<string>();
        var pipe = ToPipe(json, bufferSize: 64);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            JsonPath.At("items").Each(),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

        items.Should().HaveCount(2);
        System.Text.Json.JsonDocument.Parse(items[0]).RootElement
            .GetProperty("id").GetInt32().Should().Be(1);
        System.Text.Json.JsonDocument.Parse(items[1]).RootElement
            .GetProperty("id").GetInt32().Should().Be(2);
    }

    [Fact]
    public async Task ProjectItems_WritesToOutputPipeWriter()
    {
        var pipe = ToPipe(PeopleJson);
        await using var outputStream = new MemoryStream();
        var output = PipeWriter.Create(outputStream);

        await pipe.ProjectItemsAsync(
            JsonPath.Each().Key("name"),
            output,
            (itemBytes, writer) =>
            {
                foreach (var segment in itemBytes)
                    writer.Write(segment.Span);
                writer.Write("\n"u8);
                return ValueTask.CompletedTask;
            });

        await output.FlushAsync();
        var result = Encoding.UTF8.GetString(outputStream.ToArray());
        var lines = result.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        lines.Should().Equal("\"Adeel Solangi\"", "\"Afzal Ghaffar\"", "\"Aamir Solangi\"");
    }

    // language=JSON
    private const string NestedArraysJson = """
        { "data": { "pages": [
            { "todos": [{"id":1},{"id":2}] },
            { "todos": [{"id":3}] }
        ] } }
        """;

    [Fact]
    public async Task ProjectItems_SelectMany_FlattensNestedArrays()
    {
        var items = new List<string>();
        var pipe = ToPipe(NestedArraysJson);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            JsonPath.At("data").Key("pages").Each().Key("todos").Each(),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

        items.Should().HaveCount(3);
    }

    [Fact]
    public async Task ProjectItems_EmptyArray_NoCallbacks()
    {
        var json = """{"items":[]}""";
        var items = new List<string>();
        var pipe = ToPipe(json);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            JsonPath.At("items").Each(),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ProjectItemsHighLevel_WritesProjectedArray()
    {
        var pipe = ToPipe(PeopleJson, bufferSize: 37);
        await using var outputStream = new MemoryStream();
        var output = PipeWriter.Create(outputStream);

        await pipe.ProjectItemsAsyncHighLevel(
            JsonPath.Each(),
            output,
            (itemBytes, bufferWriter) =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                var person = JsonDocument.ParseValue(ref reader).RootElement;

                using var writer = new Utf8JsonWriter(bufferWriter);
                writer.WriteStartObject();
                writer.WriteString("name", person.GetProperty("name").GetString());
                writer.WriteEndObject();
                writer.Flush();
                return ValueTask.CompletedTask;
            });

        var result = Encoding.UTF8.GetString(outputStream.ToArray());
        using var doc = JsonDocument.Parse(result);
        doc.RootElement.ValueKind.Should().Be(JsonValueKind.Array);
        doc.RootElement.GetArrayLength().Should().Be(3);
        doc.RootElement[1].GetProperty("name").GetString().Should().Be("Afzal Ghaffar");
    }

    [Fact]
    public async Task ProjectItemsHighLevel_CanSkipItemsByWritingNothing()
    {
        var pipe = ToPipe(PeopleJson, bufferSize: 16);
        await using var outputStream = new MemoryStream();
        var output = PipeWriter.Create(outputStream);

        await pipe.ProjectItemsAsyncHighLevel(
            JsonPath.Each(),
            output,
            (itemBytes, bufferWriter) =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                var person = JsonDocument.ParseValue(ref reader).RootElement;
                if (person.GetProperty("version").GetDouble() < 2)
                    return ValueTask.CompletedTask;

                using var writer = new Utf8JsonWriter(bufferWriter);
                writer.WriteStartObject();
                writer.WriteString("name", person.GetProperty("name").GetString());
                writer.WriteEndObject();
                writer.Flush();
                return ValueTask.CompletedTask;
            });

        using var doc = JsonDocument.Parse(outputStream.ToArray());
        doc.RootElement.GetArrayLength().Should().Be(2);
    }

    [Fact]
    public async Task ProjectItemsHighLevel_LargeItemsStillProjectAcrossBuffers()
    {
        var json = $$"""
            { "items": [
                { "id": 1, "payload": "{{new string('x', 20_000)}}" },
                { "id": 2, "payload": "{{new string('y', 20_000)}}" }
            ] }
            """;

        var pipe = ToPipe(json, bufferSize: 64);
        await using var outputStream = new MemoryStream();
        var output = PipeWriter.Create(outputStream);

        await pipe.ProjectItemsAsyncHighLevel(
            JsonPath.At("items").Each(),
            output,
            (itemBytes, bufferWriter) =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                using var doc = JsonDocument.ParseValue(ref reader);
                using var writer = new Utf8JsonWriter(bufferWriter);
                writer.WriteNumberValue(doc.RootElement.GetProperty("id").GetInt32());
                writer.Flush();
                return ValueTask.CompletedTask;
            });

        using var doc = JsonDocument.Parse(outputStream.ToArray());
        doc.RootElement.EnumerateArray().Select(item => item.GetInt32()).Should().Equal(1, 2);
    }
}
