using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using FluentAssertions;
using JsonStreaming;

namespace JsonStreaming.Tests;

// Source-gen context for tests
public sealed record TestItem
{
    public int Id { get; init; }
    public string Name { get; init; } = "";
    public double Score { get; init; }
}

public sealed record TestOutput
{
    public int Id { get; init; }
    public string Label { get; init; } = "";
    public bool Passed { get; init; }
}

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(TestItem))]
[JsonSerializable(typeof(TestOutput))]
public partial class TestJsonContext : JsonSerializerContext;

public class TypedApiTests
{
    private static PipeReader ToPipe(string json) =>
        PipeReader.Create(
            new MemoryStream(Encoding.UTF8.GetBytes(json)),
            new StreamPipeReaderOptions(bufferSize: 8192)
        );

    [Fact]
    public async Task ProcessArray_Typed_DeserializesItems()
    {
        var json = """{"items":[{"id":1,"name":"alice","score":9.5},{"id":2,"name":"bob","score":7.0}]}""";
        var pipe = ToPipe(json);

        var names = new List<string>();
        await JsonStreamReaderTyped.ProcessArrayAsync(
            pipe,
            "items",
            TestJsonContext.Default.TestItem,
            item => names.Add(item.Name)
        );

        names.Should().Equal("alice", "bob");
    }

    [Fact]
    public async Task ProcessArray_Typed_WithJsonPath()
    {
        var json = """{"data":{"items":[{"id":1,"name":"x","score":1.0}]}}""";
        var pipe = ToPipe(json);
        var path = JsonPath.Root.Property("data"u8).Property("items"u8);

        var ids = new List<int>();
        await JsonStreamReaderTyped.ProcessArrayAsync(
            pipe,
            path,
            TestJsonContext.Default.TestItem,
            item => ids.Add(item.Id)
        );

        ids.Should().Equal(1);
    }

    [Fact]
    public async Task WriteArray_Typed_TransformAndSerialize()
    {
        var json = """{"items":[{"id":1,"name":"alice","score":9.5},{"id":2,"name":"bob","score":4.0}]}""";
        var pipe = ToPipe(json);

        var output = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(output);

        writer.WriteStartArray();
        var count = await JsonStreamReaderTyped.WriteArrayAsync(
            pipe,
            "items",
            writer,
            TestJsonContext.Default.TestItem,
            TestJsonContext.Default.TestOutput,
            item => new TestOutput
            {
                Id = item.Id,
                Label = item.Name.ToUpper(),
                Passed = item.Score >= 5.0,
            }
        );
        writer.WriteEndArray();
        writer.Flush();

        count.Should().Be(2);

        var result = JsonDocument.Parse(output.WrittenMemory);
        result.RootElement[0].GetProperty("id").GetInt32().Should().Be(1);
        result.RootElement[0].GetProperty("label").GetString().Should().Be("ALICE");
        result.RootElement[0].GetProperty("passed").GetBoolean().Should().BeTrue();
        result.RootElement[1].GetProperty("passed").GetBoolean().Should().BeFalse();
    }

    [Fact]
    public async Task WriteArray_Typed_FilterByReturningNull()
    {
        var json = """{"items":[{"id":1,"name":"yes","score":10.0},{"id":2,"name":"no","score":2.0},{"id":3,"name":"yes","score":8.0}]}""";
        var pipe = ToPipe(json);

        var output = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(output);

        writer.WriteStartArray();
        var count = await JsonStreamReaderTyped.WriteArrayAsync(
            pipe,
            "items",
            writer,
            TestJsonContext.Default.TestItem,
            TestJsonContext.Default.TestOutput,
            item => item.Score >= 5.0
                ? new TestOutput { Id = item.Id, Label = item.Name, Passed = true }
                : null // filtered out
        );
        writer.WriteEndArray();
        writer.Flush();

            count.Should().Be(2);
        var result = JsonDocument.Parse(output.WrittenMemory);
        result.RootElement.GetArrayLength().Should().Be(2);
        result.RootElement[0].GetProperty("id").GetInt32().Should().Be(1);
        result.RootElement[1].GetProperty("id").GetInt32().Should().Be(3);
    }

    [Fact]
    public async Task WriteArray_Typed_SameTypeInOut()
    {
        var json = """{"items":[{"id":1,"name":"a","score":1.0},{"id":2,"name":"b","score":2.0}]}""";
        var pipe = ToPipe(json);

        var output = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(output);

        writer.WriteStartArray();
        var count = await JsonStreamReaderTyped.WriteArrayAsync(
            pipe,
            "items",
            writer,
            TestJsonContext.Default.TestItem
        );
        writer.WriteEndArray();
        writer.Flush();

        count.Should().Be(2);
        var result = JsonDocument.Parse(output.WrittenMemory);
        result.RootElement[0].GetProperty("name").GetString().Should().Be("a");
    }

    [Fact]
    public async Task WriteArray_Typed_SelectMany()
    {
        var json = """{"groups":[{"items":[{"id":1,"name":"a","score":1.0}]},{"items":[{"id":2,"name":"b","score":2.0}]}]}""";
        var pipe = ToPipe(json);
        var path = JsonPath.Root.Property("groups"u8).Each().Property("items"u8);

        var output = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(output);

        writer.WriteStartArray();
        var count = await JsonStreamReaderTyped.WriteArrayAsync(
            pipe,
            path,
            writer,
            TestJsonContext.Default.TestItem,
            TestJsonContext.Default.TestOutput,
            item => new TestOutput { Id = item.Id, Label = item.Name, Passed = true }
        );
        writer.WriteEndArray();
        writer.Flush();

        count.Should().Be(2);
        var result = JsonDocument.Parse(output.WrittenMemory);
        result.RootElement[0].GetProperty("label").GetString().Should().Be("a");
        result.RootElement[1].GetProperty("label").GetString().Should().Be("b");
    }
}
