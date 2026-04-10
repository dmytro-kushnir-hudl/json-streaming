using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using FluentAssertions;

namespace JsonStreaming.Tests;

public class JsonTranscoderTests
{
    // language=JSON
    private const string OrderJson = """
                                     { "name"   : "Alice Brown",
                                       "sku"    : "54321",
                                       "price"  : 199.95,
                                       "shipTo" : { "name" : "Bob Brown", "city" : "Pretendville", "zip" : "98999" },
                                       "billTo" : { "name" : "Alice Brown", "city" : "Pretendville", "zip" : "98999" }
                                     }
                                     """;

    // ── Composability: projected output round-trips through JsonSerializer ──

    // language=JSON
    private const string PeopleJson = """
                                      [
                                        { "name": "Adeel Solangi",  "language": "Sindhi", "version": 6.1  },
                                        { "name": "Afzal Ghaffar",  "language": "Sindhi", "version": 1.88 },
                                        { "name": "Aamir Solangi",  "language": "Sindhi", "version": 7.27 }
                                      ]
                                      """;

    private static PipeReader ToPipe(string json, int bufferSize)
    {
        return PipeReader.Create(
            new MemoryStream(Encoding.UTF8.GetBytes(json)),
            new StreamPipeReaderOptions(bufferSize: bufferSize)
        );
    }

    private static async Task<string[]> ProjectAsync(string json, JsonPath path, int bufferSize)
    {
        var pipe = ToPipe(json, bufferSize);
        await using var output = new MemoryStream();
        var writer = PipeWriter.Create(output);

        await pipe.TransformItemsAsync(writer, path, (bytes, w) => w.Write(bytes));

        await writer.CompleteAsync();
        return Encoding.UTF8.GetString(output.ToArray())
            .Split('\n', StringSplitOptions.RemoveEmptyEntries);
    }

    [Theory]
    [InlineData(16)]
    [InlineData(37)]
    [InlineData(64)]
    public async Task Project_PrimitiveValue_AcrossBufferSizes(int bufferSize)
    {
        var path = JsonPath.At("shipTo").Key("city");
        var result = await ProjectAsync(OrderJson, path, bufferSize);
        result.Should().Equal("\"Pretendville\"");
    }

    [Theory]
    [InlineData(16)]
    [InlineData(37)]
    [InlineData(64)]
    public async Task Project_LargeObject_AcrossBufferSizes(int bufferSize)
    {
        var json = $$"""
                     {
                       "items": [
                         { "id": 1, "payload": "{{new string('x', 20_000)}}" },
                         { "id": 2, "payload": "{{new string('y', 20_000)}}" }
                       ]
                     }
                     """;
        var path = JsonPath.At("items").Each();

        var result = await ProjectAsync(json, path, bufferSize);
        result.Should().HaveCount(2);
        result[0].Should().Contain("\"id\":");
        JsonDocument.Parse(result[0]).RootElement.GetProperty("id").GetInt32().Should().Be(1);
        JsonDocument.Parse(result[1]).RootElement.GetProperty("id").GetInt32().Should().Be(2);
    }

    [Fact]
    public async Task Project_EachElement_RoundTrips()
    {
        var projected = await ProjectAsync(PeopleJson, JsonPath.Each(), 64);
        projected.Should().HaveCount(3);

        for (var i = 0; i < projected.Length; i++)
        {
            // verbatim output preserves input whitespace; verify it's valid JSON
            // and that normalising → re-normalising is stable (idempotent round-trip)
            var once = JsonSerializer.Serialize(JsonDocument.Parse(projected[i]).RootElement);
            var twice = JsonSerializer.Serialize(JsonDocument.Parse(once).RootElement);
            twice.Should().Be(once, $"element {i} normalised JSON should be stable");
        }
    }
}