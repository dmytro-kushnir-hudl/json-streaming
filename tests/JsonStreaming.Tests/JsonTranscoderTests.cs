using System.IO.Pipelines;
using System.Text;
using FluentAssertions;
using JsonStreaming;

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

    private static PipeReader ToPipe(string json, int bufferSize) =>
        PipeReader.Create(
            new MemoryStream(Encoding.UTF8.GetBytes(json)),
            new StreamPipeReaderOptions(bufferSize: bufferSize)
        );

    private static async Task<string[]> ProjectAsync(string json, NdJsonPath path, int bufferSize, bool direct)
    {
        var pipe = ToPipe(json, bufferSize);
        await using var output = new MemoryStream();
        var writer = PipeWriter.Create(output);

        if (direct)
            await pipe.ProjectNdJsonDirectAsync(path, writer);
        else
            await pipe.ProjectNdJsonAsync(path, writer);

        await writer.CompleteAsync();
        return Encoding.UTF8.GetString(output.ToArray())
            .Split('\n', StringSplitOptions.RemoveEmptyEntries);
    }

    [Theory]
    [InlineData(16)]
    [InlineData(37)]
    [InlineData(64)]
    public async Task ProjectNdJsonDirect_PrimitiveProjection_MatchesWriterVariant(int bufferSize)
    {
        var path = NdJsonPath.At("shipTo").Key("city");

        var expected = await ProjectAsync(OrderJson, path, bufferSize, direct: false);
        var actual = await ProjectAsync(OrderJson, path, bufferSize, direct: true);

        actual.Should().Equal(expected);
    }

    [Theory]
    [InlineData(16)]
    [InlineData(37)]
    [InlineData(64)]
    public async Task ProjectNdJsonDirect_ObjectProjection_MatchesWriterVariant(int bufferSize)
    {
        var json = $$"""
            {
              "items": [
                { "id": 1, "payload": "{{new string('x', 20_000)}}" },
                { "id": 2, "payload": "{{new string('y', 20_000)}}" }
              ]
            }
            """;
        var path = NdJsonPath.At("items").Each();

        var expected = await ProjectAsync(json, path, bufferSize, direct: false);
        var actual = await ProjectAsync(json, path, bufferSize, direct: true);

        actual.Should().Equal(expected);
    }
}