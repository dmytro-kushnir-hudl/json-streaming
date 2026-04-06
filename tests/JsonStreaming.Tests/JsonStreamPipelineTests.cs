using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using JsonStreaming;

namespace JsonStreaming.Tests;

public class JsonStreamPipelineTests
{
    private static PipeReader ToPipe(string json) =>
        PipeReader.Create(
            new MemoryStream(Encoding.UTF8.GetBytes(json)),
            new StreamPipeReaderOptions(bufferSize: 8192)
        );

    [Fact]
    public async Task TransformArray_WhenTransformThrows_ProducesValidErrorEnvelope()
    {
        var pipe = ToPipe(
            """{"items":[{"id":1,"name":"ok","score":9.5},{"id":2,"name":"boom","score":1.0}]}"""
        );
        await using var outputStream = new MemoryStream();
        var output = PipeWriter.Create(outputStream);

        var count = await JsonStreamPipeline.TransformArrayAsync(
            pipe,
            "items",
            output,
            "results",
            TestJsonContext.Default.TestItem,
            TestJsonContext.Default.TestOutput,
            item =>
            {
                if (item.Id == 2)
                    throw new InvalidOperationException("boom");

                return new TestOutput
                {
                    Id = item.Id,
                    Label = item.Name,
                    Passed = item.Score >= 5,
                };
            }
        );

        await output.CompleteAsync();
        count.Should().Be(1);

        var result = JsonDocument.Parse(outputStream.ToArray());
        result.RootElement.GetProperty("count").GetInt32().Should().Be(1);
        result.RootElement.GetProperty("error").GetString().Should().Be("boom");
        result.RootElement.GetProperty("results").GetArrayLength().Should().Be(1);
        result.RootElement.GetProperty("results")[0].GetProperty("id").GetInt32().Should().Be(1);
    }

    [Fact]
    public async Task PassthroughArray_TruncatedJson_ProducesValidErrorEnvelope()
    {
        var pipe = ToPipe("""{"items":[1,2""");
        await using var outputStream = new MemoryStream();
        var output = PipeWriter.Create(outputStream);

        var count = await JsonStreamPipeline.PassthroughArrayAsync(
            pipe,
            "items",
            output,
            "items"
        );

        await output.CompleteAsync();
        count.Should().Be(1);

        var result = JsonDocument.Parse(outputStream.ToArray());
        result.RootElement.GetProperty("count").GetInt32().Should().Be(1);
        result.RootElement.GetProperty("error").GetString().Should().NotBeNullOrWhiteSpace();
        result.RootElement.GetProperty("items").GetArrayLength().Should().Be(1);
        result.RootElement.GetProperty("items")[0].GetInt32().Should().Be(1);
    }
}