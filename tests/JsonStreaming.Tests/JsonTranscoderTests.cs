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

    private static PipeReader ToPipe(string json, int bufferSize) =>
        PipeReader.Create(
            new MemoryStream(Encoding.UTF8.GetBytes(json)),
            new StreamPipeReaderOptions(bufferSize: bufferSize)
        );

    private static async Task<string[]> ProjectAsync(string json, JsonPath path, int bufferSize, bool direct)
    {
        var pipe = ToPipe(json, bufferSize);
        await using var output = new MemoryStream();
        var writer = PipeWriter.Create(output);

        if (direct)
            await pipe.ProjectNdJsonVerbatimAsync(path, writer);
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
        var path = JsonPath.At("shipTo").Key("city");

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
        var path = JsonPath.At("items").Each();

        var expected = await ProjectAsync(json, path, bufferSize, direct: false);
        var actual = await ProjectAsync(json, path, bufferSize, direct: true);

        actual.Should().Equal(expected);
    }

    // ── Composability: formatted projection vs formatted proxy ──────────────

    // language=JSON
    private const string PeopleJson = """
        [
          { "name": "Adeel Solangi",  "language": "Sindhi", "version": 6.1  },
          { "name": "Afzal Ghaffar",  "language": "Sindhi", "version": 1.88 },
          { "name": "Aamir Solangi",  "language": "Sindhi", "version": 7.27 }
        ]
        """;

    /// <summary>
    /// Projects each array element, then formats each individually via
    /// JsonSerializer (reference implementation). Establishes expected output
    /// for a future FormattedRenderer: when it exists, formatted projection
    /// output should match these reference-formatted elements exactly.
    /// </summary>
    [Fact]
    public async Task FormattedProjection_EachElement_RoundTrips()
    {
        // Step 1: Project all elements via minified renderer
        var projected = await ProjectAsync(PeopleJson, JsonPath.Each(), bufferSize: 64, direct: false);
        projected.Should().HaveCount(3);

        // Step 2: Each projected element is valid JSON that round-trips
        var formatted = new string[projected.Length];
        for (int i = 0; i < projected.Length; i++)
        {
            var doc = JsonDocument.Parse(projected[i]);
            formatted[i] = JsonSerializer.Serialize(
                doc.RootElement,
                new JsonSerializerOptions { WriteIndented = true }
            );

            // Round-trip: formatted → minified should equal original projection
            var roundTripped = JsonSerializer.Serialize(
                JsonDocument.Parse(formatted[i]).RootElement
            );
            roundTripped.Should().Be(projected[i], $"element {i} should round-trip");
        }

        // Step 3: Verbatim renderer produces same number of elements
        // Note: verbatim output uses raw buffer copy which may differ in whitespace
        // from minified output, but element count should match
        var verbatim = await ProjectAsync(PeopleJson, JsonPath.Each(), bufferSize: 64, direct: true);
        verbatim.Should().HaveCount(projected.Length,
            "verbatim and minified renderers should find the same number of elements");

        // TODO: When FormattedRenderer exists, compare directly:
        // var formattedProjected = await ProjectFormattedAsync(PeopleJson, NdJsonPath.Each(), 64);
        // formattedProjected.Should().Equal(formatted);
    }
}