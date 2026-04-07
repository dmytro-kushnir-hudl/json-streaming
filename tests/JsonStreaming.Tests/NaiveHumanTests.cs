using System.IO.Pipelines;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace JsonStreaming.Tests;

public class NaiveHumanTests
{
    private static readonly HttpClient Client = new();

    [Theory]
    [InlineData("64KB-min.json")]
    [InlineData("5MB-min.json")]
    [InlineData("64KB.json")]
    [InlineData("5MB.json")]
    public async Task PipeIt_Handrolled(string fileName)
    {
        var ct = TestContext.Current.CancellationToken;
        var uri = $"https://microsoftedge.github.io/Demos/json-dummy-data/{fileName}";
        var rawBytes = await Client.GetByteArrayAsync(uri, ct);

        var expected = JsonSerializer.Serialize(
            JsonDocument.Parse(rawBytes).RootElement,
            new JsonSerializerOptions { WriteIndented = true, Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping }
        );

        var inputPipe = PipeReader.Create(await Client.GetStreamAsync(uri, ct));
        var outputStream = new MemoryStream();
        var outputPipe = PipeWriter.Create(outputStream);
        await inputPipe.ProxyFormattedJsonAsync(outputPipe, default, ct);
        await outputPipe.CompleteAsync();
        var actual = Encoding.UTF8.GetString(outputStream.ToArray());

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData("64KB-min.json")]
    [InlineData("5MB-min.json")]
    [InlineData("64KB.json")]
    [InlineData("5MB.json")]
    public async Task PipeIt_Handrolled_Min(string fileName)
    {
        var ct = TestContext.Current.CancellationToken;
        var uri = $"https://microsoftedge.github.io/Demos/json-dummy-data/{fileName}";
        var rawBytes = await Client.GetByteArrayAsync(uri, ct);

        var expected = JsonSerializer.Serialize(
            JsonDocument.Parse(rawBytes).RootElement,
            new JsonSerializerOptions { WriteIndented = false, Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping }
        );

        var inputPipe = PipeReader.Create(await Client.GetStreamAsync(uri, ct));
        var outputStream = new MemoryStream();
        var outputPipe = PipeWriter.Create(outputStream);
        await inputPipe.ProxyMinifiedJsonAsync(outputPipe, default, ct);
        await outputPipe.CompleteAsync();
        var actual = Encoding.UTF8.GetString(outputStream.ToArray());

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData("missing-colon.json")]
    [InlineData("unterminated.json")]
    [InlineData("binary-data.json")]
    public async Task PipeIt_Handrolled_Malformed(string fileName)
    {
        var ct = TestContext.Current.CancellationToken;

        var rawBytes = await Client.GetByteArrayAsync(
            $"https://microsoftedge.github.io/Demos/json-dummy-data/{fileName}",
            ct
        );

        var inputPipe = PipeReader.Create(new MemoryStream(rawBytes));
        var outputPipe = PipeWriter.Create(new MemoryStream());

        await Assert.ThrowsAnyAsync<JsonException>(() =>
            inputPipe.ProxyFormattedJsonAsync(outputPipe, default, ct));
    }

    // ── ProjectNdJsonAsync ────────────────────────────────────────────────────

    // language=JSON
    const string OrderJson = """
        { "name"   : "Alice Brown",
          "sku"    : "54321",
          "price"  : 199.95,
          "shipTo" : { "name" : "Bob Brown", "city" : "Pretendville", "zip" : "98999" },
          "billTo" : { "name" : "Alice Brown", "city" : "Pretendville", "zip" : "98999" }
        }
        """;

    // language=JSON
    const string PeopleJson = """
        [
          { "name": "Adeel Solangi",  "language": "Sindhi", "version": 6.1  },
          { "name": "Afzal Ghaffar",  "language": "Sindhi", "version": 1.88 },
          { "name": "Aamir Solangi",  "language": "Sindhi", "version": 7.27 }
        ]
        """;

    private static string[] Project(string json, NdJsonPath path)
    {
        var bytes = Encoding.UTF8.GetBytes(json);
        var pipe = PipeReader.Create(new MemoryStream(bytes));
        var output = new MemoryStream();
        var writer = PipeWriter.Create(output);
        pipe.ProjectNdJsonAsync(path, writer).GetAwaiter().GetResult();
        writer.CompleteAsync().GetAwaiter().GetResult();
        return Encoding.UTF8.GetString(output.ToArray())
            .Split('\n', StringSplitOptions.RemoveEmptyEntries);
    }

    [Fact]
    public void Project_TopLevelPrimitive()
    {
        var lines = Project(OrderJson, NdJsonPath.At("price"));
        Assert.Equal(["199.95"], lines);
    }

    [Fact]
    public void Project_TopLevelObject()
    {
        var lines = Project(OrderJson, NdJsonPath.At("shipTo"));
        Assert.Single(lines);
        var obj = JsonDocument.Parse(lines[0]).RootElement;
        Assert.Equal("Bob Brown", obj.GetProperty("name").GetString());
        Assert.Equal("Pretendville", obj.GetProperty("city").GetString());
    }

    [Fact]
    public void Project_NestedPrimitive()
    {
        var lines = Project(OrderJson, NdJsonPath.At("shipTo").Key("city"));
        Assert.Equal(["\"Pretendville\""], lines);
    }

    [Fact]
    public void Project_ArrayElements()
    {
        var lines = Project(PeopleJson, NdJsonPath.Each());
        Assert.Equal(3, lines.Length);
        Assert.All(lines, line => JsonDocument.Parse(line));
        Assert.Equal("Adeel Solangi", JsonDocument.Parse(lines[0]).RootElement.GetProperty("name").GetString());
        Assert.Equal("Aamir Solangi", JsonDocument.Parse(lines[2]).RootElement.GetProperty("name").GetString());
    }

    [Fact]
    public void Project_PropertyOfEachArrayElement()
    {
        var lines = Project(PeopleJson, NdJsonPath.Each().Key("name"));
        Assert.Equal(["\"Adeel Solangi\"", "\"Afzal Ghaffar\"", "\"Aamir Solangi\""], lines);
    }

    [Fact]
    public void Project_NumberOfEachArrayElement()
    {
        var lines = Project(PeopleJson, NdJsonPath.Each().Key("version"));
        Assert.Equal(["6.1", "1.88", "7.27"], lines);
    }

    [Fact]
    public void Project_NoMatch_ReturnsEmpty()
    {
        var lines = Project(OrderJson, NdJsonPath.At("nonexistent"));
        Assert.Empty(lines);
    }
}
