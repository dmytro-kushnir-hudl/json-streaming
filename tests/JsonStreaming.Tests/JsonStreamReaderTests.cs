using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using JsonStreaming;

namespace JsonStreaming.Tests;

public class JsonStreamReaderTests
{
    private static PipeReader ToPipe(string json, int bufferSize = 8192)
    {
        var bytes = Encoding.UTF8.GetBytes(json);
        return PipeReader.Create(
            new MemoryStream(bytes),
            new StreamPipeReaderOptions(bufferSize: bufferSize)
        );
    }

    private static List<T> ParseItems<T>(string json, string path, Func<JsonElement, T> selector)
    {
        var pipe = ToPipe(json);
        var items = new List<T>();
        JsonStreamReader
            .ProcessArrayAsync(
                pipe,
                path,
                bytes =>
                {
                    using var doc = JsonDocument.Parse(bytes);
                    items.Add(selector(doc.RootElement));
                }
            )
            .GetAwaiter()
            .GetResult();
        return items;
    }

    // ── Dot-path navigation ────────────────────────────────────────────────

    [Fact]
    public void FlatPath_YieldsItems()
    {
        var items = ParseItems(
            """{"messages":[{"id":1},{"id":2},{"id":3}]}""",
            "messages",
            e => e.GetProperty("id").GetInt32()
        );
        items.Should().Equal(1, 2, 3);
    }

    [Fact]
    public void NestedPath_YieldsItems()
    {
        var items = ParseItems(
            """{"response":{"data":{"items":[10,20,30]}}}""",
            "response.data.items",
            e => e.GetInt32()
        );
        items.Should().Equal(10, 20, 30);
    }

    [Fact]
    public async Task RootArray_YieldsItems()
    {
        var pipe = ToPipe("[1,2,3]");
        var items = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            "",
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                items.Add(doc.RootElement.GetInt32());
            }
        );
        items.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task EmptyArray_ReturnsZero()
    {
        var pipe = ToPipe("""{"messages":[]}""");
        var count = await JsonStreamReader.ProcessArrayAsync(pipe, "messages", _ => { });
        count.Should().Be(0);
    }

    [Fact]
    public async Task MissingPath_ReturnsZero()
    {
        var pipe = ToPipe("""{"other":[1,2,3]}""");
        var count = await JsonStreamReader.ProcessArrayAsync(pipe, "messages", _ => { });
        count.Should().Be(0);
    }

    [Fact]
    public void SkipsUnrelatedProperties()
    {
        var items = ParseItems(
            """{"meta":{"version":1},"messages":[{"id":1}],"footer":"ok"}""",
            "messages",
            e => e.GetProperty("id").GetInt32()
        );
        items.Should().Equal(1);
    }

    // ── Callback API ───────────────────────────────────────────────────────

    [Fact]
    public async Task ProcessArray_CountsItems()
    {
        var pipe = ToPipe("""{"items":["a","b","c","d"]}""");
        var count = await JsonStreamReader.ProcessArrayAsync(pipe, "items", _ => { });
        count.Should().Be(4);
    }

    [Fact]
    public async Task ProcessArray_CallbackReceivesValidJson()
    {
        var pipe = ToPipe("""{"items":[{"name":"alice"},{"name":"bob"}]}""");
        var names = new List<string>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            "items",
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                names.Add(doc.RootElement.GetProperty("name").GetString()!);
            }
        );
        names.Should().Equal("alice", "bob");
    }

    // ── JsonPath overloads ─────────────────────────────────────────────────

    [Fact]
    public async Task ProcessArray_JsonPath_NavigatesCorrectly()
    {
        var pipe = ToPipe("""{"response":{"data":{"items":[1,2,3]}}}""");
        var path = JsonPath.Root.Property("response"u8).Property("data"u8).Property("items"u8);
        var count = await JsonStreamReader.ProcessArrayAsync(pipe, path, _ => { });
        count.Should().Be(3);
    }

    [Fact]
    public async Task ProcessArray_JsonPathRoot_ReadsRootArray()
    {
        var pipe = ToPipe("""["a","b","c"]""");
        var count = await JsonStreamReader.ProcessArrayAsync(pipe, JsonPath.Root, _ => { });
        count.Should().Be(3);
    }

    // ── Buffer boundary robustness ─────────────────────────────────────────

    [Theory]
    [InlineData(16)]
    [InlineData(37)]
    [InlineData(64)]
    [InlineData(4096)]
    public async Task ProcessArray_SmallBuffers_SameResult(int bufferSize)
    {
        var pipe = ToPipe("""{"messages":[{"id":1},{"id":2},{"id":3}]}""", bufferSize);
        var count = await JsonStreamReader.ProcessArrayAsync(pipe, "messages", _ => { });
        count.Should().Be(3);
    }

    [Fact]
    public async Task ProcessArray_LargeItemSpanningBuffers()
    {
        var bigValue = new string('x', 50_000);
        var pipe = ToPipe($$"""{"items":[{"data":"{{bigValue}}"}]}""", bufferSize: 8192);
        var lengths = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            "items",
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                lengths.Add(doc.RootElement.GetProperty("data").GetString()!.Length);
            }
        );
        lengths.Should().Equal(50_000);
    }

    [Fact]
    public async Task ProcessArray_ManyItems_SmallBuffer()
    {
        var items = string.Join(",", Enumerable.Range(0, 500).Select(i => i));
        var json = $$$"""{"arr":[{{{items}}}]}""";
        var pipe = ToPipe(json, bufferSize: 32);
        var count = await JsonStreamReader.ProcessArrayAsync(pipe, "arr", _ => { });
        count.Should().Be(500);
    }

    // ── Mixed types ────────────────────────────────────────────────────────

    [Fact]
    public async Task ProcessArray_MixedTypes_AllProcessed()
    {
        var pipe = ToPipe("""{"arr":[1,"hello",true,null,{"k":"v"},[1,2]]}""");
        var types = new List<JsonValueKind>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            "arr",
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                types.Add(doc.RootElement.ValueKind);
            }
        );
        types.Should()
            .Equal(
                JsonValueKind.Number,
                JsonValueKind.String,
                JsonValueKind.True,
                JsonValueKind.Null,
                JsonValueKind.Object,
                JsonValueKind.Array
            );
    }

    // ── Cancellation ───────────────────────────────────────────────────────

    [Fact]
    public async Task ProcessArray_CancellationToken_Respected()
    {
        var pipe = ToPipe("""{"items":[1,2,3,4,5]}""");
        using var cts = new CancellationTokenSource();

        int count = 0;
        var act = async () =>
        {
            await JsonStreamReader.ProcessArrayAsync(
                pipe,
                "items",
                _ =>
                {
                    if (++count == 2)
                        cts.Cancel();
                },
                cts.Token
            );
        };

        await act.Should().ThrowAsync<OperationCanceledException>();
        count.Should().Be(2);
    }

    // ── Each() / select-many ───────────────────────────────────────────────

    [Fact]
    public async Task Each_FlattensTwoInnerArrays()
    {
        var json = """{"responses":[{"messages":[{"id":1},{"id":2}]},{"messages":[{"id":3}]}]}""";
        var pipe = ToPipe(json);
        var path = JsonPath.Root.Property("responses"u8).Each().Property("messages"u8);

        var ids = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            path,
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                ids.Add(doc.RootElement.GetProperty("id").GetInt32());
            }
        );
        ids.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task Each_NoSuffix_YieldsOuterElements()
    {
        var pipe = ToPipe("""{"items":[{"a":1},{"a":2},{"a":3}]}""");
        var path = JsonPath.Root.Property("items"u8).Each();

        var values = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            path,
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetProperty("a").GetInt32());
            }
        );
        values.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task Each_EmptyInnerArrays_ReturnsZero()
    {
        var pipe = ToPipe("""{"items":[{"msgs":[]},{"msgs":[]}]}""");
        var path = JsonPath.Root.Property("items"u8).Each().Property("msgs"u8);
        var count = await JsonStreamReader.ProcessArrayAsync(pipe, path, _ => { });
        count.Should().Be(0);
    }

    [Fact]
    public async Task Each_MixedEmpty_YieldsOnlyNonEmpty()
    {
        var json = """{"groups":[{"items":[]},{"items":[1,2]},{"items":[]},{"items":[3]}]}""";
        var pipe = ToPipe(json);
        var path = JsonPath.Root.Property("groups"u8).Each().Property("items"u8);

        var values = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            path,
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetInt32());
            }
        );
        values.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task Each_MissingSuffix_Skipped()
    {
        var json = """{"items":[{"data":[1]},{"other":"x"},{"data":[2,3]}]}""";
        var pipe = ToPipe(json);
        var path = JsonPath.Root.Property("items"u8).Each().Property("data"u8);

        var values = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            path,
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetInt32());
            }
        );
        values.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task Each_ExtraPropertiesSkipped()
    {
        var json =
            """{"items":[{"meta":"x","data":[10],"footer":"y"},{"data":[20,30],"extra":{"nested":true}}]}""";
        var pipe = ToPipe(json);
        var path = JsonPath.Root.Property("items"u8).Each().Property("data"u8);

        var values = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            path,
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetInt32());
            }
        );
        values.Should().Equal(10, 20, 30);
    }

    [Fact]
    public async Task Each_ParsedJsonPath()
    {
        var pipe = ToPipe("""{"groups":[{"items":[1]},{"items":[2,3]}]}""");
        var path = JsonPath.Parse("$.groups[*].items");

        var values = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            path,
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetInt32());
            }
        );
        values.Should().Equal(1, 2, 3);
    }

    [Theory]
    [InlineData(64)]
    [InlineData(128)]
    [InlineData(8192)]
    public async Task Each_SmallBuffers_SameResult(int bufferSize)
    {
        var json =
            """{"data":[{"items":[{"v":1},{"v":2}]},{"items":[{"v":3}]},{"items":[{"v":4},{"v":5},{"v":6}]}]}""";
        var pipe = ToPipe(json, bufferSize);
        var path = JsonPath.Root.Property("data"u8).Each().Property("items"u8);

        var values = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            path,
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetProperty("v").GetInt32());
            }
        );
        values.Should().Equal(1, 2, 3, 4, 5, 6);
    }

    [Fact]
    public async Task Each_EmptyOuterArray_ReturnsZero()
    {
        var pipe = ToPipe("""{"items":[]}""");
        var path = JsonPath.Root.Property("items"u8).Each().Property("data"u8);
        var count = await JsonStreamReader.ProcessArrayAsync(pipe, path, _ => { });
        count.Should().Be(0);
    }

    [Fact]
    public async Task Each_NestedSuffix()
    {
        var json =
            """{"pages":[{"response":{"data":{"items":[1,2]}}},{"response":{"data":{"items":[3]}}}]}""";
        var pipe = ToPipe(json);
        var path = JsonPath.Root
            .Property("pages"u8)
            .Each()
            .Property("response"u8)
            .Property("data"u8)
            .Property("items"u8);

        var values = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            path,
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetInt32());
            }
        );
        values.Should().Equal(1, 2, 3);
    }

    // ── WriteArrayAsync (write-through) ────────────────────────────────────

    [Fact]
    public async Task WriteArray_VerbatimPassthrough()
    {
        var pipe = ToPipe("""{"items":[{"a":1},{"a":2}]}""");
        var output = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(output);

        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(pipe, "items", writer);
        writer.WriteEndArray();
        writer.Flush();

        count.Should().Be(2);
        var result = JsonDocument.Parse(output.WrittenMemory);
        result.RootElement.GetArrayLength().Should().Be(2);
        result.RootElement[0].GetProperty("a").GetInt32().Should().Be(1);
        result.RootElement[1].GetProperty("a").GetInt32().Should().Be(2);
    }

    [Fact]
    public async Task WriteArray_WithTransform_SelectiveFields()
    {
        var pipe = ToPipe(
            """{"items":[{"name":"alice","age":30,"internal":"x"},{"name":"bob","age":25,"internal":"y"}]}"""
        );
        var output = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(output);

        writer.WriteStartArray();
        await JsonStreamReader.WriteArrayAsync(
            pipe,
            "items",
            writer,
            (itemBytes, w) =>
            {
                using var doc = JsonDocument.Parse(itemBytes);
                w.WriteStartObject();
                w.WriteString("name"u8, doc.RootElement.GetProperty("name").GetString());
                w.WriteNumber("age"u8, doc.RootElement.GetProperty("age").GetInt32());
                w.WriteEndObject();
            }
        );
        writer.WriteEndArray();
        writer.Flush();

        var result = JsonDocument.Parse(output.WrittenMemory);
        result.RootElement.GetArrayLength().Should().Be(2);

        // "internal" field should NOT be present
        result.RootElement[0].TryGetProperty("internal", out _).Should().BeFalse();
        result.RootElement[0].GetProperty("name").GetString().Should().Be("alice");
    }

    [Fact]
    public async Task WriteArray_WithJsonPath_SelectMany()
    {
        var json = """{"pages":[{"items":[1,2]},{"items":[3]}]}""";
        var pipe = ToPipe(json);
        var path = JsonPath.Root.Property("pages"u8).Each().Property("items"u8);

        var output = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(output);

        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(pipe, path, writer);
        writer.WriteEndArray();
        writer.Flush();

        count.Should().Be(3);
        var result = JsonDocument.Parse(output.WrittenMemory);
        result.RootElement.GetArrayLength().Should().Be(3);
        result.RootElement[0].GetInt32().Should().Be(1);
        result.RootElement[1].GetInt32().Should().Be(2);
        result.RootElement[2].GetInt32().Should().Be(3);
    }

    [Fact]
    public async Task WriteArray_EmptyArray_WritesNothing()
    {
        var pipe = ToPipe("""{"items":[]}""");
        var output = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(output);

        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(pipe, "items", writer);
        writer.WriteEndArray();
        writer.Flush();

        count.Should().Be(0);
        var result = JsonDocument.Parse(output.WrittenMemory);
        result.RootElement.GetArrayLength().Should().Be(0);
    }

    [Fact]
    public async Task WriteArray_LargeItems_StreamsCorrectly()
    {
        var bigValue = new string('x', 10_000);
        var json = $$"""{"items":[{"data":"{{bigValue}}"},{"data":"{{bigValue}}"}]}""";
        var pipe = ToPipe(json, bufferSize: 4096);

        var output = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(output);

        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(pipe, "items", writer);
        writer.WriteEndArray();
        writer.Flush();

        count.Should().Be(2);
        var result = JsonDocument.Parse(output.WrittenMemory);
        result.RootElement[0].GetProperty("data").GetString()!.Length.Should().Be(10_000);
    }
}
