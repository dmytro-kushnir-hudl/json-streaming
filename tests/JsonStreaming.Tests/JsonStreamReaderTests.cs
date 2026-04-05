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
        return PipeReader.Create(new MemoryStream(bytes), new StreamPipeReaderOptions(bufferSize: bufferSize));
    }

    // ── Dot-path navigation (string overload) ──────────────────────────────

    [Fact]
    public async Task EnumerateArray_FlatPath_YieldsItems()
    {
        var json = """{"messages":[{"id":1},{"id":2},{"id":3}]}""";
        var pipe = ToPipe(json);

        var items = new List<int>();
        await foreach (var doc in JsonStreamReader.EnumerateArrayAsync(pipe, "messages"))
        {
            using (doc)
                items.Add(doc.RootElement.GetProperty("id").GetInt32());
        }

        items.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task EnumerateArray_NestedPath_YieldsItems()
    {
        var json = """{"response":{"data":{"items":[10,20,30]}}}""";
        var pipe = ToPipe(json);

        var items = new List<int>();
        await foreach (var doc in JsonStreamReader.EnumerateArrayAsync(pipe, "response.data.items"))
        {
            using (doc)
                items.Add(doc.RootElement.GetInt32());
        }

        items.Should().Equal(10, 20, 30);
    }

    [Fact]
    public async Task EnumerateArray_RootArray_YieldsItems()
    {
        var json = """[1,2,3]""";
        var pipe = ToPipe(json);

        var items = new List<int>();
        await foreach (var doc in JsonStreamReader.EnumerateArrayAsync(pipe, ""))
        {
            using (doc)
                items.Add(doc.RootElement.GetInt32());
        }

        items.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task EnumerateArray_EmptyArray_YieldsNothing()
    {
        var json = """{"messages":[]}""";
        var pipe = ToPipe(json);

        var count = 0;
        await foreach (var doc in JsonStreamReader.EnumerateArrayAsync(pipe, "messages"))
        {
            doc.Dispose();
            count++;
        }

        count.Should().Be(0);
    }

    [Fact]
    public async Task EnumerateArray_MissingPath_YieldsNothing()
    {
        var json = """{"other":[1,2,3]}""";
        var pipe = ToPipe(json);

        var count = 0;
        await foreach (var doc in JsonStreamReader.EnumerateArrayAsync(pipe, "messages"))
        {
            doc.Dispose();
            count++;
        }

        count.Should().Be(0);
    }

    [Fact]
    public async Task EnumerateArray_SkipsUnrelatedProperties()
    {
        var json = """{"meta":{"version":1},"messages":[{"id":1}],"footer":"ok"}""";
        var pipe = ToPipe(json);

        var items = new List<int>();
        await foreach (var doc in JsonStreamReader.EnumerateArrayAsync(pipe, "messages"))
        {
            using (doc)
                items.Add(doc.RootElement.GetProperty("id").GetInt32());
        }

        items.Should().Equal(1);
    }

    // ── ProcessArrayAsync (zero-copy callback) ──────────────────────────────

    [Fact]
    public async Task ProcessArray_CountsItems()
    {
        var json = """{"items":["a","b","c","d"]}""";
        var pipe = ToPipe(json);

        var count = await JsonStreamReader.ProcessArrayAsync(pipe, "items", _ => { });

        count.Should().Be(4);
    }

    [Fact]
    public async Task ProcessArray_CallbackReceivesValidJson()
    {
        var json = """{"items":[{"name":"alice"},{"name":"bob"}]}""";
        var pipe = ToPipe(json);

        var names = new List<string>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            "items",
            bytes =>
            {
                var doc = JsonDocument.Parse(bytes);
                names.Add(doc.RootElement.GetProperty("name").GetString()!);
                doc.Dispose();
            }
        );

        names.Should().Equal("alice", "bob");
    }

    // ── JsonPath overloads ──────────────────────────────────────────────────

    [Fact]
    public async Task ProcessArray_JsonPath_NavigatesCorrectly()
    {
        var json = """{"response":{"data":{"items":[1,2,3]}}}""";
        var pipe = ToPipe(json);
        var path = JsonPath.Root.Property("response"u8).Property("data"u8).Property("items"u8);

        var count = await JsonStreamReader.ProcessArrayAsync(pipe, path, _ => { });

        count.Should().Be(3);
    }

    [Fact]
    public async Task EnumerateArray_JsonPath_NavigatesCorrectly()
    {
        var json = """{"data":{"items":["x","y"]}}""";
        var pipe = ToPipe(json);
        var path = JsonPath.Root.Property("data"u8).Property("items"u8);

        var items = new List<string>();
        await foreach (var doc in JsonStreamReader.EnumerateArrayAsync(pipe, path))
        {
            using (doc)
                items.Add(doc.RootElement.GetString()!);
        }

        items.Should().Equal("x", "y");
    }

    [Fact]
    public async Task ProcessArray_JsonPathRoot_ReadsRootArray()
    {
        var json = """["a","b","c"]""";
        var pipe = ToPipe(json);

        var count = await JsonStreamReader.ProcessArrayAsync(pipe, JsonPath.Root, _ => { });

        count.Should().Be(3);
    }

    // ── Buffer boundary robustness ──────────────────────────────────────────

    [Theory]
    [InlineData(16)]
    [InlineData(37)]
    [InlineData(64)]
    [InlineData(4096)]
    public async Task ProcessArray_SmallBuffers_SameResult(int bufferSize)
    {
        var json = """{"messages":[{"id":1},{"id":2},{"id":3}]}""";
        var pipe = ToPipe(json, bufferSize);

        var count = await JsonStreamReader.ProcessArrayAsync(pipe, "messages", _ => { });

        count.Should().Be(3);
    }

    [Fact]
    public async Task ProcessArray_LargeItemSpanningBuffers_Works()
    {
        var bigValue = new string('x', 50_000);
        var json = $$"""{"items":[{"data":"{{bigValue}}"}]}""";
        var pipe = ToPipe(json, bufferSize: 8192);

        var lengths = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            "items",
            bytes =>
            {
                var doc = JsonDocument.Parse(bytes);
                lengths.Add(doc.RootElement.GetProperty("data").GetString()!.Length);
                doc.Dispose();
            }
        );

        lengths.Should().Equal(50_000);
    }

    [Fact]
    public async Task EnumerateArray_ManyItems_SmallBuffer()
    {
        var items = string.Join(",", Enumerable.Range(0, 500).Select(i => $"\"{i}\""));
        var json = $$$"""{"arr":[{{{items}}}]}""";
        var pipe = ToPipe(json, bufferSize: 32);

        var count = 0;
        await foreach (var doc in JsonStreamReader.EnumerateArrayAsync(pipe, "arr"))
        {
            doc.Dispose();
            count++;
        }

        count.Should().Be(500);
    }

    // ── Mixed element types ──────────────────────────────────────────────────

    [Fact]
    public async Task EnumerateArray_MixedTypes_AllYielded()
    {
        var json = """{"arr":[1,"hello",true,null,{"k":"v"},[1,2]]}""";
        var pipe = ToPipe(json);

        var types = new List<JsonValueKind>();
        await foreach (var doc in JsonStreamReader.EnumerateArrayAsync(pipe, "arr"))
        {
            using (doc)
                types.Add(doc.RootElement.ValueKind);
        }

        types.Should().Equal(
            JsonValueKind.Number,
            JsonValueKind.String,
            JsonValueKind.True,
            JsonValueKind.Null,
            JsonValueKind.Object,
            JsonValueKind.Array
        );
    }

    // ── Cancellation ────────────────────────────────────────────────────────

    [Fact]
    public async Task ProcessArray_CancellationToken_Respected()
    {
        var json = """{"items":[1,2,3,4,5]}""";
        var pipe = ToPipe(json);
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

    // ── Each() / select-many ────────────────────────────────────────────────

    [Fact]
    public async Task ProcessArray_Each_FlattensTwoInnerArrays()
    {
        var json = """
        {
            "responses": [
                {"messages": [{"id":1},{"id":2}]},
                {"messages": [{"id":3}]}
            ]
        }
        """;
        var pipe = ToPipe(json);
        var path = JsonPath.Root.Property("responses"u8).Each().Property("messages"u8);

        var ids = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            path,
            bytes =>
            {
                var doc = JsonDocument.Parse(bytes);
                ids.Add(doc.RootElement.GetProperty("id").GetInt32());
                doc.Dispose();
            }
        );

        ids.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task ProcessArray_Each_NoSuffix_YieldsOuterElements()
    {
        var json = """{"items":[{"a":1},{"a":2},{"a":3}]}""";
        var pipe = ToPipe(json);
        var path = JsonPath.Root.Property("items"u8).Each();

        var values = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            path,
            bytes =>
            {
                var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetProperty("a").GetInt32());
                doc.Dispose();
            }
        );

        values.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task ProcessArray_Each_EmptyInnerArrays_YieldsNothing()
    {
        var json = """{"items":[{"msgs":[]},{"msgs":[]}]}""";
        var pipe = ToPipe(json);
        var path = JsonPath.Root.Property("items"u8).Each().Property("msgs"u8);

        var count = await JsonStreamReader.ProcessArrayAsync(pipe, path, _ => { });

        count.Should().Be(0);
    }

    [Fact]
    public async Task ProcessArray_Each_MixedEmpty_YieldsOnlyNonEmpty()
    {
        var json = """
        {
            "groups": [
                {"items": []},
                {"items": [1,2]},
                {"items": []},
                {"items": [3]}
            ]
        }
        """;
        var pipe = ToPipe(json);
        var path = JsonPath.Root.Property("groups"u8).Each().Property("items"u8);

        var values = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            path,
            bytes =>
            {
                var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetInt32());
                doc.Dispose();
            }
        );

        values.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task ProcessArray_Each_ElementMissingSuffix_Skipped()
    {
        var json = """
        {
            "items": [
                {"data": [1]},
                {"other": "no data here"},
                {"data": [2,3]}
            ]
        }
        """;
        var pipe = ToPipe(json);
        var path = JsonPath.Root.Property("items"u8).Each().Property("data"u8);

        var values = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            path,
            bytes =>
            {
                var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetInt32());
                doc.Dispose();
            }
        );

        values.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task ProcessArray_Each_ExtraPropertiesSkipped()
    {
        var json = """
        {
            "items": [
                {"meta":"x","data":[10],"footer":"y"},
                {"data":[20,30],"extra":{"nested":true}}
            ]
        }
        """;
        var pipe = ToPipe(json);
        var path = JsonPath.Root.Property("items"u8).Each().Property("data"u8);

        var values = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            path,
            bytes =>
            {
                var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetInt32());
                doc.Dispose();
            }
        );

        values.Should().Equal(10, 20, 30);
    }

    [Fact]
    public async Task EnumerateArray_Each_YieldsAcrossInnerArrays()
    {
        var json = """
        {
            "pages": [
                {"results": ["a","b"]},
                {"results": ["c"]}
            ]
        }
        """;
        var pipe = ToPipe(json);
        var path = JsonPath.Root.Property("pages"u8).Each().Property("results"u8);

        var items = new List<string>();
        await foreach (var doc in JsonStreamReader.EnumerateArrayAsync(pipe, path))
        {
            using (doc)
                items.Add(doc.RootElement.GetString()!);
        }

        items.Should().Equal("a", "b", "c");
    }

    [Fact]
    public async Task ProcessArray_Each_ParsedJsonPath()
    {
        var json = """{"groups":[{"items":[1]},{"items":[2,3]}]}""";
        var pipe = ToPipe(json);
        var path = JsonPath.Parse("$.groups[*].items");

        var values = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            path,
            bytes =>
            {
                var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetInt32());
                doc.Dispose();
            }
        );

        values.Should().Equal(1, 2, 3);
    }

    [Theory]
    [InlineData(64)]
    [InlineData(128)]
    [InlineData(8192)]
    public async Task ProcessArray_Each_SmallBuffers_SameResult(int bufferSize)
    {
        var json = """
        {
            "data": [
                {"items": [{"v":1},{"v":2}]},
                {"items": [{"v":3}]},
                {"items": [{"v":4},{"v":5},{"v":6}]}
            ]
        }
        """;
        var pipe = ToPipe(json, bufferSize);
        var path = JsonPath.Root.Property("data"u8).Each().Property("items"u8);

        var values = new List<int>();
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            path,
            bytes =>
            {
                var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetProperty("v").GetInt32());
                doc.Dispose();
            }
        );

        values.Should().Equal(1, 2, 3, 4, 5, 6);
    }

    [Fact]
    public async Task ProcessArray_Each_EmptyOuterArray_ReturnsZero()
    {
        var json = """{"items":[]}""";
        var pipe = ToPipe(json);
        var path = JsonPath.Root.Property("items"u8).Each().Property("data"u8);

        var count = await JsonStreamReader.ProcessArrayAsync(pipe, path, _ => { });

        count.Should().Be(0);
    }

    [Fact]
    public async Task ProcessArray_Each_NestedSuffix()
    {
        var json = """
        {
            "pages": [
                {"response": {"data": {"items": [1,2]}}},
                {"response": {"data": {"items": [3]}}}
            ]
        }
        """;
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
                var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetInt32());
                doc.Dispose();
            }
        );

        values.Should().Equal(1, 2, 3);
    }
}
