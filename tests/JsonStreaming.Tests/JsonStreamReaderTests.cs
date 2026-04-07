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
        var path = NdJsonPath.At("response").Key("data").Key("items");
        var count = await JsonStreamReader.ProcessArrayAsync(pipe, path, _ => { });
        count.Should().Be(3);
    }

    [Fact]
    public async Task ProcessArray_JsonPathRoot_ReadsRootArray()
    {
        var pipe = ToPipe("""["a","b","c"]""");
        var count = await JsonStreamReader.ProcessArrayAsync(pipe, NdJsonPath.Root, _ => { });
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

    [Fact]
    public async Task ProcessArray_TruncatedJson_ThrowsJsonException()
    {
        var pipe = ToPipe("""{"items":[1,2""");

        var act = async () => await JsonStreamReader.ProcessArrayAsync(pipe, "items", _ => { });

        await act.Should().ThrowAsync<JsonException>();
    }

    [Fact]
    public async Task WriteArray_TruncatedJson_ThrowsJsonException()
    {
        var pipe = ToPipe("""{"items":[{"id":1},{"id":2}""");
        var output = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(output);

        writer.WriteStartArray();
        var act = async () => await JsonStreamReader.WriteArrayAsync(pipe, "items", writer);

        await act.Should().ThrowAsync<JsonException>();
    }

    // ── Each() / select-many ───────────────────────────────────────────────

    [Fact]
    public async Task Each_FlattensTwoInnerArrays()
    {
        var json = """{"responses":[{"messages":[{"id":1},{"id":2}]},{"messages":[{"id":3}]}]}""";
        var pipe = ToPipe(json);
        var path = NdJsonPath.At("responses").Each().Key("messages");

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
        var path = NdJsonPath.At("items").Each();

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
        var path = NdJsonPath.At("items").Each().Key("msgs");
        var count = await JsonStreamReader.ProcessArrayAsync(pipe, path, _ => { });
        count.Should().Be(0);
    }

    [Fact]
    public async Task Each_MixedEmpty_YieldsOnlyNonEmpty()
    {
        var json = """{"groups":[{"items":[]},{"items":[1,2]},{"items":[]},{"items":[3]}]}""";
        var pipe = ToPipe(json);
        var path = NdJsonPath.At("groups").Each().Key("items");

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
        var path = NdJsonPath.At("items").Each().Key("data");

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
        var path = NdJsonPath.At("items").Each().Key("data");

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
        var path = NdJsonPath.Parse("$.groups[*].items");

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
        var path = NdJsonPath.At("data").Each().Key("items");

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
        var path = NdJsonPath.At("items").Each().Key("data");
        var count = await JsonStreamReader.ProcessArrayAsync(pipe, path, _ => { });
        count.Should().Be(0);
    }

    [Fact]
    public async Task Each_NestedSuffix()
    {
        var json =
            """{"pages":[{"response":{"data":{"items":[1,2]}}},{"response":{"data":{"items":[3]}}}]}""";
        var pipe = ToPipe(json);
        var path = NdJsonPath.At("pages")
            .Each()
            .Key("response")
            .Key("data")
            .Key("items");

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
        await using var writer = new Utf8JsonWriter(output);

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
        await using var writer = new Utf8JsonWriter(output);

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
        var path = NdJsonPath.At("pages").Each().Key("items");

        var output = new ArrayBufferWriter<byte>();
        await using var writer = new Utf8JsonWriter(output);

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
        await using var writer = new Utf8JsonWriter(output);

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
        await using var writer = new Utf8JsonWriter(output);

        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(pipe, "items", writer);
        writer.WriteEndArray();
        writer.Flush();

        count.Should().Be(2);
        var result = JsonDocument.Parse(output.WrittenMemory);
        result.RootElement[0].GetProperty("data").GetString()!.Length.Should().Be(10_000);
    }

    // ── Flush behavior ─────────────────────────────────────────────────────

    [Fact]
    public async Task WriteArray_FlushesAtThreshold()
    {
        // 50 items × ~50 bytes each = ~2500 bytes total
        // With a 500-byte threshold (90% = 450), should flush multiple times
        var items = string.Join(",", Enumerable.Range(0, 50).Select(i => $$"""{"id":{{i}},"val":"data-{{i}}"}"""));
        var json = $$$"""{"items":[{{{items}}}]}""";
        var pipe = ToPipe(json);

        var flushCounter = new FlushCountingBufferWriter();
        await using var writer = new Utf8JsonWriter(flushCounter);
        var options = new WriteOptions { FlushThreshold = 500 };

        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(pipe, "items", writer, options);
        writer.WriteEndArray();
        writer.Flush(); // final flush

        count.Should().Be(50);
        // With ~2500 bytes total and 450-byte effective threshold,
        // we expect multiple mid-stream flushes
        flushCounter.FlushCount.Should().BeGreaterThanOrEqualTo(3);
    }

    [Fact]
    public async Task WriteArray_NoFlushWhenDisabled()
    {
        var items = string.Join(",", Enumerable.Range(0, 50).Select(i => $$"""{"id":{{i}}}"""));
        var json = $$$"""{"items":[{{{items}}}]}""";
        var pipe = ToPipe(json);

        var flushCounter = new FlushCountingBufferWriter();
        await using var writer = new Utf8JsonWriter(flushCounter);
        var options = new WriteOptions { FlushThreshold = 0 }; // disabled

        writer.WriteStartArray();
        await JsonStreamReader.WriteArrayAsync(pipe, "items", writer, options);
        writer.WriteEndArray();
        writer.Flush(); // only this one

        // Only the explicit final Flush() should have triggered
        flushCounter.FlushCount.Should().Be(1);
    }

    [Fact]
    public async Task WriteArray_SelectMany_FlushesAtThreshold()
    {
        // 3 groups × 10 items each = 30 items, ~1500 bytes
        var groups = string.Join(
            ",",
            Enumerable.Range(0, 3).Select(g =>
            {
                var innerItems = string.Join(",", Enumerable.Range(0, 10).Select(i => $$"""{"g":{{g}},"i":{{i}}}"""));
                return $$"""{"items":[{{innerItems}}]}""";
            })
        );
        var json = $$$"""{"data":[{{{groups}}}]}""";
        var pipe = ToPipe(json);
        var path = NdJsonPath.At("data").Each().Key("items");

        var flushCounter = new FlushCountingBufferWriter();
        await using var writer = new Utf8JsonWriter(flushCounter);
        var options = new WriteOptions { FlushThreshold = 300 };

        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(pipe, path, writer, options);
        writer.WriteEndArray();
        writer.Flush();

        count.Should().Be(30);
        flushCounter.FlushCount.Should().BeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task WriteArray_AsyncFlush_IsInvoked()
    {
        // 20 items, small threshold — should trigger multiple async flushes
        var items = string.Join(
            ",",
            Enumerable.Range(0, 20).Select(i => $$"""{"id":{{i}},"data":"padding-value-here"}""")
        );
        var json = $$$"""{"items":[{{{items}}}]}""";
        var pipe = ToPipe(json);

        var output = new ArrayBufferWriter<byte>();
        await using var writer = new Utf8JsonWriter(output);

        int asyncFlushCount = 0;
        var options = new WriteOptions
        {
            FlushThreshold = 200,
            AsyncFlush = _ =>
            {
                asyncFlushCount++;
                return ValueTask.CompletedTask;
            },
        };

        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(pipe, "items", writer, options);
        writer.WriteEndArray();
        writer.Flush();

        count.Should().Be(20);
        asyncFlushCount.Should().BeGreaterThanOrEqualTo(2, "async flush should fire multiple times");

        // Verify output is still valid JSON
        var result = JsonDocument.Parse(output.WrittenMemory);
        result.RootElement.GetArrayLength().Should().Be(20);
    }

    [Fact]
    public async Task WriteArray_AsyncFlush_SelectMany_IsInvoked()
    {
        // Each item ~40 bytes, 15 items total ~600 bytes, threshold 100 → multiple flushes
        var makeItems = (int start, int count) =>
            string.Join(
                ",",
                Enumerable
                    .Range(start, count)
                    .Select(i => $$"""{"id":{{i}},"val":"item-{{i}}-padding"}""")
            );
        var json =
            $$"""{"data":[{"items":[{{makeItems(1, 5)}}]},{"items":[{{makeItems(6, 5)}}]},{"items":[{{makeItems(11, 5)}}]}]}""";
        var pipe = ToPipe(json);
        var path = NdJsonPath.At("data").Each().Key("items");

        var output = new ArrayBufferWriter<byte>();
        await using var writer = new Utf8JsonWriter(output);

        int asyncFlushCount = 0;
        var options = new WriteOptions
        {
            FlushThreshold = 100,
            AsyncFlush = _ =>
            {
                asyncFlushCount++;
                return ValueTask.CompletedTask;
            },
        };

        writer.WriteStartArray();
        var count = await JsonStreamReader.WriteArrayAsync(pipe, path, writer, options);
        writer.WriteEndArray();
        writer.Flush();

        count.Should().Be(15);
        asyncFlushCount.Should().BeGreaterThanOrEqualTo(2);

        var result = JsonDocument.Parse(output.WrittenMemory);
        result.RootElement.GetArrayLength().Should().Be(15);
    }

    /// <summary>
    /// IBufferWriter that counts how many times Utf8JsonWriter.Flush() commits bytes.
    /// Each Flush() call triggers Advance() with the pending bytes — we count those.
    /// </summary>
    private sealed class FlushCountingBufferWriter : IBufferWriter<byte>
    {
        private byte[] _buffer = new byte[65_536];
        private int _written;

        public int FlushCount { get; private set; }

        public void Advance(int count)
        {
            // Utf8JsonWriter calls Advance after each Flush with the pending byte count.
            // A Flush with BytesPending > 0 calls Advance once.
            if (count > 0)
                FlushCount++;
            _written += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            if (_written + sizeHint > _buffer.Length)
                System.Array.Resize(ref _buffer, Math.Max(_buffer.Length * 2, _written + sizeHint));
            return _buffer.AsMemory(_written);
        }

        public Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;
    }
}
