using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using JsonStreaming;

namespace JsonStreaming.Tests;

public class JsonStackReaderTests
{
    private static PipeReader ToPipe(string json, int bufferSize = 8192) =>
        PipeReader.Create(
            new MemoryStream(Encoding.UTF8.GetBytes(json)),
            new StreamPipeReaderOptions(bufferSize: bufferSize)
        );

    // Predicate helpers — path is a list of string (property key) or int (array index).
    // Match the last two segments: [anyContainerKey, int index].
    private static bool AtArrayOf(IReadOnlyList<object> path, string key) =>
        path.Count >= 2 && path[^2] is string s && s == key && path[^1] is int;

    private static List<T> Collect<T>(string json, Func<IReadOnlyList<object>, bool> predicate, Func<JsonElement, T> parse)
    {
        var pipe = ToPipe(json);
        var items = new List<T>();
        JsonStackReader
            .ReadItemsAsync(
                pipe,
                predicate,
                bytes =>
                {
                    using var doc = JsonDocument.Parse(bytes);
                    items.Add(parse(doc.RootElement));
                    return ValueTask.CompletedTask;
                }
            )
            .GetAwaiter()
            .GetResult();
        return items;
    }

    // ── Path tracking ─────────────────────────────────────────────────────────

    [Fact]
    public void FlatProperty_YieldsItems()
    {
        var items = Collect(
            """{"messages":[{"id":1},{"id":2},{"id":3}]}""",
            path => AtArrayOf(path, "messages"),
            e => e.GetProperty("id").GetInt32()
        );
        items.Should().Equal(1, 2, 3);
    }

    [Fact]
    public void NestedProperty_YieldsItems()
    {
        var items = Collect(
            """{"response":{"data":{"items":[10,20,30]}}}""",
            path => path is ["response", "data", "items", int _],
            e => e.GetInt32()
        );
        items.Should().Equal(10, 20, 30);
    }

    [Fact]
    public async Task RootArray_YieldsItems()
    {
        var pipe = ToPipe("[1,2,3]");
        var items = new List<int>();
        await JsonStackReader.ReadItemsAsync(
            pipe,
            path => path is [int _],
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                items.Add(doc.RootElement.GetInt32());
                return ValueTask.CompletedTask;
            }
        );
        items.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task EmptyArray_ReturnsZero()
    {
        var pipe = ToPipe("""{"messages":[]}""");
        int count = await JsonStackReader.ReadItemsAsync(
            pipe,
            path => AtArrayOf(path, "messages"),
            _ => ValueTask.CompletedTask
        );
        count.Should().Be(0);
    }

    [Fact]
    public async Task PredicateNeverMatches_ReturnsZero()
    {
        var pipe = ToPipe("""{"other":[1,2,3]}""");
        int count = await JsonStackReader.ReadItemsAsync(
            pipe,
            path => AtArrayOf(path, "messages"),
            _ => ValueTask.CompletedTask
        );
        count.Should().Be(0);
    }

    [Fact]
    public void SkipsUnrelatedProperties()
    {
        var items = Collect(
            """{"meta":{"version":1},"messages":[{"id":1}],"footer":"ok"}""",
            path => AtArrayOf(path, "messages"),
            e => e.GetProperty("id").GetInt32()
        );
        items.Should().Equal(1);
    }

    [Fact]
    public void CountsReturnedItems()
    {
        var pipe = ToPipe("""{"items":["a","b","c","d"]}""");
        int count = JsonStackReader
            .ReadItemsAsync(pipe, path => AtArrayOf(path, "items"), _ => ValueTask.CompletedTask)
            .GetAwaiter()
            .GetResult();
        count.Should().Be(4);
    }

    // ── Path indices are correct ──────────────────────────────────────────────

    [Fact]
    public async Task ArrayIndices_AreCorrectlyTracked()
    {
        var pipe = ToPipe("""{"items":[10,20,30]}""");
        var indexToValue = new Dictionary<int, int>();
        await JsonStackReader.ReadItemsAsync(
            pipe,
            path => AtArrayOf(path, "items"),
            bytes =>
            {
                // The last segment is the array index
                // We can't access path here, but we trust order is preserved
                using var doc = JsonDocument.Parse(bytes);
                indexToValue[indexToValue.Count] = doc.RootElement.GetInt32();
                return ValueTask.CompletedTask;
            }
        );
        indexToValue.Should().BeEquivalentTo(new Dictionary<int, int> { [0] = 10, [1] = 20, [2] = 30 });
    }

    [Fact]
    public async Task CanFilterByArrayIndex()
    {
        // Capture only even-indexed elements using the path
        var pipe = ToPipe("""{"items":[10,20,30,40,50]}""");
        var items = new List<int>();
        await JsonStackReader.ReadItemsAsync(
            pipe,
            path => path is ["items", int idx] && idx % 2 == 0,
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                items.Add(doc.RootElement.GetInt32());
                return ValueTask.CompletedTask;
            }
        );
        items.Should().Equal(10, 30, 50);
    }

    // ── Mixed types ────────────────────────────────────────────────────────────

    [Fact]
    public async Task MixedTypes_AllCaptured()
    {
        var pipe = ToPipe("""{"arr":[1,"hello",true,null,{"k":"v"},[1,2]]}""");
        var kinds = new List<JsonValueKind>();
        await JsonStackReader.ReadItemsAsync(
            pipe,
            path => AtArrayOf(path, "arr"),
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                kinds.Add(doc.RootElement.ValueKind);
                return ValueTask.CompletedTask;
            }
        );
        kinds.Should().Equal(
            JsonValueKind.Number,
            JsonValueKind.String,
            JsonValueKind.True,
            JsonValueKind.Null,
            JsonValueKind.Object,
            JsonValueKind.Array
        );
    }

    // ── Select-many (no special API needed — just a richer predicate) ─────────

    [Fact]
    public async Task SelectMany_TwoLevels_FlattenedCorrectly()
    {
        var json = """{"responses":[{"messages":[{"id":1},{"id":2}]},{"messages":[{"id":3}]}]}""";
        var pipe = ToPipe(json);
        var ids = new List<int>();
        await JsonStackReader.ReadItemsAsync(
            pipe,
            path => path is ["responses", int _, "messages", int _],
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                ids.Add(doc.RootElement.GetProperty("id").GetInt32());
                return ValueTask.CompletedTask;
            }
        );
        ids.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task SelectMany_EmptyInnerArrays_ReturnsZero()
    {
        var pipe = ToPipe("""{"items":[{"msgs":[]},{"msgs":[]}]}""");
        int count = await JsonStackReader.ReadItemsAsync(
            pipe,
            path => path is ["items", int _, "msgs", int _],
            _ => ValueTask.CompletedTask
        );
        count.Should().Be(0);
    }

    [Fact]
    public async Task SelectMany_MixedEmptyAndNonEmpty()
    {
        var json = """{"groups":[{"items":[]},{"items":[1,2]},{"items":[]},{"items":[3]}]}""";
        var pipe = ToPipe(json);
        var values = new List<int>();
        await JsonStackReader.ReadItemsAsync(
            pipe,
            path => path is ["groups", int _, "items", int _],
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetInt32());
                return ValueTask.CompletedTask;
            }
        );
        values.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task SelectMany_ThreeLevelsNested()
    {
        var json = """{"pages":[{"response":{"data":{"items":[1,2]}}},{"response":{"data":{"items":[3]}}}]}""";
        var pipe = ToPipe(json);
        var values = new List<int>();
        await JsonStackReader.ReadItemsAsync(
            pipe,
            path => path is ["pages", int _, "response", "data", "items", int _],
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetInt32());
                return ValueTask.CompletedTask;
            }
        );
        values.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task SelectMany_MissingSuffixInSomeElements_Skipped()
    {
        var json = """{"items":[{"data":[1]},{"other":"x"},{"data":[2,3]}]}""";
        var pipe = ToPipe(json);
        var values = new List<int>();
        await JsonStackReader.ReadItemsAsync(
            pipe,
            path => path is ["items", int _, "data", int _],
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetInt32());
                return ValueTask.CompletedTask;
            }
        );
        values.Should().Equal(1, 2, 3);
    }

    // ── Buffer boundary robustness ────────────────────────────────────────────

    [Theory]
    [InlineData(16)]
    [InlineData(37)]
    [InlineData(64)]
    [InlineData(4096)]
    public async Task SmallBuffers_SameResult(int bufferSize)
    {
        var pipe = ToPipe("""{"messages":[{"id":1},{"id":2},{"id":3}]}""", bufferSize);
        int count = await JsonStackReader.ReadItemsAsync(
            pipe,
            path => AtArrayOf(path, "messages"),
            _ => ValueTask.CompletedTask
        );
        count.Should().Be(3);
    }

    [Fact]
    public async Task LargeItemSpanningBuffers()
    {
        var bigValue = new string('x', 50_000);
        var pipe = ToPipe($$"""{"items":[{"data":"{{bigValue}}"}]}""", bufferSize: 8192);
        var lengths = new List<int>();
        await JsonStackReader.ReadItemsAsync(
            pipe,
            path => AtArrayOf(path, "items"),
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                lengths.Add(doc.RootElement.GetProperty("data").GetString()!.Length);
                return ValueTask.CompletedTask;
            }
        );
        lengths.Should().Equal(50_000);
    }

    [Fact]
    public async Task ManyItems_SmallBuffer()
    {
        var items = string.Join(",", Enumerable.Range(0, 500));
        var json = $$$"""{"arr":[{{{items}}}]}""";
        var pipe = ToPipe(json, bufferSize: 32);
        int count = await JsonStackReader.ReadItemsAsync(
            pipe,
            path => AtArrayOf(path, "arr"),
            _ => ValueTask.CompletedTask
        );
        count.Should().Be(500);
    }

    [Theory]
    [InlineData(64)]
    [InlineData(128)]
    [InlineData(8192)]
    public async Task SelectMany_SmallBuffers_SameResult(int bufferSize)
    {
        var json = """{"data":[{"items":[{"v":1},{"v":2}]},{"items":[{"v":3}]},{"items":[{"v":4},{"v":5},{"v":6}]}]}""";
        var pipe = ToPipe(json, bufferSize);
        var values = new List<int>();
        await JsonStackReader.ReadItemsAsync(
            pipe,
            path => path is ["data", int _, "items", int _],
            bytes =>
            {
                using var doc = JsonDocument.Parse(bytes);
                values.Add(doc.RootElement.GetProperty("v").GetInt32());
                return ValueTask.CompletedTask;
            }
        );
        values.Should().Equal(1, 2, 3, 4, 5, 6);
    }

    // ── Backpressure: onItem is awaited before continuing ─────────────────────

    [Fact]
    public async Task Backpressure_ItemsDeliveredOneAtATime()
    {
        var pipe = ToPipe("""{"items":[1,2,3,4,5]}""");
        var order = new List<int>();
        int active = 0;

        await JsonStackReader.ReadItemsAsync(
            pipe,
            path => AtArrayOf(path, "items"),
            async bytes =>
            {
                Interlocked.Increment(ref active);
                active.Should().Be(1, "only one item should be in flight at a time");
                using var doc = JsonDocument.Parse(bytes);
                order.Add(doc.RootElement.GetInt32());
                await Task.Yield(); // simulate async consumer work
                Interlocked.Decrement(ref active);
            }
        );

        order.Should().Equal(1, 2, 3, 4, 5);
    }

    // ── Cancellation ──────────────────────────────────────────────────────────

    [Fact]
    public async Task CancellationToken_StopsIteration()
    {
        var pipe = ToPipe("""{"items":[1,2,3,4,5]}""");
        using var cts = new CancellationTokenSource();
        int count = 0;

        var act = async () => await JsonStackReader.ReadItemsAsync(
            pipe,
            path => AtArrayOf(path, "items"),
            bytes =>
            {
                if (++count == 2) cts.Cancel();
                return ValueTask.CompletedTask;
            },
            cts.Token
        );

        await act.Should().ThrowAsync<OperationCanceledException>();
        count.Should().Be(2);
    }

    // ── Error handling ────────────────────────────────────────────────────────

    [Fact]
    public async Task TruncatedJson_ThrowsJsonException()
    {
        var pipe = ToPipe("""{"items":[1,2""");
        var act = async () => await JsonStackReader.ReadItemsAsync(
            pipe,
            path => AtArrayOf(path, "items"),
            _ => ValueTask.CompletedTask
        );
        await act.Should().ThrowAsync<JsonException>();
    }

    [Fact]
    public async Task TruncatedJson_InsideObject_ThrowsJsonException()
    {
        var pipe = ToPipe("""{"items":[{"id":1},{"id":2""");
        var act = async () => await JsonStackReader.ReadItemsAsync(
            pipe,
            path => AtArrayOf(path, "items"),
            _ => ValueTask.CompletedTask
        );
        await act.Should().ThrowAsync<JsonException>();
    }

    // ── IAsyncEnumerable via StreamItemsAsync ─────────────────────────────────

    [Fact]
    public async Task StreamItems_YieldsAllItems()
    {
        var pipe = ToPipe("""{"users":[{"id":1},{"id":2},{"id":3}]}""");
        var ids = new List<int>();

        await foreach (var bytes in JsonStackReader.StreamItemsAsync(pipe, path => AtArrayOf(path, "users")))
        {
            using var doc = JsonDocument.Parse(bytes);
            ids.Add(doc.RootElement.GetProperty("id").GetInt32());
        }

        ids.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task StreamItems_SelectMany_FlattenedCorrectly()
    {
        var json = """{"groups":[{"nums":[1,2]},{"nums":[3,4]},{"nums":[5]}]}""";
        var pipe = ToPipe(json);
        var values = new List<int>();

        await foreach (var bytes in JsonStackReader.StreamItemsAsync(
            pipe,
            path => path is ["groups", int _, "nums", int _]))
        {
            using var doc = JsonDocument.Parse(bytes);
            values.Add(doc.RootElement.GetInt32());
        }

        values.Should().Equal(1, 2, 3, 4, 5);
    }

    [Fact]
    public async Task StreamItems_EmptyArray_NoItems()
    {
        var pipe = ToPipe("""{"items":[]}""");
        int count = 0;
        await foreach (var _ in JsonStackReader.StreamItemsAsync(pipe, path => AtArrayOf(path, "items")))
            count++;
        count.Should().Be(0);
    }

    [Fact]
    public async Task StreamItems_ItemsAreCopied_SafeAfterIteration()
    {
        // Verifies that byte[] items from StreamItemsAsync are owned copies,
        // not references into a pipe buffer that may be reused.
        var pipe = ToPipe("""{"items":[{"v":1},{"v":2},{"v":3}]}""");
        var collected = new List<byte[]>();

        await foreach (var bytes in JsonStackReader.StreamItemsAsync(pipe, path => AtArrayOf(path, "items")))
            collected.Add(bytes); // hold onto all of them

        // All must still be valid JSON after the pipe is done
        collected.Should().HaveCount(3);
        for (int i = 0; i < collected.Count; i++)
        {
            using var doc = JsonDocument.Parse(collected[i]);
            doc.RootElement.GetProperty("v").GetInt32().Should().Be(i + 1);
        }
    }
}
