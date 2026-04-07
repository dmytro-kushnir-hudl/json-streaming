# Consolidation: ProjectItemsAsync + Delete Old APIs

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `ProjectItemsAsync` to the transcoder (raw-bytes callback per matched item), prove all typed patterns as client extension methods in the sample app, then delete `JsonStreamReader`, `JsonStreamReaderTyped`, `JsonStreamPipeline`, `JsonPath`, and `JsonPathNavigator`.

**Architecture:** `ProjectItemsAsync` reuses the transcoder's existing FSM (`ProjectionState.Advance()`) with a new per-chunk method that tracks item byte ranges instead of writing tokens. For items within one read chunk, it slices directly from the buffer (zero-copy). For items spanning chunks, it accumulates into a rented `ArrayPool<byte>` buffer. The async method breaks out of the sync token loop after each complete item to await `processItem`.

**Tech Stack:** C# / .NET 10, System.Text.Json, System.IO.Pipelines

**Spec:** `docs/superpowers/specs/2026-04-07-consolidation-design.md`

**Baseline:** 91 tests passing.

---

## File Map

- **Modify:** `src/JsonStreaming/JsonTranscoder.cs` — add `ProjectItemsAsync` public method + `ScanProjection` private per-chunk method
- **Delete:** `src/JsonStreaming/JsonPath.cs`
- **Delete:** `src/JsonStreaming/JsonPathNavigator.cs`
- **Delete:** `src/JsonStreaming/JsonStreamReader.cs`
- **Delete:** `src/JsonStreaming/JsonStreamReaderTyped.cs`
- **Delete:** `src/JsonStreaming/JsonStreamPipeline.cs`
- **Rewrite:** `tests/JsonStreaming.Tests/JsonStreamReaderTests.cs` → `tests/JsonStreaming.Tests/ProjectItemsTests.cs`
- **Rewrite:** `tests/JsonStreaming.Tests/TypedApiTests.cs` → tests in sample app or inline
- **Rewrite:** `tests/JsonStreaming.Tests/JsonStreamPipelineTests.cs` → tests using client extension
- **Modify:** `samples/WebApiSample/Program.cs` — add client extension methods, update endpoints

---

## Task 1: Implement ProjectItemsAsync with Core Tests

**Files:**
- Modify: `src/JsonStreaming/JsonTranscoder.cs`
- Create: `tests/JsonStreaming.Tests/ProjectItemsTests.cs`

- [ ] **Step 1: Write the core test — primitive extraction callback**

Create `tests/JsonStreaming.Tests/ProjectItemsTests.cs`:

```csharp
using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using FluentAssertions;
using JsonStreaming;

namespace JsonStreaming.Tests;

public class ProjectItemsTests
{
    private static PipeReader ToPipe(string json, int bufferSize = 64) =>
        PipeReader.Create(
            new MemoryStream(Encoding.UTF8.GetBytes(json)),
            new StreamPipeReaderOptions(bufferSize: bufferSize)
        );

    // language=JSON
    private const string OrderJson = """
        { "name": "Alice", "price": 199.95,
          "shipTo": { "city": "Pretendville", "zip": "98999" } }
        """;

    // language=JSON
    private const string PeopleJson = """
        [
          { "name": "Adeel Solangi",  "language": "Sindhi", "version": 6.1  },
          { "name": "Afzal Ghaffar",  "language": "Sindhi", "version": 1.88 },
          { "name": "Aamir Solangi",  "language": "Sindhi", "version": 7.27 }
        ]
        """;

    [Fact]
    public async Task ProjectItems_PrimitiveValue_CallbackReceivesBytes()
    {
        var items = new List<string>();
        var pipe = ToPipe(OrderJson);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            NdJsonPath.At("price"),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

        items.Should().Equal("199.95");
    }

    [Fact]
    public async Task ProjectItems_ObjectValue_CallbackReceivesCompleteJson()
    {
        var items = new List<string>();
        var pipe = ToPipe(OrderJson);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            NdJsonPath.At("shipTo"),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

        items.Should().HaveCount(1);
        var doc = System.Text.Json.JsonDocument.Parse(items[0]);
        doc.RootElement.GetProperty("city").GetString().Should().Be("Pretendville");
    }

    [Fact]
    public async Task ProjectItems_ArrayElements_CallbackPerElement()
    {
        var items = new List<string>();
        var pipe = ToPipe(PeopleJson);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            NdJsonPath.Each(),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

        items.Should().HaveCount(3);
        System.Text.Json.JsonDocument.Parse(items[0]).RootElement
            .GetProperty("name").GetString().Should().Be("Adeel Solangi");
    }

    [Fact]
    public async Task ProjectItems_NestedProperty_ExtractsFromEachElement()
    {
        var items = new List<string>();
        var pipe = ToPipe(PeopleJson);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            NdJsonPath.Each().Key("name"),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

        items.Should().Equal("\"Adeel Solangi\"", "\"Afzal Ghaffar\"", "\"Aamir Solangi\"");
    }

    [Fact]
    public async Task ProjectItems_NoMatch_CallbackNeverCalled()
    {
        var items = new List<string>();
        var pipe = ToPipe(OrderJson);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            NdJsonPath.At("nonexistent"),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

        items.Should().BeEmpty();
    }

    [Theory]
    [InlineData(16)]
    [InlineData(37)]
    [InlineData(64)]
    [InlineData(4096)]
    public async Task ProjectItems_SmallBuffers_SameResults(int bufferSize)
    {
        var items = new List<string>();
        var pipe = ToPipe(PeopleJson, bufferSize);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            NdJsonPath.Each(),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

        items.Should().HaveCount(3);
        foreach (var item in items)
            System.Text.Json.JsonDocument.Parse(item); // all valid JSON
    }

    [Fact]
    public async Task ProjectItems_LargeItemSpanningBuffers()
    {
        var json = $$"""
            { "items": [
                { "id": 1, "payload": "{{new string('x', 20_000)}}" },
                { "id": 2, "payload": "{{new string('y', 20_000)}}" }
            ] }
            """;

        var items = new List<string>();
        var pipe = ToPipe(json, bufferSize: 64);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            NdJsonPath.At("items").Each(),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

        items.Should().HaveCount(2);
        System.Text.Json.JsonDocument.Parse(items[0]).RootElement
            .GetProperty("id").GetInt32().Should().Be(1);
        System.Text.Json.JsonDocument.Parse(items[1]).RootElement
            .GetProperty("id").GetInt32().Should().Be(2);
    }

    [Fact]
    public async Task ProjectItems_WritesToOutputPipeWriter()
    {
        var pipe = ToPipe(PeopleJson);
        await using var outputStream = new MemoryStream();
        var output = PipeWriter.Create(outputStream);

        await pipe.ProjectItemsAsync(
            NdJsonPath.Each().Key("name"),
            output,
            (itemBytes, writer) =>
            {
                writer.Write(itemBytes);
                writer.Write("\n"u8);
                return ValueTask.CompletedTask;
            });

        await output.FlushAsync();
        var result = Encoding.UTF8.GetString(outputStream.ToArray());
        var lines = result.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        lines.Should().Equal("\"Adeel Solangi\"", "\"Afzal Ghaffar\"", "\"Aamir Solangi\"");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test --filter "FullyQualifiedName~ProjectItemsTests"`
Expected: FAIL — `ProjectItemsAsync` does not exist.

- [ ] **Step 3: Implement ProjectItemsAsync**

Add to `JsonTranscoder` after `ProjectNdJsonVerbatimAsync`. The method uses the FSM but instead of rendering tokens, it tracks item byte ranges and calls the user callback:

```csharp
/// <summary>
/// Reads JSON from <paramref name="reader"/>, navigates to each value matching
/// <paramref name="path"/>, and invokes <paramref name="processItem"/> with the
/// raw bytes of each matched value. The callback receives the item bytes as a
/// <see cref="ReadOnlySequence{T}"/> (valid only during the call) and the
/// <paramref name="writer"/> for output.
/// </summary>
public static async Task ProjectItemsAsync(
    this PipeReader reader,
    NdJsonPath path,
    PipeWriter writer,
    Func<ReadOnlySequence<byte>, PipeWriter, ValueTask> processItem,
    JsonReaderOptions readerOptions = default,
    CancellationToken ct = default)
{
    var state = new ProjectionState { ReaderState = new JsonReaderState(readerOptions) };
    ct.ThrowIfCancellationRequested();

    byte[]? accumulator = null;
    int accumulatedLength = 0;
    long captureStartIndex = -1;

    while (true)
    {
        var result = await reader.ReadAsync(ct);
        var buffer = result.Buffer;

        if (result.IsCanceled)
            throw new OperationCanceledException(ct);

        var jsonReader = new Utf8JsonReader(
            buffer, result.IsCompleted, state.ReaderState);

        long consumedUpTo = 0;
        bool itemFound = false;
        ReadOnlySequence<byte> itemSlice = default;

        bool hasToken = jsonReader.Read();
        while (hasToken)
        {
            byte[]? rentedName = null;
            ParserDirective directive;
            try
            {
                ReadOnlySpan<byte> name = jsonReader.TokenType == JsonTokenType.PropertyName
                        ? GetPropertyName(ref jsonReader, ref rentedName)
                        : default;
                directive = state.Advance(jsonReader.TokenType, path.Segments, name);
            }
            finally
            {
                if (rentedName != null)
                    ArrayPool<byte>.Shared.Return(rentedName);
            }

            switch (directive)
            {
                case ParserDirective.Skip:
                    break;

                case ParserDirective.YieldValue:
                {
                    // Primitive — single token, always fits in buffer
                    long start = jsonReader.TokenStartIndex;
                    long length = jsonReader.BytesConsumed - start;
                    itemSlice = buffer.Slice(buffer.GetPosition(start), length);
                    consumedUpTo = jsonReader.BytesConsumed;
                    state.ReaderState = jsonReader.CurrentState;
                    itemFound = true;
                    goto exitSyncLoop;
                }

                case ParserDirective.BeginCapture:
                    captureStartIndex = jsonReader.TokenStartIndex;
                    continue; // don't advance reader

                case ParserDirective.Capture:
                    break;

                case ParserDirective.EndCapture:
                {
                    long endPos = jsonReader.BytesConsumed;

                    if (accumulator != null && accumulatedLength > 0)
                    {
                        // Multi-chunk item — append final portion
                        int finalLen = (int)(endPos - captureStartIndex);
                        EnsureAccumulator(ref accumulator, accumulatedLength + finalLen);
                        buffer.Slice(buffer.GetPosition(captureStartIndex), finalLen)
                              .CopyTo(accumulator.AsSpan(accumulatedLength));
                        accumulatedLength += finalLen;

                        itemSlice = new ReadOnlySequence<byte>(
                            accumulator, 0, accumulatedLength);
                    }
                    else
                    {
                        // Single-chunk item — zero-copy slice
                        itemSlice = buffer.Slice(
                            buffer.GetPosition(captureStartIndex),
                            endPos - captureStartIndex);
                    }

                    captureStartIndex = -1;
                    consumedUpTo = endPos;
                    state.ReaderState = jsonReader.CurrentState;
                    itemFound = true;
                    goto exitSyncLoop;
                }
            }

            hasToken = jsonReader.Read();
        }

        // End of tokens in this chunk
        if (captureStartIndex >= 0)
        {
            // Capture in progress — accumulate bytes and advance
            int captureLen = (int)(jsonReader.BytesConsumed - captureStartIndex);
            if (captureLen > 0)
            {
                EnsureAccumulator(ref accumulator, accumulatedLength + captureLen);
                buffer.Slice(buffer.GetPosition(captureStartIndex), captureLen)
                      .CopyTo(accumulator.AsSpan(accumulatedLength));
                accumulatedLength += captureLen;
            }
            captureStartIndex = 0; // next chunk: capture continues from start
        }

        state.ReaderState = jsonReader.CurrentState;
        consumedUpTo = jsonReader.BytesConsumed;
        reader.AdvanceTo(buffer.GetPosition(consumedUpTo), buffer.End);

        if (result.IsCompleted)
            break;
        continue;

    exitSyncLoop:
        reader.AdvanceTo(buffer.GetPosition(consumedUpTo), buffer.End);

        await processItem(itemSlice, writer);

        // Clean up accumulator after use
        if (accumulatedLength > 0)
            accumulatedLength = 0;

        if (writer is { CanGetUnflushedBytes: true, UnflushedBytes: >= FlushThreshold })
            await writer.FlushAsync(ct);

        if (result.IsCompleted)
            break;
    }

    if (accumulator != null)
        ArrayPool<byte>.Shared.Return(accumulator);
}

private static void EnsureAccumulator(ref byte[]? buffer, int needed)
{
    if (buffer == null || buffer.Length < needed)
    {
        var old = buffer;
        buffer = ArrayPool<byte>.Shared.Rent(Math.Max(needed, 4096));
        if (old != null)
        {
            old.AsSpan().CopyTo(buffer);
            ArrayPool<byte>.Shared.Return(old);
        }
    }
}
```

- [ ] **Step 4: Run tests**

Run: `dotnet test --filter "FullyQualifiedName~ProjectItemsTests"`
Expected: All 8 tests pass.

- [ ] **Step 5: Run full suite to verify no regressions**

Run: `dotnet test`
Expected: All 91 existing tests pass + 8 new = 99.

- [ ] **Step 6: Commit**

```bash
git add src/JsonStreaming/JsonTranscoder.cs tests/JsonStreaming.Tests/ProjectItemsTests.cs
git commit -m "feat: add ProjectItemsAsync — raw-bytes callback per matched item"
```

---

## Task 2: Add Client Extension Methods to Sample App

**Files:**
- Modify: `samples/WebApiSample/Program.cs`

- [ ] **Step 1: Add StreamingExtensions class**

Add at the end of `Program.cs` (or in a new file `samples/WebApiSample/StreamingExtensions.cs`):

```csharp
using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using JsonStreaming;

/// <summary>
/// Client-side extension methods proving the transcoder is sufficient for typed streaming.
/// These are NOT part of the library — they live in application code.
/// </summary>
static class StreamingExtensions
{
    /// <summary>
    /// Typed transform with filter + explode (selectMany).
    /// Transform returns: [mapped] = project, [] = filter, [a,b,c] = explode.
    /// </summary>
    public static async Task<int> ProjectTypedAsync<TIn, TOut>(
        this PipeReader reader,
        NdJsonPath path,
        Utf8JsonWriter writer,
        JsonTypeInfo<TIn> inputType,
        JsonTypeInfo<TOut> outputType,
        Func<TIn, IEnumerable<TOut>> transform,
        CancellationToken ct = default)
    {
        int count = 0;

        await reader.ProjectItemsAsync(
            path,
            PipeWriter.Create(Stream.Null),
            (itemBytes, _) =>
            {
                var input = JsonSerializer.Deserialize(itemBytes, inputType);
                if (input is null) return ValueTask.CompletedTask;

                foreach (var result in transform(input))
                {
                    JsonSerializer.Serialize(writer, result, outputType);
                    count++;
                }
                return ValueTask.CompletedTask;
            },
            ct: ct);

        return count;
    }

    /// <summary>
    /// Raw byte callback — replaces ProcessArrayAsync.
    /// </summary>
    public static async Task<int> ForEachItemAsync(
        this PipeReader reader,
        NdJsonPath path,
        Action<ReadOnlySequence<byte>> processItem,
        CancellationToken ct = default)
    {
        int count = 0;
        await reader.ProjectItemsAsync(
            path,
            PipeWriter.Create(Stream.Null),
            (itemBytes, _) =>
            {
                processItem(itemBytes);
                count++;
                return ValueTask.CompletedTask;
            },
            ct: ct);
        return count;
    }
}
```

- [ ] **Step 2: Update sample endpoints to use new extension methods**

Replace `JsonStreamReader.ProcessArrayAsync` calls with `ForEachItemAsync`, replace `JsonStreamPipeline.TransformArrayAsync` calls with `ProjectTypedAsync` + manual envelope writing. Replace `JsonStreamReader.WriteArrayAsync` calls with `ProjectItemsAsync` directly.

The key replacements:
- `JsonStreamReader.ProcessArrayAsync(pipe, path, callback, ct)` → `pipe.ForEachItemAsync(path, callback, ct)`
- `JsonStreamPipeline.TransformArrayAsync(...)` → manual envelope + `ProjectTypedAsync`
- `JsonStreamPipeline.PassthroughArrayAsync(...)` → manual envelope + `ProjectItemsAsync`
- `JsonStreamReader.WriteArrayAsync(pipe, path, writer, ct)` → `pipe.ProjectItemsAsync(path, writer, writeRawCallback, ct)`
- `JsonStreamReaderTyped.WriteArrayAsync(...)` → `pipe.ProjectTypedAsync(...)`

- [ ] **Step 3: Build sample app**

Run: `dotnet build samples/WebApiSample/`
Expected: Build succeeded.

- [ ] **Step 4: Commit**

```bash
git add samples/
git commit -m "feat: prove typed streaming as client extension methods in sample app"
```

---

## Task 3: Rewrite Tests for New API

**Files:**
- Create: `tests/JsonStreaming.Tests/ProjectItemsTests.cs` (already created in Task 1, add more tests)
- Delete: `tests/JsonStreaming.Tests/JsonStreamReaderTests.cs`
- Delete: `tests/JsonStreaming.Tests/JsonStreamPipelineTests.cs`
- Delete: `tests/JsonStreaming.Tests/TypedApiTests.cs`

- [ ] **Step 1: Add select-many and edge case tests to ProjectItemsTests**

Add to the existing `ProjectItemsTests.cs`:

```csharp
    // language=JSON
    private const string NestedArraysJson = """
        { "data": { "pages": [
            { "todos": [{"id":1},{"id":2}] },
            { "todos": [{"id":3}] }
        ] } }
        """;

    [Fact]
    public async Task ProjectItems_SelectMany_FlattensNestedArrays()
    {
        var items = new List<string>();
        var pipe = ToPipe(NestedArraysJson);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            NdJsonPath.At("data").Key("pages").Each().Key("todos").Each(),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

        items.Should().HaveCount(3);
    }

    [Fact]
    public async Task ProjectItems_EmptyArray_NoCallbacks()
    {
        var json = """{"items":[]}""";
        var items = new List<string>();
        var pipe = ToPipe(json);
        var output = PipeWriter.Create(Stream.Null);

        await pipe.ProjectItemsAsync(
            NdJsonPath.At("items").Each(),
            output,
            (itemBytes, writer) =>
            {
                items.Add(Encoding.UTF8.GetString(itemBytes));
                return ValueTask.CompletedTask;
            });

        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ProjectItems_ProcessItemCanWriteToOutput()
    {
        var pipe = ToPipe(PeopleJson);
        await using var ms = new MemoryStream();
        var output = PipeWriter.Create(ms);

        await pipe.ProjectItemsAsync(
            NdJsonPath.Each().Key("name"),
            output,
            async (itemBytes, writer) =>
            {
                writer.Write(itemBytes);
                writer.Write("\n"u8);
                await writer.FlushAsync();
            });

        await output.CompleteAsync();
        var lines = Encoding.UTF8.GetString(ms.ToArray())
            .Split('\n', StringSplitOptions.RemoveEmptyEntries);
        lines.Should().HaveCount(3);
    }
```

- [ ] **Step 2: Delete old test files**

```bash
git rm tests/JsonStreaming.Tests/JsonStreamReaderTests.cs
git rm tests/JsonStreaming.Tests/JsonStreamPipelineTests.cs
git rm tests/JsonStreaming.Tests/TypedApiTests.cs
```

- [ ] **Step 3: Run tests**

Run: `dotnet test`
Expected: All tests pass (new ProjectItemsTests + existing NaiveHumanTests + JsonTranscoderTests + JsonPathTests).

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test: rewrite tests for ProjectItemsAsync, delete old API tests"
```

---

## Task 4: Delete Old Source Files

**Files:**
- Delete: `src/JsonStreaming/JsonPath.cs`
- Delete: `src/JsonStreaming/JsonPathNavigator.cs`
- Delete: `src/JsonStreaming/JsonStreamReader.cs`
- Delete: `src/JsonStreaming/JsonStreamReaderTyped.cs`
- Delete: `src/JsonStreaming/JsonStreamPipeline.cs`

- [ ] **Step 1: Delete the files**

```bash
git rm src/JsonStreaming/JsonPath.cs
git rm src/JsonStreaming/JsonPathNavigator.cs
git rm src/JsonStreaming/JsonStreamReader.cs
git rm src/JsonStreaming/JsonStreamReaderTyped.cs
git rm src/JsonStreaming/JsonStreamPipeline.cs
```

- [ ] **Step 2: Build and fix any remaining references**

Run: `dotnet build`

If there are compilation errors from lingering references in sample app or tests, fix them. The sample app should already use extension methods from Task 2. The ConsoleProfiler sample needs updating — replace `JsonStreamReader.WriteArrayAsync` with `ProjectItemsAsync`.

- [ ] **Step 3: Run tests**

Run: `dotnet test`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor: delete JsonStreamReader, JsonStreamReaderTyped, JsonStreamPipeline, JsonPath, JsonPathNavigator"
```

---

## Task 5: Final Verification

- [ ] **Step 1: Run full test suite**

Run: `dotnet test`
Expected: All tests pass.

- [ ] **Step 2: Build all projects**

Run: `dotnet build`
Expected: 0 errors.

- [ ] **Step 3: Verify no old type references remain**

```bash
grep -r "JsonStreamReader\|JsonStreamReaderTyped\|JsonStreamPipeline\|WriteItemDelegate\|WriteOptions\b\|SegmentKind\|JsonPathNavigator" src/ samples/ tests/ --include="*.cs" | grep -v "obj/"
```
Expected: No matches.

- [ ] **Step 4: Verify library public API surface**

The library should expose exactly:
- `JsonTranscoder` (static class): `ProxyFormattedJsonAsync`, `ProxyMinifiedJsonAsync`, `ProjectNdJsonAsync`, `ProjectNdJsonVerbatimAsync`, `ProjectItemsAsync`
- `NdJsonPath` (sealed class): `Root`, `At`, `Each`, `Parse`, `ToJsonPath`, `Builder`

No other public types.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: consolidation complete — transcoder is the single streaming engine"
```
