# Consolidation: ProjectItemsAsync + Delete Old APIs

**Date:** 2026-04-07
**Status:** Approved
**Scope:** Add `ProjectItemsAsync` to the transcoder, delete all old streaming APIs, prove typed patterns as client extension methods in the sample app.

## Problem

`JsonStreamReader`, `JsonStreamReaderTyped`, `JsonStreamPipeline`, `JsonPath`, and `JsonPathNavigator` are legacy layers. The transcoder's directive-based FSM already handles all navigation and projection. These old files duplicate the FSM logic and couple typed deserialization into the library. The goal: one library method (`ProjectItemsAsync`) + client extension methods replace everything.

## Design

### ProjectItemsAsync — Library Addition

One new public method on `JsonTranscoder`:

```csharp
public static async Task ProjectItemsAsync(
    this PipeReader reader,
    NdJsonPath path,
    PipeWriter writer,
    Func<ReadOnlySequence<byte>, PipeWriter, ValueTask> processItem,
    JsonReaderOptions readerOptions = default,
    CancellationToken ct = default)
```

**Internals:**
- Uses the existing FSM (`ProjectionState.Advance()`) and directive loop
- Instead of `ITokenRenderer` writing tokens one by one, a buffering capture accumulates complete item bytes
- On `EndCapture` / `YieldValue`: slice item bytes from the read buffer, call `processItem(itemBytes, writer)`
- After the call, check `writer.UnflushedBytes` delta — if > 0, handle flushing if threshold exceeded
- No `IItemFramer` needed — the client owns framing (writes directly to PipeWriter)

**Item byte capture:**
- The FSM reports `BeginCapture` (start of complex item) and `EndCapture` (item complete)
- For items that fit in one read chunk: slice directly from the read buffer (zero-copy)
- For items spanning chunks: accumulate into a rented `ArrayPool<byte>` buffer, hand off as `ReadOnlySequence<byte>`, return to pool after `processItem` completes
- For primitives (`YieldValue`): single token, slice directly from current read buffer

### What Gets Deleted

**Files deleted:**
- `src/JsonStreaming/JsonPath.cs`
- `src/JsonStreaming/JsonPathNavigator.cs`
- `src/JsonStreaming/JsonStreamReader.cs`
- `src/JsonStreaming/JsonStreamReaderTyped.cs`
- `src/JsonStreaming/JsonStreamPipeline.cs`

**Types deleted:**
- `JsonPath`, `Segment`, `SegmentKind` (internal, in JsonPath.cs)
- `JsonPathNavigator` (internal)
- `JsonStreamReader` (public static)
- `JsonStreamReaderTyped` (public static)
- `JsonStreamPipeline` (public static)
- `WriteItemDelegate`
- `WriteOptions`

**Test files deleted and rewritten:**
- `tests/JsonStreaming.Tests/JsonStreamReaderTests.cs` — rewritten to test `ProjectItemsAsync`
- `tests/JsonStreaming.Tests/JsonStreamPipelineTests.cs` — rewritten as envelope test using client extension
- `tests/JsonStreaming.Tests/TypedApiTests.cs` — rewritten as client extension method tests

**Kept unchanged:**
- `JsonTranscoder.cs` (modified — gains `ProjectItemsAsync`)
- `NdJsonPath.cs`
- `tests/JsonStreaming.Tests/NaiveHumanTests.cs`
- `tests/JsonStreaming.Tests/JsonTranscoderTests.cs`
- `tests/JsonStreaming.Tests/JsonPathTests.cs`

### Sample App Proves Typed APIs as Client Code

Extension methods in the sample app — NOT in the library. These prove the transcoder is sufficient:

**1. Typed transform with filter + explode:**

```csharp
static async Task<int> ProjectTypedAsync<TIn, TOut>(
    this PipeReader reader, NdJsonPath path, PipeWriter output,
    JsonTypeInfo<TIn> inputType, JsonTypeInfo<TOut> outputType,
    Func<TIn, IEnumerable<TOut>> transform,
    CancellationToken ct = default)
```

Transform return values:
- `[mapped]` — project (1:1 transform)
- `[]` — filter (skip item)
- `[a, b, c]` — explode/selectMany

**2. Envelope wrapping (replaces JsonStreamPipeline):**

```csharp
static async Task<int> ProjectEnvelopeAsync<TIn, TOut>(
    this PipeReader reader, NdJsonPath path, PipeWriter output,
    string arrayName, JsonTypeInfo<TIn> inputType, JsonTypeInfo<TOut> outputType,
    Func<TIn, IEnumerable<TOut>> transform, CancellationToken ct = default)
```

Writes `{"<arrayName>":[...], "count": N}` envelope, delegates item streaming to `ProjectTypedAsync`.

**3. Raw callback (replaces ProcessArrayAsync):**

```csharp
static async Task<int> ForEachItemAsync(
    this PipeReader reader, NdJsonPath path,
    Action<ReadOnlySequence<byte>> processItem,
    CancellationToken ct = default)
```

Passes `PipeWriter.Create(Stream.Null)` as output — client only reads, doesn't write.

### Pattern Mapping

| Old API | New equivalent |
|---------|---------------|
| `JsonStreamReader.ProcessArrayAsync` | `ProjectItemsAsync` + `ForEachItemAsync` client extension |
| `JsonStreamReader.WriteArrayAsync` (verbatim) | `ProjectNdJsonVerbatimAsync` (already exists) |
| `JsonStreamReader.WriteArrayAsync` + `WriteItemDelegate` | `ProjectItemsAsync` directly |
| `JsonStreamReaderTyped.WriteArrayAsync<TIn,TOut>` | `ProjectTypedAsync` client extension |
| `JsonStreamReaderTyped.ProcessArrayAsync<T>` | `ForEachItemAsync` + deserialize in closure |
| `JsonStreamPipeline.TransformArrayAsync` | `ProjectEnvelopeAsync` client extension |
| `JsonStreamPipeline.PassthroughArrayAsync` | `ProjectItemsAsync` + write raw bytes |

### Definition of Done

If the sample app can replicate all use cases using only:
- `ProjectNdJsonAsync` / `ProjectNdJsonVerbatimAsync` (existing)
- `ProjectItemsAsync` (new)
- `ProxyFormattedJsonAsync` / `ProxyMinifiedJsonAsync` (existing)
- Client extension methods (in sample app code)

...then the old APIs are proven redundant and can be deleted.
