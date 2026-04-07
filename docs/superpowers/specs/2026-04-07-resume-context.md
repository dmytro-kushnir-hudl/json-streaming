# Resume Context: JSON Streaming Refactoring Session
**Date:** 2026-04-07
**Repo:** https://github.com/dmytro-kushnir-hudl/json-streaming

## What Was Done

### 1. Directive Pattern + Strategy Split (complete)
- `ParserDirective` enum: `Skip`, `YieldValue`, `BeginCapture`, `Capture`, `EndCapture`
- `ProjectionState.Advance()` encapsulates all FSM logic, private state
- `ITokenRenderer` (MinifiedRenderer, VerbatimRenderer) + `IItemFramer` (NdJsonFramer, JsonArrayFramer, JsonEnvelopeFramer)
- Single generic `WriteProjection<TRenderer, TFramer>` replaced two duplicate methods
- CopyToken bug fixed (was using `reader.ValueSpan` which strips quotes — now uses raw buffer slice)

### 2. Path Unification (complete)
- `NdJsonPath` is the single public path type with `Parse()`, `Property()`, `ToJsonPath()`, `Root`
- Old `JsonPath`, `Segment`, `SegmentKind` made internal (hidden behind `JsonStreamReader` until deletion)

### 3. API Consolidation (complete, but ProjectItemsAsync needs redesign)
- Deleted: `JsonStreamReader`, `JsonStreamReaderTyped`, `JsonStreamPipeline`, `JsonPath`, `JsonPathNavigator`
- Library is now just `JsonTranscoder.cs` + `NdJsonPath.cs`
- Public API: `ProxyFormattedJsonAsync`, `ProxyMinifiedJsonAsync`, `ProjectNdJsonAsync`, `ProjectNdJsonVerbatimAsync`, `ProjectItemsAsync`
- Sample app has client extension methods proving typed transform/filter/explode patterns

### 4. Benchmark Results (91→54 tests, all passing)

**Good results (no regressions vs pre-refactor):**
| Method | Before | After |
|--------|--------|-------|
| Transcoder NDJSON titles (direct) | 38ms / 2KB | 43ms / 2KB |
| Transcoder NDJSON all items (jwriter) | 60ms / 2KB | 58ms / 2KB |
| ProjectItems verbatim | 49ms / 15MB (old API) | 41ms / 49KB |
| ProjectItems Utf8JsonReader transform | 85ms / 15MB | 77ms / 49KB |
| NDJSON ProjectItems titles | 74ms / 15MB | 60ms / 2.7KB |

## The Unsolved Problem: ProjectItemsAsync Writer Lifecycle

### The Issue
`ProjectItemsAsync` signature:
```csharp
Func<ReadOnlySequence<byte>, PipeWriter, ValueTask> processItem
```

The client captures a `Utf8JsonWriter` OUTSIDE the library's control. The library can't flush it → unbounded buffer growth (67MB for 200K items). Client must manually flush in every callback — same footgun the old `WriteArrayAsync` had.

When we tried library-side flushing (`PipeWriter.FlushAsync` in sync fast-path), it crashed when `Utf8JsonWriter` wraps the `PipeWriter` via `IBufferWriter<byte>` — flushing the PipeWriter invalidates memory the jwriter was writing to.

### The Insight
This is the exact problem that triggered the original refactoring. The library needs to OWN the writer lifecycle to control flushing.

### Proposed Solution: Lifecycle Handler with TState

The library owns the full lifecycle: init → header → [onItem... flush...] → footer → dispose.

```csharp
Task ProjectItemsAsync<TState>(
    this PipeReader reader,
    NdJsonPath path,
    PipeWriter output,
    Func<PipeWriter, TState> init,                                         // create jwriter, counter
    Action<TState, PipeWriter> writeHeader,                                // write "[" or envelope
    Action<ReadOnlySequence<byte>, TState, PipeWriter> onItem,             // per match
    Action<TState, PipeWriter> writeFooter,                                // write "]", count, etc.
    Action<TState>? flush = null,                                          // library calls at 16KB
    Action<TState>? dispose = null,                                        // cleanup
    ...)
```

**Why this works:** `flush` is the missing piece. The library calls `flush(state)` at the threshold between items. The client's flush drains their jwriter into the PipeWriter (`jwriter.Flush()`). THEN the library flushes the PipeWriter (`pw.FlushAsync()`). Order matters — jwriter first, then PipeWriter.

**This unifies with IItemFramer:** header/footer IS framing. onItem IS processing. One interface, one lifecycle. Could also be expressed as a struct interface:

```csharp
interface IItemHandler<TState>
{
    TState Init(PipeWriter output);
    void WriteHeader(ref TState state, PipeWriter output);
    void OnItem(ReadOnlySequence<byte> itemBytes, ref TState state, PipeWriter output);
    void WriteFooter(ref TState state, PipeWriter output);
    void Flush(ref TState state);
}
```

### Key Constraint
When `Utf8JsonWriter` wraps a `PipeWriter` (IBufferWriter<byte>), `PipeWriter.FlushAsync` invalidates memory obtained via `GetSpan()`. The library must flush the jwriter FIRST (`jwriter.Flush()`), THEN flush the PipeWriter. The lifecycle's `flush` callback solves this — client flushes their writer, library flushes the pipe.

### Open Question
Delegates vs struct interface? Struct interface is zero-cost (JIT monomorphizes) but verbose to implement. Delegates are ergonomic but allocate closures. The existing `ITokenRenderer`/`IItemFramer` pattern uses struct interfaces — consistency argues for it. But for `ProjectItemsAsync` which is user-facing (not internal), delegates might be more practical.

## Files
- `src/JsonStreaming/JsonTranscoder.cs` — the single streaming engine (~550 lines)
- `src/JsonStreaming/NdJsonPath.cs` — the single path type (~120 lines)
- `samples/WebApiSample/StreamingExtensions.cs` — client extension methods proving typed patterns
- `benchmarks/JsonStreaming.Benchmarks/StreamingBenchmarks.cs` — perf benchmarks
- `docs/superpowers/specs/2026-04-07-directive-strategy-design.md` — directive pattern spec
- `docs/superpowers/specs/2026-04-07-consolidation-design.md` — consolidation spec

## Design Specs Still Relevant
- `IItemProcessor<TState>` from directive strategy spec — the struct interface approach may be the right answer for the writer lifecycle problem
- `ITokenRenderer` + `IItemFramer` pattern works well for the projection methods
- The init+callback+TState pattern needs brainstorming in next session

## What's Left
1. **Redesign `ProjectItemsAsync`** to solve the writer lifecycle/flush problem
2. **Investigate** PipeWriter.FlushAsync crash when Utf8JsonWriter wraps it
3. **Re-run benchmarks** after fix to confirm no regressions
4. **Consider** whether `IItemProcessor<TState>` struct interface is better than delegate+TState
