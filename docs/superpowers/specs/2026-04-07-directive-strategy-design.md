# Directive Pattern + Strategy Split for WriteProjection

**Date:** 2026-04-07
**Status:** Approved
**Scope:** `JsonTranscoder.cs` — refactor projection FSM into directive-producing state machine + generic strategy-based rendering

## Problem

`WriteProjection` and `WriteProjectionDirect` duplicate the entire search/capture FSM. They differ only in how tokens are rendered (Utf8JsonWriter vs raw buffer copy). This makes the FSM hard to maintain and impossible to extend with new renderers or output formats without further duplication.

## Design

### ParserDirective Enum

```csharp
enum ParserDirective
{
    Skip,             // Not on match path — ignore
    YieldValue,       // Complete primitive match — render and finish item
    BeginCapture,     // Complex match found — start capturing (current element is the opener)
    Capture,          // Inside capture — render this piece of the composite
    EndCapture,       // Capture complete (closer) — render and finish item
}
```

### ProjectionState.Advance()

Instance method on existing `ProjectionState` class. Encapsulates all FSM logic:

```csharp
public ParserDirective Advance(
    JsonTokenType tokenType,
    byte[][] pattern,
    ReadOnlySpan<byte> propertyName = default)
```

- `propertyName` is passed only for `PropertyName` tokens (caller materializes `ValueSequence` if needed)
- Manages: depth tracking, matched-depth stack, `IsArray`, `PendingPropertyMatches`, `IsCapturing`, `CaptureDepth`
- `MatchesProjectionSegment` and `MatchesPropertyName` become private helpers called inside `Advance()`

### ITokenRenderer (struct interface)

```csharp
interface ITokenRenderer
{
    void WriteToken(ref Utf8JsonReader reader, PipeWriter pipeWriter, ReadResult readResult);
    void Reset();  // called after each completed item
}
```

Two implementations:

**MinifiedRenderer** — wraps `Utf8JsonWriter`, uses existing `WriteToken` logic (WriteStartObject, WritePropertyName, etc.). `Reset()` calls `jwriter.Flush()` + `jwriter.Reset()`.

**VerbatimRenderer** — raw buffer copy via `CopyToken`. Owns comma tracking (`_needsComma`), which moves out of `ProjectionState`. `Reset()` clears `_needsComma`.

### IItemFramer (struct interface)

```csharp
interface IItemFramer
{
    void BeginDocument(PipeWriter pipeWriter);
    void FinishItem(PipeWriter pipeWriter);
    void EndDocument(PipeWriter pipeWriter);
}
```

Two implementations:

**NdJsonFramer** — `FinishItem` writes `\n`. `BeginDocument`/`EndDocument` are no-ops (JIT eliminates).

**JsonArrayFramer** — `BeginDocument` writes `[`, `FinishItem` writes `,` between items, `EndDocument` writes `]`. Included as a type but not wired to a public API yet.

**JsonEnvelopeFramer** — Wraps output in an object with metadata and a results array. Tracks item count. Example output:
```json
{"results":[{...},{...},{...}],"count":3}
```
`BeginDocument` writes `{"results":[`, `FinishItem` writes `,` between items, `EndDocument` writes `],"count":N}`. Included as a type but not wired to a public API yet.

### Unified Generic Method

```csharp
private static long WriteProjection<TRenderer, TFramer>(
    ProjectionState state,
    ReadResult readResult,
    PipeWriter pipeWriter,
    byte[][] pattern,
    ref TRenderer renderer,
    ref TFramer framer)
    where TRenderer : struct, ITokenRenderer
    where TFramer : struct, IItemFramer
```

Structs passed by `ref` to preserve mutable state across calls. JIT monomorphizes per `(TRenderer, TFramer)` pair — zero virtual dispatch overhead.

### Caller Loop Shape

```csharp
framer.BeginDocument(pipeWriter);
bool hasToken = reader.Read();

while (hasToken)
{
    ReadOnlySpan<byte> name = reader.TokenType == JsonTokenType.PropertyName
        ? GetPropertyName(ref reader)
        : default;

    var directive = state.Advance(reader.TokenType, pattern, name);

    switch (directive)
    {
        case ParserDirective.Skip:
            break;
        case ParserDirective.YieldValue:
            renderer.WriteToken(ref reader, pipeWriter, readResult);
            renderer.Reset();
            framer.FinishItem(pipeWriter);
            break;
        case ParserDirective.BeginCapture:
            continue;  // do not advance reader
        case ParserDirective.Capture:
            renderer.WriteToken(ref reader, pipeWriter, readResult);
            break;
        case ParserDirective.EndCapture:
            renderer.WriteToken(ref reader, pipeWriter, readResult);
            renderer.Reset();
            framer.FinishItem(pipeWriter);
            break;
    }

    hasToken = reader.Read();
}

framer.EndDocument(pipeWriter);
```

### Helper: GetPropertyName

Extracts property name bytes from the reader. For single-segment values, returns `reader.ValueSpan` directly (zero-copy). For multi-segment `ValueSequence`, rents a buffer from `ArrayPool<byte>`, copies into it, and returns a span over the rented buffer. The caller is responsible for returning the rented buffer after `Advance()` completes — a simple `try/finally` around the `GetPropertyName` + `Advance` pair. This mirrors the existing pattern in `WriteTokenSequence`.

## What Changes

**Added:**
- `ParserDirective` enum
- `ITokenRenderer` interface + `MinifiedRenderer`, `VerbatimRenderer` structs
- `IItemFramer` interface + `NdJsonFramer`, `JsonArrayFramer` structs
- `ProjectionState.Advance()` method
- `GetPropertyName()` helper

**Removed:**
- `WriteProjectionDirect` (unified into generic `WriteProjection<TRenderer, TFramer>`)
- `CaptureNeedsComma` from `ProjectionState` (moved to `VerbatimRenderer`)
- Local `static void WriteToken()` function (absorbed into `MinifiedRenderer`)
- Duplicated FSM logic between the two methods

**Unchanged:**
- `WriteFormatted` / `WriteMinified` (proxy methods)
- `CopyToken` shared helper
- All public API signatures (`ProjectNdJsonAsync`, `ProjectNdJsonVerbatimAsync`)
- `NdJsonPath.cs`
- All existing tests pass without modification

## Future Extension: IItemProcessor (not implemented now)

A user-provided struct strategy for map/filter/transform of matched items. The library buffers each complete matched item, then hands it off as `ReadOnlySequence<byte>`. The user decides what (if anything) to write to the output.

**Agreed shape:**

```csharp
interface IItemProcessor<TState>
{
    ValueTask ProcessAsync(ReadOnlySequence<byte> itemBytes, PipeWriter output, ref TState state);
}
```

**Composition:** A second generic overload of `WriteProjection` that shares the FSM + directives but consumes `BeginCapture`...`EndCapture` by buffering, not token-by-token rendering. The processor owns both transformation and framing.

```csharp
WriteProjection<TProcessor, TState>(state, readResult, pipeWriter, pattern, ref processor, ref TState userState)
```

**Use case:** SumoLogic log field filtering for LLM context preservation — select/drop fields from streamed objects before feeding into context window. Item sizes are small-to-medium, so buffering cost is negligible.

**Status:** Design agreed, implementation deferred. Will add a second overload when the first concrete consumer needs it.

## All Types Nested Inside JsonTranscoder

Consistent with existing pattern (`FormattedState`, `MinifiedState`, `ProjectionState`). No new files.
