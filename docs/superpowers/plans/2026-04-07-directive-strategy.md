# Directive Pattern + Strategy Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the projection FSM into a directive-producing state machine with generic struct strategies for rendering and framing, unifying `WriteProjection` and `WriteProjectionDirect` into a single generic method.

**Architecture:** `ProjectionState.Advance()` encapsulates all search/capture FSM logic and returns a `ParserDirective` enum. A unified `WriteProjection<TRenderer, TFramer>()` generic method consumes directives and delegates rendering to struct strategies (`MinifiedRenderer`, `VerbatimRenderer`) and framing to struct framers (`NdJsonFramer`). JIT monomorphizes per type pair for zero virtual dispatch.

**Tech Stack:** C# / .NET 10, System.Text.Json, System.IO.Pipelines

**Spec:** `docs/superpowers/specs/2026-04-07-directive-strategy-design.md`

**Baseline:** 79 passing tests (11 pre-existing failures: 3 verbatim-vs-minified comparison, 8 external URL-dependent). Run: `dotnet test --filter "FullyQualifiedName~NaiveHumanTests.Project_ | FullyQualifiedName~JsonTranscoderTests.ProjectNdJsonDirect_Primitive"` for the stable projection subset.

---

## File Map

All changes in a single file, consistent with existing project structure:

- **Modify:** `src/JsonStreaming/JsonTranscoder.cs`
  - Add: `ParserDirective` enum (nested)
  - Add: `ITokenRenderer` interface (nested)
  - Add: `MinifiedRenderer` struct (nested)
  - Add: `VerbatimRenderer` struct (nested)
  - Add: `IItemFramer` interface (nested)
  - Add: `NdJsonFramer` struct (nested)
  - Add: `JsonArrayFramer` struct (nested, not wired to public API)
  - Add: `JsonEnvelopeFramer` struct (nested, not wired to public API)
  - Modify: `ProjectionState` — add `Advance()` method, remove `CaptureNeedsComma`
  - Add: `GetPropertyName()` static helper
  - Replace: `WriteProjection` + `WriteProjectionDirect` with single generic `WriteProjection<TRenderer, TFramer>`
  - Update: `ProjectNdJsonAsync` + `ProjectNdJsonVerbatimAsync` to call generic method

- **No changes:** `NdJsonPath.cs`, test files, sample projects

---

## Task 1: Add ParserDirective Enum and Interfaces

**Files:**
- Modify: `src/JsonStreaming/JsonTranscoder.cs:743-770` (state classes section)

- [ ] **Step 1: Add the enum and interfaces**

Add these nested types inside `JsonTranscoder`, just before the state classes section (before line 743):

```csharp
// ── Directive & Strategy types ───────────────────────────────────────

private enum ParserDirective
{
    Skip,
    YieldValue,
    BeginCapture,
    Capture,
    EndCapture,
}

private interface ITokenRenderer
{
    void WriteToken(ref Utf8JsonReader reader, PipeWriter pipeWriter, ReadResult readResult);
    void Reset();
}

private interface IItemFramer
{
    void BeginDocument(PipeWriter pipeWriter);
    void FinishItem(PipeWriter pipeWriter);
    void EndDocument(PipeWriter pipeWriter);
}
```

- [ ] **Step 2: Build to verify compilation**

Run: `dotnet build`
Expected: Build succeeded, 0 errors. The types are private and unused — no warnings expected beyond any pre-existing ones.

- [ ] **Step 3: Commit**

```bash
git add src/JsonStreaming/JsonTranscoder.cs
git commit -m "refactor: add ParserDirective enum and strategy interfaces"
```

---

## Task 2: Add Struct Strategy Implementations

**Files:**
- Modify: `src/JsonStreaming/JsonTranscoder.cs` (after the interfaces from Task 1)

- [ ] **Step 1: Add MinifiedRenderer**

Add immediately after the `IItemFramer` interface:

```csharp
private struct MinifiedRenderer : ITokenRenderer
{
    private Utf8JsonWriter _jwriter;

    public MinifiedRenderer(Utf8JsonWriter jwriter) => _jwriter = jwriter;

    public void WriteToken(ref Utf8JsonReader reader, PipeWriter pipeWriter, ReadResult readResult)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.StartObject: _jwriter.WriteStartObject(); break;
            case JsonTokenType.EndObject:   _jwriter.WriteEndObject(); break;
            case JsonTokenType.StartArray:  _jwriter.WriteStartArray(); break;
            case JsonTokenType.EndArray:    _jwriter.WriteEndArray(); break;
            case JsonTokenType.True:        _jwriter.WriteBooleanValue(true); break;
            case JsonTokenType.False:       _jwriter.WriteBooleanValue(false); break;
            case JsonTokenType.Null:        _jwriter.WriteNullValue(); break;

            case JsonTokenType.PropertyName:
            case JsonTokenType.String:
            case JsonTokenType.Number:
                WriteValueToken(ref reader);
                break;

            case JsonTokenType.Comment:
            case JsonTokenType.None:
                break;
        }
    }

    public void Reset()
    {
        _jwriter.Flush();
        _jwriter.Reset();
    }

    private void WriteValueToken(ref Utf8JsonReader reader)
    {
        if (!reader.HasValueSequence)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.PropertyName: _jwriter.WritePropertyName(reader.ValueSpan); break;
                case JsonTokenType.String:       _jwriter.WriteStringValue(reader.ValueSpan); break;
                case JsonTokenType.Number:       _jwriter.WriteRawValue(reader.ValueSpan, skipInputValidation: true); break;
            }
        }
        else
        {
            int len = (int)reader.ValueSequence.Length;
            byte[] rented = System.Buffers.ArrayPool<byte>.Shared.Rent(len);
            try
            {
                reader.ValueSequence.CopyTo(rented);
                ReadOnlySpan<byte> span = rented.AsSpan(0, len);
                switch (reader.TokenType)
                {
                    case JsonTokenType.PropertyName: _jwriter.WritePropertyName(span); break;
                    case JsonTokenType.String:       _jwriter.WriteStringValue(span); break;
                    case JsonTokenType.Number:       _jwriter.WriteRawValue(span, skipInputValidation: true); break;
                }
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }
}
```

- [ ] **Step 2: Add VerbatimRenderer**

Add immediately after `MinifiedRenderer`:

```csharp
private struct VerbatimRenderer : ITokenRenderer
{
    private bool _needsComma;

    public void WriteToken(ref Utf8JsonReader reader, PipeWriter pipeWriter, ReadResult readResult)
    {
        if (_needsComma && reader.TokenType is not JsonTokenType.EndObject and not JsonTokenType.EndArray)
            pipeWriter.Write(","u8);

        _needsComma = reader.TokenType switch
        {
            JsonTokenType.StartObject or JsonTokenType.StartArray or JsonTokenType.PropertyName => false,
            _ => true,
        };

        CopyToken(reader, pipeWriter, readResult);
    }

    public void Reset() => _needsComma = false;
}
```

- [ ] **Step 3: Add NdJsonFramer, JsonArrayFramer, JsonEnvelopeFramer**

Add immediately after `VerbatimRenderer`:

```csharp
private struct NdJsonFramer : IItemFramer
{
    public void BeginDocument(PipeWriter pipeWriter) { }
    public void FinishItem(PipeWriter pipeWriter) => pipeWriter.Write("\n"u8);
    public void EndDocument(PipeWriter pipeWriter) { }
}

private struct JsonArrayFramer : IItemFramer
{
    private bool _needsComma;

    public void BeginDocument(PipeWriter pipeWriter) => pipeWriter.Write("["u8);

    public void FinishItem(PipeWriter pipeWriter)
    {
        if (_needsComma)
            pipeWriter.Write(","u8);
        _needsComma = true;
    }

    public void EndDocument(PipeWriter pipeWriter) => pipeWriter.Write("]"u8);
}

private struct JsonEnvelopeFramer : IItemFramer
{
    private bool _needsComma;
    private int _count;

    public void BeginDocument(PipeWriter pipeWriter) => pipeWriter.Write("{\"results\":["u8);

    public void FinishItem(PipeWriter pipeWriter)
    {
        if (_needsComma)
            pipeWriter.Write(","u8);
        _needsComma = true;
        _count++;
    }

    public void EndDocument(PipeWriter pipeWriter)
    {
        pipeWriter.Write("],\"count\":"u8);
        Span<byte> buf = stackalloc byte[20];
        if (System.Buffers.Text.Utf8Formatter.TryFormat(_count, buf, out int written))
            pipeWriter.Write(buf[..written]);
        pipeWriter.Write("}"u8);
    }
}
```

- [ ] **Step 4: Build to verify compilation**

Run: `dotnet build`
Expected: Build succeeded. Structs are unused — no functional change yet.

- [ ] **Step 5: Commit**

```bash
git add src/JsonStreaming/JsonTranscoder.cs
git commit -m "refactor: add MinifiedRenderer, VerbatimRenderer, and framer structs"
```

---

## Task 3: Add ProjectionState.Advance() Method

**Files:**
- Modify: `src/JsonStreaming/JsonTranscoder.cs` — the `ProjectionState` class

- [ ] **Step 1: Write a focused unit test for the Advance() FSM**

Create a new test file `tests/JsonStreaming.Tests/ProjectionStateAdvanceTests.cs`:

```csharp
using System.Text.Json;
using JsonStreaming;

namespace JsonStreaming.Tests;

/// <summary>
/// Tests the ProjectionState FSM via the public ProjectNdJsonAsync API.
/// Validates that directive transitions are correct by checking output.
/// These tests verify the FSM behavior indirectly — the Advance() method
/// is internal to JsonTranscoder, so we test through the public surface.
/// </summary>
public class ProjectionStateAdvanceTests
{
    // We test the FSM through the existing public API.
    // If Advance() produces wrong directives, these tests will fail
    // because the output will be wrong.
    // This file exists to make the INTENT clear — these are FSM tests,
    // not rendering tests.

    // Tested via NaiveHumanTests already — this task is about adding Advance(),
    // not new test coverage. We verify by running the full suite after wiring.
}
```

Actually — `Advance()` is private (nested in `JsonTranscoder`). We can't unit test it directly. The correct approach: implement `Advance()`, wire it up (Task 4), then verify all 79 tests still pass. The existing tests already cover every FSM path (primitive match, object capture, array wildcard, nested paths, no-match).

Skip creating a new test file. The existing suite IS the test.

- [ ] **Step 2: Add GetPropertyName helper**

Add as a `private static` method inside `JsonTranscoder`, near the existing `CopyToken` helper:

```csharp
private static ReadOnlySpan<byte> GetPropertyName(
    ref Utf8JsonReader reader,
    ref byte[]? rentedBuffer)
{
    if (!reader.HasValueSequence)
        return reader.ValueSpan;

    int len = (int)reader.ValueSequence.Length;
    rentedBuffer = ArrayPool<byte>.Shared.Rent(len);
    reader.ValueSequence.CopyTo(rentedBuffer);
    return rentedBuffer.AsSpan(0, len);
}
```

The caller passes a `byte[]? rentedBuffer = null` and returns it to the pool after `Advance()`:

```csharp
byte[]? rentedBuffer = null;
try
{
    ReadOnlySpan<byte> name = reader.TokenType == JsonTokenType.PropertyName
        ? GetPropertyName(ref reader, ref rentedBuffer)
        : default;
    directive = state.Advance(reader.TokenType, pattern, name);
}
finally
{
    if (rentedBuffer != null)
    {
        ArrayPool<byte>.Shared.Return(rentedBuffer);
        rentedBuffer = null;
    }
}
```

- [ ] **Step 3: Add Advance() to ProjectionState**

Add as a public method on the `ProjectionState` class. This method contains ALL the search/capture FSM logic currently spread across `WriteProjection` and `WriteProjectionDirect`:

```csharp
private sealed class ProjectionState
{
    public int Depth = -1;
    public int MatchedDepth;
    public readonly bool[] IsArray = new bool[64];
    public readonly int[] MatchedDepthStack = new int[64];
    public bool PendingPropertyMatches;
    public bool IsCapturing;
    public int CaptureDepth;
    public JsonReaderState ReaderState;

    public ParserDirective Advance(
        JsonTokenType tokenType,
        byte[][] pattern,
        ReadOnlySpan<byte> propertyName = default)
    {
        // ── CAPTURE PHASE ─────────────────────────────────────────
        if (IsCapturing)
        {
            if (tokenType is JsonTokenType.StartObject or JsonTokenType.StartArray)
                CaptureDepth++;
            else if (tokenType is JsonTokenType.EndObject or JsonTokenType.EndArray)
                CaptureDepth--;

            if (CaptureDepth == 0)
            {
                IsCapturing = false;
                return ParserDirective.EndCapture;
            }

            return ParserDirective.Capture;
        }

        // ── SEARCH PHASE ──────────────────────────────────────────
        switch (tokenType)
        {
            case JsonTokenType.PropertyName:
                PendingPropertyMatches = MatchedDepth == Depth
                    && MatchesPropertyName(pattern, MatchedDepth, propertyName);
                return ParserDirective.Skip;

            case JsonTokenType.StartObject:
            case JsonTokenType.StartArray:
            {
                bool isArray = tokenType == JsonTokenType.StartArray;
                bool parentIsArray = Depth >= 0 && IsArray[Depth];

                bool seg = MatchedDepth == Depth
                    && MatchesSegment(MatchedDepth, pattern, parentIsArray, PendingPropertyMatches);
                PendingPropertyMatches = false;

                Depth++;
                IsArray[Depth] = isArray;
                MatchedDepthStack[Depth] = MatchedDepth;

                if (seg && MatchedDepth + 1 == pattern.Length)
                {
                    Depth--;
                    MatchedDepth = MatchedDepthStack[Depth + 1];
                    IsCapturing = true;
                    CaptureDepth = 0;
                    return ParserDirective.BeginCapture;
                }

                if (seg)
                    MatchedDepth++;

                return ParserDirective.Skip;
            }

            case JsonTokenType.EndObject:
            case JsonTokenType.EndArray:
                PendingPropertyMatches = false;
                if (Depth >= 0)
                {
                    MatchedDepth = MatchedDepthStack[Depth];
                    Depth--;
                }
                return ParserDirective.Skip;

            default:
            {
                bool parentIsArray = Depth >= 0 && IsArray[Depth];

                bool seg = MatchedDepth == Depth
                    && MatchesSegment(MatchedDepth, pattern, parentIsArray, PendingPropertyMatches);
                PendingPropertyMatches = false;

                if (seg && MatchedDepth + 1 == pattern.Length)
                    return ParserDirective.YieldValue;

                return ParserDirective.Skip;
            }
        }
    }

    private static bool MatchesSegment(
        int matchedDepth,
        byte[][] pattern,
        bool parentIsArray,
        bool pendingPropertyMatches)
    {
        if (matchedDepth >= pattern.Length)
            return false;

        var seg = pattern[matchedDepth];

        if (seg.Length == 0)
            return parentIsArray;

        return !parentIsArray && pendingPropertyMatches;
    }

    private static bool MatchesPropertyName(
        byte[][] pattern,
        int matchedDepth,
        ReadOnlySpan<byte> propertyName)
    {
        if (matchedDepth >= pattern.Length)
            return false;

        var expected = pattern[matchedDepth];
        if (expected.Length == 0)
            return false;

        return propertyName.SequenceEqual(expected);
    }
}
```

Note: `MatchesSegment` and `MatchesPropertyName` are simplified versions of the existing static helpers. The key difference: `MatchesPropertyName` now takes a `ReadOnlySpan<byte>` instead of a `ref Utf8JsonReader` — the caller already materialized the name. This makes the FSM independent of the reader.

- [ ] **Step 4: Build to verify compilation**

Run: `dotnet build`
Expected: Build succeeded. `Advance()` exists but is not yet called.

- [ ] **Step 5: Commit**

```bash
git add src/JsonStreaming/JsonTranscoder.cs
git commit -m "refactor: add ProjectionState.Advance() and GetPropertyName helper"
```

---

## Task 4: Unify WriteProjection into Generic Method

This is the critical task — replace both `WriteProjection` and `WriteProjectionDirect` with a single generic method.

**Files:**
- Modify: `src/JsonStreaming/JsonTranscoder.cs:336-668` (both WriteProjection methods)

- [ ] **Step 1: Add the unified generic WriteProjection method**

Add this method BELOW the existing `WriteProjection` and `WriteProjectionDirect` methods (we'll remove the old ones after verifying):

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
{
    var reader = new Utf8JsonReader(
        readResult.Buffer,
        readResult.IsCompleted,
        state.ReaderState
    );

    bool hasToken = reader.Read();

    while (hasToken)
    {
        byte[]? rentedBuffer = null;
        ParserDirective directive;
        try
        {
            ReadOnlySpan<byte> name = reader.TokenType == JsonTokenType.PropertyName
                ? GetPropertyName(ref reader, ref rentedBuffer)
                : default;

            directive = state.Advance(reader.TokenType, pattern, name);
        }
        finally
        {
            if (rentedBuffer != null)
            {
                ArrayPool<byte>.Shared.Return(rentedBuffer);
            }
        }

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
                // Do not advance reader — capture phase will process
                // the current StartObject/StartArray on next iteration
                continue;

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

    state.ReaderState = reader.CurrentState;
    return reader.BytesConsumed;
}
```

- [ ] **Step 2: Update ProjectNdJsonAsync to use the generic method**

Change the call inside `ProjectNdJsonAsync` from:

```csharp
var bytesConsumed = WriteProjection(state, result, jwriter, writer, path.Segments);
```

to:

```csharp
var minRenderer = new MinifiedRenderer(jwriter);
var framer = new NdJsonFramer();
var bytesConsumed = WriteProjection(state, result, writer, path.Segments, ref minRenderer, ref framer);
```

Also remove the `jwriter.Flush()` and `jwriter.Reset()` calls from the async method if they exist in the flush block — the renderer's `Reset()` now handles flushing the jwriter per item. The async-level flush (`await jwriter.FlushAsync`) still applies for backpressure.

- [ ] **Step 3: Run tests to verify ProjectNdJsonAsync still works**

Run: `dotnet test --filter "FullyQualifiedName~NaiveHumanTests.Project_"`
Expected: All 7 projection tests pass (Project_TopLevelPrimitive, Project_TopLevelObject, etc.)

- [ ] **Step 4: Update ProjectNdJsonVerbatimAsync to use the generic method**

Change the call inside `ProjectNdJsonVerbatimAsync` from:

```csharp
var bytesConsumed = WriteProjectionDirect(state, result, writer, path.Segments);
```

to:

```csharp
var verbatimRenderer = new VerbatimRenderer();
var framer = new NdJsonFramer();
var bytesConsumed = WriteProjection(state, result, writer, path.Segments, ref verbatimRenderer, ref framer);
```

- [ ] **Step 5: Run full test suite**

Run: `dotnet test`
Expected: Same 79 pass / 11 fail baseline. No regressions.

- [ ] **Step 6: Commit**

```bash
git add src/JsonStreaming/JsonTranscoder.cs
git commit -m "refactor: wire ProjectNdJsonAsync and ProjectNdJsonVerbatimAsync to generic WriteProjection"
```

---

## Task 5: Remove Old Code

**Files:**
- Modify: `src/JsonStreaming/JsonTranscoder.cs`

- [ ] **Step 1: Delete the old WriteProjection method (non-generic)**

Remove the entire old `WriteProjection` method (the one with signature `WriteProjection(ProjectionState state, ReadResult readResult, Utf8JsonWriter jwriter, PipeWriter pipeWriter, byte[][] pattern)`), including its local `WriteToken` and `WriteTokenSequence` static functions.

- [ ] **Step 2: Delete the old WriteProjectionDirect method**

Remove the entire `WriteProjectionDirect` method.

- [ ] **Step 3: Delete the old static MatchesProjectionSegment and MatchesPropertyName helpers**

These are now inlined into `ProjectionState.Advance()` as `MatchesSegment` and `MatchesPropertyName`. Remove the old top-level static versions.

- [ ] **Step 4: Remove CaptureNeedsComma from ProjectionState**

Remove the `public bool CaptureNeedsComma;` field from `ProjectionState`. This state is now owned by `VerbatimRenderer._needsComma`.

- [ ] **Step 5: Build and test**

Run: `dotnet build && dotnet test`
Expected: Build succeeded. Same 79 pass / 11 fail baseline.

- [ ] **Step 6: Commit**

```bash
git add src/JsonStreaming/JsonTranscoder.cs
git commit -m "refactor: remove old WriteProjection, WriteProjectionDirect, and duplicated helpers"
```

---

## Task 6: Add BeginDocument/EndDocument Calls

The framer's `BeginDocument` / `EndDocument` need to be called at the async method level, not inside `WriteProjection` (which is called per-chunk). The generic `WriteProjection` only sees one chunk at a time.

**Files:**
- Modify: `src/JsonStreaming/JsonTranscoder.cs` — `ProjectNdJsonAsync` and `ProjectNdJsonVerbatimAsync`

- [ ] **Step 1: Add BeginDocument/EndDocument to ProjectNdJsonAsync**

In `ProjectNdJsonAsync`, add `framer.BeginDocument(writer)` before the `while (true)` loop and `framer.EndDocument(writer)` after the loop (before the method returns). Since `NdJsonFramer.BeginDocument` is a no-op, this has zero effect but establishes the contract.

The framer must be declared outside the loop so it persists across chunks:

```csharp
public static async Task ProjectNdJsonAsync(...)
{
    var jwriter = new Utf8JsonWriter(writer, writerOptions);
    var state = new ProjectionState { ReaderState = new JsonReaderState(readerOptions) };
    var renderer = new MinifiedRenderer(jwriter);
    var framer = new NdJsonFramer();
    ct.ThrowIfCancellationRequested();

    framer.BeginDocument(writer);

    while (true)
    {
        // ... existing chunk loop ...
        var bytesConsumed = WriteProjection(state, result, writer, path.Segments, ref renderer, ref framer);
        // ...
    }

    framer.EndDocument(writer);
}
```

- [ ] **Step 2: Same for ProjectNdJsonVerbatimAsync**

```csharp
public static async Task ProjectNdJsonVerbatimAsync(...)
{
    var state = new ProjectionState { ReaderState = new JsonReaderState(options) };
    var renderer = new VerbatimRenderer();
    var framer = new NdJsonFramer();
    ct.ThrowIfCancellationRequested();

    framer.BeginDocument(writer);

    while (true)
    {
        // ... existing chunk loop ...
        var bytesConsumed = WriteProjection(state, result, writer, path.Segments, ref renderer, ref framer);
        // ...
    }

    framer.EndDocument(writer);
}
```

- [ ] **Step 3: Run tests**

Run: `dotnet test`
Expected: Same 79 pass / 11 fail baseline. No change in behavior (NdJsonFramer's Begin/End are no-ops).

- [ ] **Step 4: Commit**

```bash
git add src/JsonStreaming/JsonTranscoder.cs
git commit -m "refactor: add BeginDocument/EndDocument framer calls to projection async methods"
```

---

## Task 7: Final Verification and Cleanup

- [ ] **Step 1: Run full test suite one final time**

Run: `dotnet test`
Expected: 79 pass / 11 fail (same baseline).

- [ ] **Step 2: Verify the sample app still compiles**

Run: `dotnet build samples/WebApiSample/`
Expected: Build succeeded. Public API unchanged.

- [ ] **Step 3: Quick review of final file**

Read through `JsonTranscoder.cs` and verify:
- No leftover references to old `WriteProjectionDirect`
- No leftover `CaptureNeedsComma` on `ProjectionState`
- No orphaned local functions (`WriteToken`, `WriteTokenSequence`)
- `MatchesProjectionSegment` and `MatchesPropertyName` top-level statics are gone
- The generic `WriteProjection<TRenderer, TFramer>` is the only projection write method

- [ ] **Step 4: Commit any cleanup**

```bash
git add src/JsonStreaming/JsonTranscoder.cs
git commit -m "refactor: directive pattern + strategy split complete"
```
