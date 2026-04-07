# Path Unification & Dead Code Deletion

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unify `NdJsonPath` and `JsonPath` into a single path type, delete the redundant `JsonPathNavigator`, and update all consumers.

**Architecture:** `NdJsonPath` absorbs `JsonPath`'s `Parse()`, fluent `Property()`/`Each()` API, and `ToJsonPath()` serialization. The `byte[][]` segment encoding stays (simpler, already UTF-8). `JsonPath` and `JsonPathNavigator` are deleted. `JsonStreamReader`/`JsonStreamReaderTyped`/`JsonStreamPipeline` are updated to use `NdJsonPath` — they keep working via an internal shim that converts `NdJsonPath` segments to the old navigator format until Plan B replaces them with the transcoder.

**Tech Stack:** C# / .NET 10, System.Text.Json

**Baseline:** 91 tests passing.

---

## File Map

- **Modify:** `src/JsonStreaming/NdJsonPath.cs` — add `Parse()`, `Property()`, `ToJsonPath()`, `ToString()`
- **Modify:** `src/JsonStreaming/JsonStreamReader.cs` — accept `NdJsonPath` instead of `JsonPath`, add internal conversion
- **Modify:** `src/JsonStreaming/JsonStreamReaderTyped.cs` — accept `NdJsonPath` instead of `JsonPath`
- **Modify:** `src/JsonStreaming/JsonStreamPipeline.cs` — accept `NdJsonPath` instead of `JsonPath`
- **Delete:** `src/JsonStreaming/JsonPath.cs`
- **Delete:** `src/JsonStreaming/JsonPathNavigator.cs`
- **Modify:** `tests/JsonStreaming.Tests/JsonPathTests.cs` — rewrite for `NdJsonPath`
- **Modify:** `tests/JsonStreaming.Tests/JsonStreamReaderTests.cs` — switch to `NdJsonPath`
- **Modify:** `tests/JsonStreaming.Tests/JsonStreamPipelineTests.cs` — switch to `NdJsonPath`
- **Modify:** `tests/JsonStreaming.Tests/TypedApiTests.cs` — switch to `NdJsonPath`
- **Modify:** `samples/WebApiSample/Program.cs` — switch to `NdJsonPath`
- **Modify:** `samples/ConsoleProfiler/Program.cs` — switch to `NdJsonPath`

---

## Task 1: Expand NdJsonPath with JsonPath's API

**Files:**
- Modify: `src/JsonStreaming/NdJsonPath.cs`
- Modify: `tests/JsonStreaming.Tests/JsonPathTests.cs`

- [ ] **Step 1: Add Property(byte[]) and Property(string) to Builder**

In `NdJsonPath.Builder`, `Key(string)` already exists. Add `Property()` as an alias that matches the old `JsonPath.Property()` pattern, plus a `ReadOnlySpan<byte>` overload:

```csharp
// In NdJsonPath.Builder:

/// <summary>Descend into a named object property (alias for Key).</summary>
public Builder Property(string name) => Key(name);

/// <summary>Descend into a named object property from UTF-8 bytes.</summary>
public Builder Property(ReadOnlySpan<byte> utf8Name)
{
    _segments.Add(utf8Name.ToArray());
    return this;
}
```

- [ ] **Step 2: Add static Parse(string) to NdJsonPath**

Port from `JsonPath.Parse()`:

```csharp
/// <summary>
/// Parse a JSONPath string into an <see cref="NdJsonPath"/>.
/// Supported subset: <c>$</c>, <c>.property</c>, <c>[*]</c>.
/// </summary>
public static NdJsonPath Parse(ReadOnlySpan<char> jsonPath)
{
    if (jsonPath.IsEmpty)
        return new NdJsonPath([]);

    var segments = new List<byte[]>();
    int i = 0;

    if (i < jsonPath.Length && jsonPath[i] == '$')
        i++;

    while (i < jsonPath.Length)
    {
        if (jsonPath[i] == '.')
        {
            i++;
            int start = i;
            while (i < jsonPath.Length && jsonPath[i] != '.' && jsonPath[i] != '[')
                i++;
            if (i > start)
                segments.Add(Encoding.UTF8.GetBytes(jsonPath[start..i].ToString()));
        }
        else if (i + 2 < jsonPath.Length && jsonPath[i] == '[' && jsonPath[i + 1] == '*' && jsonPath[i + 2] == ']')
        {
            segments.Add(Wildcard);
            i += 3;
        }
        else
        {
            i++;
        }
    }

    return new NdJsonPath([.. segments]);
}
```

- [ ] **Step 3: Add ToJsonPath() and ToString()**

```csharp
/// <summary>
/// Converts this path to a JSONPath string (e.g. <c>$.response[*].messages</c>).
/// </summary>
public string ToJsonPath()
{
    if (Segments.Length == 0)
        return "$";

    var sb = new StringBuilder("$");
    foreach (var seg in Segments)
    {
        if (seg.Length == 0)
            sb.Append("[*]");
        else
            sb.Append('.').Append(Encoding.UTF8.GetString(seg));
    }
    return sb.ToString();
}

/// <inheritdoc />
public override string ToString() => ToJsonPath();
```

- [ ] **Step 4: Add static Root and convenience At() that returns NdJsonPath directly**

```csharp
/// <summary>Empty path — targets the root.</summary>
public static NdJsonPath Root { get; } = new([]);
```

- [ ] **Step 5: Rewrite JsonPathTests for NdJsonPath**

Replace all `JsonPath` references with `NdJsonPath`. Drop `SegmentKind`-based assertions (NdJsonPath doesn't expose segment kinds — empty = wildcard, non-empty = property). The test file becomes:

```csharp
using System.Text;
using FluentAssertions;
using JsonStreaming;

namespace JsonStreaming.Tests;

public class JsonPathTests
{
    [Fact]
    public void Root_HasNoSegments()
    {
        NdJsonPath.Root.Segments.Length.Should().Be(0);
    }

    [Fact]
    public void At_CreatesPropertySegment()
    {
        NdJsonPath path = NdJsonPath.At("messages");
        path.Segments.Length.Should().Be(1);
        path.Segments[0].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("messages"));
    }

    [Fact]
    public void Each_CreatesWildcardSegment()
    {
        NdJsonPath path = NdJsonPath.Each();
        path.Segments.Length.Should().Be(1);
        path.Segments[0].Length.Should().Be(0);
    }

    [Fact]
    public void Chained_BuildsCorrectPath()
    {
        NdJsonPath path = NdJsonPath.At("response").Each().Key("messages");
        path.Segments.Length.Should().Be(3);
        path.Segments[0].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("response"));
        path.Segments[1].Length.Should().Be(0); // wildcard
        path.Segments[2].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("messages"));
    }

    [Fact]
    public void Property_Utf8_Works()
    {
        NdJsonPath path = NdJsonPath.Each().Property("name"u8);
        path.Segments.Length.Should().Be(2);
        path.Segments[1].Should().BeEquivalentTo("name"u8.ToArray());
    }

    [Fact]
    public void ToJsonPath_Root_ReturnsDollar()
    {
        NdJsonPath.Root.ToJsonPath().Should().Be("$");
    }

    [Fact]
    public void ToJsonPath_SingleProperty()
    {
        NdJsonPath path = NdJsonPath.At("messages");
        path.ToJsonPath().Should().Be("$.messages");
    }

    [Fact]
    public void ToJsonPath_NestedWithEach()
    {
        NdJsonPath path = NdJsonPath.At("response").Each().Key("messages");
        path.ToJsonPath().Should().Be("$.response[*].messages");
    }

    [Fact]
    public void Parse_Empty_ReturnsRoot()
    {
        NdJsonPath.Parse("").Segments.Length.Should().Be(0);
    }

    [Fact]
    public void Parse_DollarOnly_ReturnsRoot()
    {
        NdJsonPath.Parse("$").Segments.Length.Should().Be(0);
    }

    [Fact]
    public void Parse_SimpleProperty()
    {
        var path = NdJsonPath.Parse("$.messages");
        path.Segments.Length.Should().Be(1);
        path.Segments[0].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("messages"));
    }

    [Fact]
    public void Parse_NestedPath()
    {
        NdJsonPath.Parse("$.response.data.items").Segments.Length.Should().Be(3);
    }

    [Fact]
    public void Parse_WithWildcard()
    {
        var path = NdJsonPath.Parse("$.response[*].messages");
        path.Segments.Length.Should().Be(3);
        path.Segments[0].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("response"));
        path.Segments[1].Length.Should().Be(0); // wildcard
        path.Segments[2].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("messages"));
    }

    [Fact]
    public void Roundtrip_BuilderToJsonPathAndBack()
    {
        NdJsonPath original = NdJsonPath.At("response").Each().Key("items");
        var jsonPathStr = original.ToJsonPath();
        var parsed = NdJsonPath.Parse(jsonPathStr);
        parsed.ToJsonPath().Should().Be(jsonPathStr);
    }

    [Fact]
    public void ToString_MatchesToJsonPath()
    {
        NdJsonPath path = NdJsonPath.At("data");
        path.ToString().Should().Be(path.ToJsonPath());
    }

    [Fact]
    public void Immutable_BuilderDoesNotMutate()
    {
        var builder = NdJsonPath.At("x");
        var extended = builder.Key("y");

        NdJsonPath pathA = builder;
        NdJsonPath pathB = extended;

        pathA.Segments.Length.Should().Be(1);
        pathB.Segments.Length.Should().Be(2);
    }
}
```

- [ ] **Step 6: Build and run path tests**

Run: `dotnet test --filter "FullyQualifiedName~JsonPathTests"`
Expected: All path tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/JsonStreaming/NdJsonPath.cs tests/JsonStreaming.Tests/JsonPathTests.cs
git commit -m "feat: expand NdJsonPath with Parse(), Property(), ToJsonPath()"
```

---

## Task 2: Add NdJsonPath-to-JsonPath Bridge in JsonPathNavigator

Before deleting the old code, we need the old `JsonStreamReader` to work with `NdJsonPath`. Add a conversion method.

**Files:**
- Modify: `src/JsonStreaming/JsonPathNavigator.cs`

- [ ] **Step 1: Add ToJsonPath(NdJsonPath) conversion**

Add a static method that converts `NdJsonPath` segments to `JsonPath`:

```csharp
internal static JsonPath ToLegacyPath(NdJsonPath path)
{
    var result = JsonPath.Root;
    foreach (var seg in path.Segments)
    {
        if (seg.Length == 0)
            result = result.Each();
        else
            result = result.Property(seg);
    }
    return result;
}
```

- [ ] **Step 2: Build to verify**

Run: `dotnet build`
Expected: Build succeeded.

- [ ] **Step 3: Commit**

```bash
git add src/JsonStreaming/JsonPathNavigator.cs
git commit -m "refactor: add NdJsonPath-to-JsonPath bridge in navigator"
```

---

## Task 3: Migrate JsonStreamReader to NdJsonPath

**Files:**
- Modify: `src/JsonStreaming/JsonStreamReader.cs`
- Modify: `tests/JsonStreaming.Tests/JsonStreamReaderTests.cs`

- [ ] **Step 1: Change all public JsonPath parameters to NdJsonPath**

In `JsonStreamReader`, change every public method signature from `JsonPath path` to `NdJsonPath path`. Internally, convert via `JsonPathNavigator.ToLegacyPath(path)` at the entry point. The `string path` overloads change from `JsonPathNavigator.ParseDotPath(path)` to `NdJsonPath.Parse("$." + path)` (or keep the dot-path semantics).

For `string path` overloads, convert the dot-separated path to NdJsonPath:

```csharp
private static NdJsonPath ParseDotPath(string path)
{
    if (string.IsNullOrEmpty(path))
        return NdJsonPath.Root;

    var builder = new NdJsonPath.Builder();
    foreach (var segment in path.Split('.'))
        builder.Key(segment);
    return builder;
}
```

Change each public method that takes `JsonPath path` → `NdJsonPath path`, and at the call to internal methods, convert: `var legacyPath = JsonPathNavigator.ToLegacyPath(path);`

- [ ] **Step 2: Update JsonStreamReaderTests**

Replace all `JsonPath.Root.Property("x"u8)` with `NdJsonPath.At("x")`, etc. The key mappings:
- `JsonPath.Root` → `NdJsonPath.Root`
- `.Property("name"u8)` → `.Key("name")` or `.Property("name"u8)`
- `.Each()` → `.Each()`
- `JsonPath.Parse("$.x")` → `NdJsonPath.Parse("$.x")`

- [ ] **Step 3: Build and run tests**

Run: `dotnet test --filter "FullyQualifiedName~JsonStreamReaderTests"`
Expected: All stream reader tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/JsonStreaming/JsonStreamReader.cs tests/JsonStreaming.Tests/JsonStreamReaderTests.cs
git commit -m "refactor: migrate JsonStreamReader to NdJsonPath"
```

---

## Task 4: Migrate JsonStreamReaderTyped and JsonStreamPipeline

**Files:**
- Modify: `src/JsonStreaming/JsonStreamReaderTyped.cs`
- Modify: `src/JsonStreaming/JsonStreamPipeline.cs`
- Modify: `tests/JsonStreaming.Tests/TypedApiTests.cs`
- Modify: `tests/JsonStreaming.Tests/JsonStreamPipelineTests.cs`

- [ ] **Step 1: Update JsonStreamReaderTyped**

Change all `JsonPath path` parameters to `NdJsonPath path`. Replace `JsonPathNavigator.ParseDotPath(path)` calls with the `ParseDotPath` helper (or inline `NdJsonPath.Parse`). All methods delegate to `JsonStreamReader` which now accepts `NdJsonPath`.

- [ ] **Step 2: Update JsonStreamPipeline**

Same changes. Replace `JsonPathNavigator.ParseDotPath(sourcePath)` with `NdJsonPath.Parse("$." + sourcePath)` or a local helper. Change `JsonPath sourcePath` parameters to `NdJsonPath sourcePath`.

- [ ] **Step 3: Update TypedApiTests and JsonStreamPipelineTests**

Replace all `JsonPath` references with `NdJsonPath`.

- [ ] **Step 4: Build and run all tests**

Run: `dotnet test`
Expected: All 91 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/JsonStreaming/JsonStreamReaderTyped.cs src/JsonStreaming/JsonStreamPipeline.cs tests/JsonStreaming.Tests/TypedApiTests.cs tests/JsonStreaming.Tests/JsonStreamPipelineTests.cs
git commit -m "refactor: migrate JsonStreamReaderTyped and JsonStreamPipeline to NdJsonPath"
```

---

## Task 5: Update Sample Apps

**Files:**
- Modify: `samples/WebApiSample/Program.cs`
- Modify: `samples/ConsoleProfiler/Program.cs`

- [ ] **Step 1: Update WebApiSample**

Replace all `JsonPath` usage:
- `JsonPath.Root` → `NdJsonPath.Root`
- `JsonPath.Root.Property("products"u8)` → `NdJsonPath.At("products")`
- `JsonPath.Root.Property("data"u8).Property("pages"u8).Each().Property("todos"u8)` → `NdJsonPath.At("data").Key("pages").Each().Key("todos")`
- `JsonPath.Parse("$.products")` → `NdJsonPath.Parse("$.products")`

- [ ] **Step 2: Update ConsoleProfiler**

Same replacements.

- [ ] **Step 3: Build samples**

Run: `dotnet build samples/WebApiSample/ && dotnet build samples/ConsoleProfiler/`
Expected: Both build successfully.

- [ ] **Step 4: Commit**

```bash
git add samples/
git commit -m "refactor: migrate sample apps to NdJsonPath"
```

---

## Task 6: Delete JsonPath.cs and JsonPathNavigator.cs

**Files:**
- Delete: `src/JsonStreaming/JsonPath.cs`
- Modify: `src/JsonStreaming/JsonStreamReader.cs` — inline the navigator logic or keep a minimal internal helper

- [ ] **Step 1: Move necessary navigator logic into JsonStreamReader**

`JsonStreamReader` still uses `JsonPathNavigator.NavigateToArrayAsync`, `SplitAtEach`, `HasEach`, and `ExtractPropertyNames` internally. Move these as `private static` methods inside `JsonStreamReader`. They now operate on `NdJsonPath` directly (no legacy conversion needed since the navigator logic works with `byte[][]` property names — which is what `NdJsonPath.Segments` already is).

Simplify `NavigateToArrayAsync` to accept `byte[][] segments` directly instead of going through `JsonPath`.

`HasEach` becomes:
```csharp
private static bool HasEach(NdJsonPath path) =>
    path.Segments.Any(s => s.Length == 0);
```

`SplitAtEach` becomes:
```csharp
private static (byte[][] Prefix, byte[][] Suffix) SplitAtEach(NdJsonPath path)
{
    var segments = path.Segments;
    int eachIndex = Array.FindIndex(segments, s => s.Length == 0);
    if (eachIndex < 0)
        return (segments, []);

    var prefix = segments[..eachIndex];
    var suffix = segments[(eachIndex + 1)..];
    return (prefix, suffix);
}
```

`ExtractPropertyNames` is no longer needed — `NdJsonPath.Segments` IS already `byte[][]`.

- [ ] **Step 2: Delete JsonPath.cs**

```bash
git rm src/JsonStreaming/JsonPath.cs
```

- [ ] **Step 3: Delete JsonPathNavigator.cs**

```bash
git rm src/JsonStreaming/JsonPathNavigator.cs
```

- [ ] **Step 4: Remove SegmentKind enum references**

`SegmentKind` and `Segment` lived in `JsonPath.cs`. After deletion, ensure no references remain. The only consumer was `JsonPathNavigator` (also deleted) and the test file (already rewritten).

- [ ] **Step 5: Build and run all tests**

Run: `dotnet build && dotnet test`
Expected: Build succeeded, all 91 tests pass.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: delete JsonPath.cs and JsonPathNavigator.cs — NdJsonPath is the single path type"
```

---

## Task 7: Final Verification

- [ ] **Step 1: Run full test suite**

Run: `dotnet test`
Expected: 91 tests pass.

- [ ] **Step 2: Build all projects**

Run: `dotnet build`
Expected: 0 errors across all targets.

- [ ] **Step 3: Grep for any leftover JsonPath references**

Run: `grep -r "JsonPath\b" src/ samples/ tests/ --include="*.cs" | grep -v NdJsonPath | grep -v "ToJsonPath\|jsonPath\|jsonPathStr"`
Expected: No matches (all references are either NdJsonPath or the ToJsonPath() method).

- [ ] **Step 4: Commit any cleanup**

```bash
git add -A
git commit -m "refactor: path unification complete — NdJsonPath is the single path type"
```
