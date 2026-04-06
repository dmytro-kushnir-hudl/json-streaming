using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace JsonStreaming;

/// <summary>
/// Incrementally reads a JSON stream, navigates to nested arrays via <see cref="JsonPath"/>,
/// and yields each array element with bounded memory (~8KB working set).
///
/// Two consumption modes:
/// <list type="bullet">
///   <item><c>EnumerateArrayAsync</c> — yields <see cref="JsonDocument"/> per item</item>
///   <item><c>ProcessArrayAsync</c> — invokes a callback with raw bytes per item (zero-copy)</item>
/// </list>
///
/// Supports <see cref="SegmentKind.Each"/> for select-many: <c>$.responses[*].messages</c>
/// iterates each response and yields all messages across all of them.
/// </summary>
public static class JsonStreamReader
{
    // ── IAsyncEnumerable API ───────────────────────────────────────────────

    /// <summary>
    /// Navigates to the target array(s) and yields each element as a <see cref="JsonDocument"/>.
    /// The caller must dispose each document.
    /// </summary>
    public static IAsyncEnumerable<JsonDocument> EnumerateArrayAsync(
        PipeReader pipeReader,
        JsonPath path,
        CancellationToken ct = default
    ) =>
        HasEach(path)
            ? EnumerateSelectManyCoreAsync(pipeReader, path, ct)
            : EnumerateArrayCoreAsync(pipeReader, path, ct);

    /// <summary>
    /// Convenience overload accepting a dot-separated path string.
    /// </summary>
    public static IAsyncEnumerable<JsonDocument> EnumerateArrayAsync(
        PipeReader pipeReader,
        string path,
        CancellationToken ct = default
    ) => EnumerateArrayCoreAsync(pipeReader, ParseDotPath(path), ct);

    // ── Callback API (zero-copy) ───────────────────────────────────────────

    /// <summary>
    /// Navigates to the target array(s) and invokes <paramref name="processItem"/> for each
    /// element's raw bytes. The <see cref="ReadOnlySequence{T}"/> is only valid during
    /// the callback. Returns the number of items processed.
    /// </summary>
    public static Task<int> ProcessArrayAsync(
        PipeReader pipeReader,
        JsonPath path,
        Action<ReadOnlySequence<byte>> processItem,
        CancellationToken ct = default
    ) =>
        HasEach(path)
            ? ProcessSelectManyCoreAsync(pipeReader, path, processItem, ct)
            : ProcessArrayCoreAsync(pipeReader, path, processItem, ct);

    /// <summary>
    /// Convenience overload accepting a dot-separated path string.
    /// </summary>
    public static Task<int> ProcessArrayAsync(
        PipeReader pipeReader,
        string path,
        Action<ReadOnlySequence<byte>> processItem,
        CancellationToken ct = default
    ) => ProcessArrayCoreAsync(pipeReader, ParseDotPath(path), processItem, ct);

    // ── Simple path (no Each) ──────────────────────────────────────────────

    private static async IAsyncEnumerable<JsonDocument> EnumerateArrayCoreAsync(
        PipeReader pipeReader,
        JsonPath path,
        [EnumeratorCancellation] CancellationToken ct = default
    )
    {
        var navState = await NavigateToArrayAsync(pipeReader, path, ct);
        if (navState is null)
            yield break;

        await foreach (var doc in YieldItemsAsDocumentsAsync(pipeReader, navState.Value, ct))
            yield return doc;
    }

    private static async Task<int> ProcessArrayCoreAsync(
        PipeReader pipeReader,
        JsonPath path,
        Action<ReadOnlySequence<byte>> processItem,
        CancellationToken ct
    )
    {
        var navState = await NavigateToArrayAsync(pipeReader, path, ct);
        if (navState is null)
            return 0;

        return await IterateItemsAsync(pipeReader, navState.Value, processItem, ct);
    }

    // ── Select-many (Each) ─────────────────────────────────────────────────
    //
    // Path: $.responses[*].messages
    //   prefix = ["responses"]  →  navigate to outer array
    //   suffix = ["messages"]   →  for each element, navigate to inner array
    //
    // Path: $.items[*]
    //   prefix = ["items"]  →  navigate to outer array
    //   suffix = []         →  yield each outer element directly

    private static (JsonPath prefix, JsonPath suffix) SplitAtEach(JsonPath path)
    {
        var segments = path.Segments;
        var prefix = JsonPath.Root;
        int i = 0;

        // Collect segments before the first Each()
        while (i < segments.Length && segments[i].Kind != SegmentKind.Each)
        {
            prefix = prefix.Property(segments[i].Name.Span);
            i++;
        }

        // Skip the Each() itself
        if (i < segments.Length)
            i++;

        // Remaining segments are the suffix
        var suffix = JsonPath.Root;
        while (i < segments.Length)
        {
            suffix = suffix.Property(segments[i].Name.Span);
            i++;
        }

        return (prefix, suffix);
    }

    private static async Task<int> ProcessSelectManyCoreAsync(
        PipeReader pipeReader,
        JsonPath path,
        Action<ReadOnlySequence<byte>> processItem,
        CancellationToken ct
    )
    {
        var (prefix, suffix) = SplitAtEach(path);
        var suffixNames = ExtractPropertyNames(suffix);

        // Navigate prefix to reach the outer array
        var outerState = await NavigateToArrayAsync(pipeReader, prefix, ct);
        if (outerState is null)
            return 0;

        if (suffixNames.Length == 0)
        {
            // Each() at end: yield outer elements directly
            return await IterateItemsAsync(pipeReader, outerState.Value, processItem, ct);
        }

        // Each() with suffix: for each outer element, find inner array and yield its items
        return await IterateSelectManyAsync(
            pipeReader,
            outerState.Value,
            suffixNames,
            processItem,
            ct
        );
    }

    private static async IAsyncEnumerable<JsonDocument> EnumerateSelectManyCoreAsync(
        PipeReader pipeReader,
        JsonPath path,
        [EnumeratorCancellation] CancellationToken ct = default
    )
    {
        var (prefix, suffix) = SplitAtEach(path);
        var suffixNames = ExtractPropertyNames(suffix);

        var outerState = await NavigateToArrayAsync(pipeReader, prefix, ct);
        if (outerState is null)
            yield break;

        if (suffixNames.Length == 0)
        {
            await foreach (var doc in YieldItemsAsDocumentsAsync(pipeReader, outerState.Value, ct))
                yield return doc;
            yield break;
        }

        // Each() with suffix — unified state machine yields JsonDocuments
        await foreach (
            var doc in IterateSelectManyDocumentsAsync(
                pipeReader,
                outerState.Value,
                suffixNames,
                ct
            )
        )
            yield return doc;
    }

    // ── Select-many state machine ──────────────────────────────────────────
    //
    // Operates on the PipeReader positioned just after the outer array's StartArray.
    // For each element in the outer array:
    //   1. Expect StartObject
    //   2. Navigate suffix properties to find inner array
    //   3. Yield inner array items
    //   4. Skip rest of outer element (depth-tracked)
    //   5. Loop back for next outer element
    // When outer EndArray is found: done.

    private enum EachPhase
    {
        InOuterArray, // Expect StartObject or EndArray
        NavSuffixSearch, // Inside element, searching for suffix property
        NavSuffixExpectObj, // Found non-terminal suffix prop, expect StartObject
        NavSuffixSkip, // Skip non-matching property value
        ExpectInnerArray, // Found terminal suffix prop, expect StartArray
        InInnerArray, // Yielding inner items (handled by caller)
        SkipElement, // Skip remaining content of outer element
        Done,
    }

    private static async Task<int> IterateSelectManyAsync(
        PipeReader pipeReader,
        JsonReaderState initialState,
        byte[][] suffixNames,
        Action<ReadOnlySequence<byte>> processItem,
        CancellationToken ct
    )
    {
        var jsonState = initialState;
        var phase = EachPhase.InOuterArray;
        var returnPhase = EachPhase.InOuterArray;
        int suffixIndex = 0;
        int skipDepth = 0;
        int elementDepth = 0; // depth relative to outer array
        int count = 0;

        while (phase != EachPhase.Done)
        {
            ct.ThrowIfCancellationRequested();

            var readResult = await pipeReader.ReadAsync(ct);
            var buffer = readResult.Buffer;
            var reader = new Utf8JsonReader(
                buffer,
                isFinalBlock: readResult.IsCompleted,
                jsonState
            );

            while (reader.Read())
            {
                // ── Skipping (depth-tracked) ──────────────────────
                if (phase is EachPhase.NavSuffixSkip or EachPhase.SkipElement)
                {
                    switch (reader.TokenType)
                    {
                        case JsonTokenType.StartObject:
                        case JsonTokenType.StartArray:
                            skipDepth++;
                            break;
                        case JsonTokenType.EndObject:
                        case JsonTokenType.EndArray:
                            if (--skipDepth <= 0)
                            {
                                skipDepth = 0;
                                if (phase == EachPhase.SkipElement)
                                {
                                    elementDepth--;
                                    if (elementDepth <= 0)
                                    {
                                        // Back at outer array level
                                        phase = EachPhase.InOuterArray;
                                        suffixIndex = 0;
                                    }
                                }
                                else
                                {
                                    phase = returnPhase;
                                }
                            }
                            else if (phase == EachPhase.SkipElement)
                            {
                                // Track element depth separately
                                elementDepth--;
                                if (elementDepth <= 0)
                                {
                                    skipDepth = 0;
                                    phase = EachPhase.InOuterArray;
                                    suffixIndex = 0;
                                }
                            }
                            break;
                        default:
                            if (skipDepth == 0)
                            {
                                if (phase == EachPhase.SkipElement)
                                {
                                    // Primitive value at element level — shouldn't happen normally
                                    phase = EachPhase.InOuterArray;
                                    suffixIndex = 0;
                                }
                                else
                                {
                                    phase = returnPhase;
                                }
                            }
                            break;
                    }
                    continue;
                }

                // ── Main phases ───────────────────────────────────
                switch (phase)
                {
                    case EachPhase.InOuterArray:
                        if (reader.TokenType == JsonTokenType.EndArray)
                        {
                            phase = EachPhase.Done;
                        }
                        else if (reader.TokenType == JsonTokenType.StartObject)
                        {
                            suffixIndex = 0;
                            elementDepth = 1;
                            phase = EachPhase.NavSuffixSearch;
                        }
                        else
                        {
                            // Non-object element in outer array — skip it
                            // (primitive or array — can't navigate suffix into it)
                        }
                        break;

                    case EachPhase.NavSuffixSearch:
                        if (reader.TokenType == JsonTokenType.PropertyName)
                        {
                            if (reader.ValueTextEquals(suffixNames[suffixIndex]))
                            {
                                if (suffixIndex == suffixNames.Length - 1)
                                    phase = EachPhase.ExpectInnerArray;
                                else
                                {
                                    suffixIndex++;
                                    phase = EachPhase.NavSuffixExpectObj;
                                }
                            }
                            else
                            {
                                phase = EachPhase.NavSuffixSkip;
                                returnPhase = EachPhase.NavSuffixSearch;
                                skipDepth = 0;
                            }
                        }
                        else if (reader.TokenType == JsonTokenType.EndObject)
                        {
                            // Element doesn't have suffix path — skip to next
                            elementDepth--;
                            if (elementDepth <= 0)
                            {
                                phase = EachPhase.InOuterArray;
                                suffixIndex = 0;
                            }
                        }
                        break;

                    case EachPhase.NavSuffixExpectObj:
                        if (reader.TokenType == JsonTokenType.StartObject)
                        {
                            elementDepth++;
                            phase = EachPhase.NavSuffixSearch;
                        }
                        else
                        {
                            // Not an object — can't navigate deeper, skip element
                            phase = EachPhase.SkipElement;
                            skipDepth = 0;
                        }
                        break;

                    case EachPhase.ExpectInnerArray:
                        if (reader.TokenType == JsonTokenType.StartArray)
                        {
                            phase = EachPhase.InInnerArray;
                        }
                        else
                        {
                            // Not an array — skip rest of element
                            phase = EachPhase.SkipElement;
                            skipDepth = 0;
                        }
                        break;

                    case EachPhase.InInnerArray:
                        if (reader.TokenType == JsonTokenType.EndArray)
                        {
                            // Inner array done — skip rest of outer element
                            phase = EachPhase.SkipElement;
                            skipDepth = 0;
                        }
                        else
                        {
                            // Yield this item
                            long itemStart = reader.TokenStartIndex;
                            if (reader.TrySkip())
                            {
                                long itemLength = reader.BytesConsumed - itemStart;
                                var itemSlice = buffer.Slice(
                                    buffer.GetPosition(itemStart),
                                    itemLength
                                );
                                processItem(itemSlice);
                                count++;
                            }
                            else
                            {
                                // Incomplete — need more data. Save state and break inner loop.
                                jsonState = reader.CurrentState;
                                pipeReader.AdvanceTo(buffer.GetPosition(itemStart), buffer.End);
                                goto continueOuter;
                            }
                        }
                        break;
                }

                if (phase == EachPhase.Done)
                    break;
            }

            jsonState = reader.CurrentState;
            pipeReader.AdvanceTo(reader.Position, buffer.End);

            continueOuter:
            if (readResult.IsCompleted && phase != EachPhase.Done)
                break;
        }

        return count;
    }

    private static async IAsyncEnumerable<JsonDocument> IterateSelectManyDocumentsAsync(
        PipeReader pipeReader,
        JsonReaderState initialState,
        byte[][] suffixNames,
        [EnumeratorCancellation] CancellationToken ct = default
    )
    {
        // Reuse the callback-based implementation, collecting items into a queue
        // per buffer read to maintain streaming semantics.
        // This allocates one JsonDocument per item (same as EnumerateArrayCoreAsync).
        var queue = new Queue<JsonDocument>();
        await IterateSelectManyAsync(
            pipeReader,
            initialState,
            suffixNames,
            itemBytes =>
            {
                queue.Enqueue(JsonDocument.Parse(itemBytes));
            },
            ct
        );

        while (queue.Count > 0)
            yield return queue.Dequeue();
    }

    // ── Simple item iteration ──────────────────────────────────────────────

    private static async Task<int> IterateItemsAsync(
        PipeReader pipeReader,
        JsonReaderState initialState,
        Action<ReadOnlySequence<byte>> processItem,
        CancellationToken ct
    )
    {
        var jsonState = initialState;
        int count = 0;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var readResult = await pipeReader.ReadAsync(ct);
            var buffer = readResult.Buffer;
            var reader = new Utf8JsonReader(
                buffer,
                isFinalBlock: readResult.IsCompleted,
                jsonState
            );

            if (!reader.Read())
            {
                pipeReader.AdvanceTo(buffer.Start, buffer.End);
                if (readResult.IsCompleted)
                    break;
                continue;
            }

            if (reader.TokenType == JsonTokenType.EndArray)
            {
                pipeReader.AdvanceTo(reader.Position);
                break;
            }

            long itemStart = reader.TokenStartIndex;

            if (reader.TrySkip())
            {
                long itemLength = reader.BytesConsumed - itemStart;
                var itemSlice = buffer.Slice(buffer.GetPosition(itemStart), itemLength);

                processItem(itemSlice);

                jsonState = reader.CurrentState;
                pipeReader.AdvanceTo(reader.Position);
                count++;
            }
            else if (readResult.IsCompleted)
            {
                pipeReader.AdvanceTo(buffer.End);
                break;
            }
            else
            {
                pipeReader.AdvanceTo(buffer.Start, buffer.End);
            }
        }

        return count;
    }

    private static async IAsyncEnumerable<JsonDocument> YieldItemsAsDocumentsAsync(
        PipeReader pipeReader,
        JsonReaderState initialState,
        [EnumeratorCancellation] CancellationToken ct = default
    )
    {
        var jsonState = initialState;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var readResult = await pipeReader.ReadAsync(ct);
            var buffer = readResult.Buffer;
            var reader = new Utf8JsonReader(
                buffer,
                isFinalBlock: readResult.IsCompleted,
                jsonState
            );

            if (!reader.Read())
            {
                pipeReader.AdvanceTo(buffer.Start, buffer.End);
                if (readResult.IsCompleted)
                    yield break;
                continue;
            }

            if (reader.TokenType == JsonTokenType.EndArray)
            {
                pipeReader.AdvanceTo(reader.Position);
                yield break;
            }

            long itemStart = reader.TokenStartIndex;

            if (reader.TrySkip())
            {
                long itemLength = reader.BytesConsumed - itemStart;
                var itemSlice = buffer.Slice(buffer.GetPosition(itemStart), itemLength);
                var doc = JsonDocument.Parse(itemSlice);

                jsonState = reader.CurrentState;
                pipeReader.AdvanceTo(reader.Position);
                yield return doc;
            }
            else if (readResult.IsCompleted)
            {
                pipeReader.AdvanceTo(buffer.End);
                yield break;
            }
            else
            {
                pipeReader.AdvanceTo(buffer.Start, buffer.End);
            }
        }
    }

    // ── Navigation ─────────────────────────────────────────────────────────

    private enum NavPhase
    {
        SearchProperty,
        ExpectObject,
        ExpectArray,
        Skipping,
    }

    private static async Task<JsonReaderState?> NavigateToArrayAsync(
        PipeReader pipeReader,
        JsonPath path,
        CancellationToken ct
    )
    {
        var segmentsSpan = path.Segments;
        if (segmentsSpan.IsEmpty)
            return await SkipToRootArrayAsync(pipeReader, ct);

        var propertyNames = ExtractPropertyNames(path);

        var jsonState = new JsonReaderState();
        var phase = NavPhase.SearchProperty;
        var returnPhase = NavPhase.SearchProperty;
        int segmentIndex = 0;
        int skipDepth = 0;

        while (true)
        {
            var readResult = await pipeReader.ReadAsync(ct);
            var buffer = readResult.Buffer;
            var reader = new Utf8JsonReader(
                buffer,
                isFinalBlock: readResult.IsCompleted,
                jsonState
            );

            while (reader.Read())
            {
                if (phase == NavPhase.Skipping)
                {
                    switch (reader.TokenType)
                    {
                        case JsonTokenType.StartObject:
                        case JsonTokenType.StartArray:
                            skipDepth++;
                            break;
                        case JsonTokenType.EndObject:
                        case JsonTokenType.EndArray:
                            if (--skipDepth <= 0)
                            {
                                skipDepth = 0;
                                phase = returnPhase;
                            }
                            break;
                        default:
                            if (skipDepth == 0)
                                phase = returnPhase;
                            break;
                    }
                    continue;
                }

                switch (phase)
                {
                    case NavPhase.SearchProperty:
                        if (reader.TokenType == JsonTokenType.PropertyName)
                        {
                            if (reader.ValueTextEquals(propertyNames[segmentIndex]))
                            {
                                if (segmentIndex == propertyNames.Length - 1)
                                    phase = NavPhase.ExpectArray;
                                else
                                {
                                    segmentIndex++;
                                    phase = NavPhase.ExpectObject;
                                }
                            }
                            else
                            {
                                phase = NavPhase.Skipping;
                                returnPhase = NavPhase.SearchProperty;
                                skipDepth = 0;
                            }
                        }
                        else if (reader.TokenType == JsonTokenType.EndObject)
                        {
                            pipeReader.AdvanceTo(reader.Position);
                            return null;
                        }
                        break;

                    case NavPhase.ExpectObject:
                        if (reader.TokenType == JsonTokenType.StartObject)
                            phase = NavPhase.SearchProperty;
                        else
                        {
                            pipeReader.AdvanceTo(reader.Position);
                            return null;
                        }
                        break;

                    case NavPhase.ExpectArray:
                        if (reader.TokenType == JsonTokenType.StartArray)
                        {
                            jsonState = reader.CurrentState;
                            pipeReader.AdvanceTo(reader.Position, buffer.End);
                            return jsonState;
                        }
                        else
                        {
                            pipeReader.AdvanceTo(reader.Position);
                            return null;
                        }
                }

            }

            jsonState = reader.CurrentState;
            pipeReader.AdvanceTo(reader.Position, buffer.End);

            if (readResult.IsCompleted)
                return null;
        }
    }

    private static async Task<JsonReaderState?> SkipToRootArrayAsync(
        PipeReader pipeReader,
        CancellationToken ct
    )
    {
        var jsonState = new JsonReaderState();

        while (true)
        {
            var readResult = await pipeReader.ReadAsync(ct);
            var buffer = readResult.Buffer;
            var reader = new Utf8JsonReader(
                buffer,
                isFinalBlock: readResult.IsCompleted,
                jsonState
            );

            if (reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                jsonState = reader.CurrentState;
                pipeReader.AdvanceTo(reader.Position, buffer.End);
                return jsonState;
            }

            jsonState = reader.CurrentState;
            pipeReader.AdvanceTo(reader.Position, buffer.End);

            if (readResult.IsCompleted)
                return null;
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static bool HasEach(JsonPath path)
    {
        var segments = path.Segments;
        for (int i = 0; i < segments.Length; i++)
        {
            if (segments[i].Kind == SegmentKind.Each)
                return true;
        }
        return false;
    }

    private static byte[][] ExtractPropertyNames(JsonPath path)
    {
        var segments = path.Segments;
        var names = new byte[segments.Length][];
        for (int i = 0; i < segments.Length; i++)
            names[i] = segments[i].Name.ToArray();
        return names;
    }

    private static JsonPath ParseDotPath(string path)
    {
        if (string.IsNullOrEmpty(path))
            return JsonPath.Root;

        var result = JsonPath.Root;
        foreach (var segment in path.Split('.'))
            result = result.Property(System.Text.Encoding.UTF8.GetBytes(segment));
        return result;
    }
}
