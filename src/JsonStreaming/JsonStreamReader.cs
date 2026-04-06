using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;

namespace JsonStreaming;

/// <summary>
/// Bounded-memory streaming JSON array processor. Navigates to nested arrays
/// via <see cref="JsonPath"/>, invokes a callback per item with zero-copy byte access.
///
/// Three overloads:
/// <list type="bullet">
///   <item>Callback with raw bytes — zero-copy, caller parses as needed</item>
///   <item>Write-through — reads items from input, writes directly to <see cref="Utf8JsonWriter"/> output</item>
///   <item>Write-through with transform — caller controls what gets written per item</item>
/// </list>
///
/// Write-through methods flush automatically when the writer's pending bytes
/// exceed a threshold (90% of <see cref="WriteOptions.FlushThreshold"/>),
/// matching <c>System.Text.Json</c>'s backpressure strategy.
///
/// Supports <see cref="SegmentKind.Each"/> for select-many: <c>$.responses[*].messages</c>
/// iterates each response and yields all messages across all of them.
/// </summary>
public static class JsonStreamReader
{
    private const string IncompleteJsonMessage = "Incomplete or malformed JSON while streaming array items.";

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
        JsonPathNavigator.HasEach(path)
            ? ProcessSelectManyAsync(pipeReader, path, processItem, ct)
            : ProcessSimpleAsync(pipeReader, path, processItem, ct);

    /// <summary>
    /// Convenience overload accepting a dot-separated path string.
    /// </summary>
    public static Task<int> ProcessArrayAsync(
        PipeReader pipeReader,
        string path,
        Action<ReadOnlySequence<byte>> processItem,
        CancellationToken ct = default
    ) => ProcessSimpleAsync(pipeReader, JsonPathNavigator.ParseDotPath(path), processItem, ct);

    // ── Write-through API (PipeReader → Utf8JsonWriter) ────────────────────

    /// <summary>
    /// Navigates to the target array(s) and writes each item verbatim to
    /// <paramref name="writer"/>. The writer must already be inside an array
    /// (caller writes StartArray/EndArray). Returns the number of items written.
    ///
    /// Flushes to the underlying <see cref="PipeWriter"/> when buffered bytes
    /// exceed the threshold, providing automatic backpressure for HTTP streaming.
    /// Pass <c>new Utf8JsonWriter(httpContext.Response.BodyWriter)</c> as the writer.
    /// </summary>
    public static Task<int> WriteArrayAsync(
        PipeReader pipeReader,
        JsonPath path,
        Utf8JsonWriter writer,
        CancellationToken ct = default
    ) => WriteArrayAsync(pipeReader, path, writer, WriteOptions.Default, ct);

    /// <summary>
    /// Write-through with explicit options controlling flush behavior.
    /// </summary>
    public static Task<int> WriteArrayAsync(
        PipeReader pipeReader,
        JsonPath path,
        Utf8JsonWriter writer,
        WriteOptions options,
        CancellationToken ct = default
    ) =>
        WriteArrayCoreAsync(
            pipeReader,
            path,
            writer,
            static (itemBytes, w) =>
            {
                if (itemBytes.IsSingleSegment)
                    w.WriteRawValue(itemBytes.FirstSpan, skipInputValidation: true);
                else
                    w.WriteRawValue(itemBytes.ToArray(), skipInputValidation: true);
            },
            options,
            ct
        );

    /// <summary>
    /// Convenience overload accepting a dot-separated path string.
    /// </summary>
    public static Task<int> WriteArrayAsync(
        PipeReader pipeReader,
        string path,
        Utf8JsonWriter writer,
        CancellationToken ct = default
    ) => WriteArrayAsync(pipeReader, JsonPathNavigator.ParseDotPath(path), writer, ct);

    /// <summary>
    /// Convenience overload accepting a dot-separated path string with explicit options.
    /// </summary>
    public static Task<int> WriteArrayAsync(
        PipeReader pipeReader,
        string path,
        Utf8JsonWriter writer,
        WriteOptions options,
        CancellationToken ct = default
    ) => WriteArrayAsync(pipeReader, JsonPathNavigator.ParseDotPath(path), writer, options, ct);

    /// <summary>
    /// Navigates to the target array(s) and invokes <paramref name="writeItem"/>
    /// for each item. The delegate receives raw item bytes and the output writer,
    /// enabling selective field copying, transformation, or filtering.
    ///
    /// Flushes automatically based on <see cref="WriteOptions.FlushThreshold"/>.
    /// </summary>
    public static Task<int> WriteArrayAsync(
        PipeReader pipeReader,
        JsonPath path,
        Utf8JsonWriter writer,
        WriteItemDelegate writeItem,
        CancellationToken ct = default
    ) => WriteArrayAsync(pipeReader, path, writer, writeItem, WriteOptions.Default, ct);

    /// <summary>
    /// Transform write-through with explicit options.
    /// </summary>
    public static Task<int> WriteArrayAsync(
        PipeReader pipeReader,
        JsonPath path,
        Utf8JsonWriter writer,
        WriteItemDelegate writeItem,
        WriteOptions options,
        CancellationToken ct = default
    ) => WriteArrayCoreAsync(pipeReader, path, writer, writeItem, options, ct);

    /// <summary>
    /// Convenience overload accepting a dot-separated path string.
    /// </summary>
    public static Task<int> WriteArrayAsync(
        PipeReader pipeReader,
        string path,
        Utf8JsonWriter writer,
        WriteItemDelegate writeItem,
        CancellationToken ct = default
    ) =>
        WriteArrayAsync(
            pipeReader,
            JsonPathNavigator.ParseDotPath(path),
            writer,
            writeItem,
            ct
        );

    /// <summary>
    /// Convenience overload: dot-separated path string + transform + explicit options.
    /// </summary>
    public static Task<int> WriteArrayAsync(
        PipeReader pipeReader,
        string path,
        Utf8JsonWriter writer,
        WriteItemDelegate writeItem,
        WriteOptions options,
        CancellationToken ct = default
    ) =>
        WriteArrayAsync(
            pipeReader,
            JsonPathNavigator.ParseDotPath(path),
            writer,
            writeItem,
            options,
            ct
        );

    // ── Write-through core (with flush) ────────────────────────────────────

    private static async Task<int> WriteArrayCoreAsync(
        PipeReader pipeReader,
        JsonPath path,
        Utf8JsonWriter writer,
        WriteItemDelegate writeItem,
        WriteOptions options,
        CancellationToken ct
    )
    {
        var flushThreshold = (long)(options.FlushThreshold * WriteOptions.FlushRatio);
        var asyncFlush = options.AsyncFlush;

        if (JsonPathNavigator.HasEach(path))
        {
            var (prefix, suffix) = JsonPathNavigator.SplitAtEach(path);
            var suffixNames = JsonPathNavigator.ExtractPropertyNames(suffix);

            var outerState = await JsonPathNavigator.NavigateToArrayAsync(pipeReader, prefix, ct);
            if (outerState is null)
                return 0;

            if (suffixNames.Length == 0)
            {
                return await IterateItemsWithFlushAsync(
                    pipeReader,
                    outerState.Value,
                    writer,
                    writeItem,
                    flushThreshold,
                    asyncFlush,
                    ct
                );
            }

            return await IterateSelectManyWithFlushAsync(
                pipeReader,
                outerState.Value,
                suffixNames,
                writer,
                writeItem,
                flushThreshold,
                asyncFlush,
                ct
            );
        }
        else
        {
            var navState = await JsonPathNavigator.NavigateToArrayAsync(pipeReader, path, ct);
            if (navState is null)
                return 0;

            return await IterateItemsWithFlushAsync(
                pipeReader,
                navState.Value,
                writer,
                writeItem,
                flushThreshold,
                asyncFlush,
                ct
            );
        }
    }

    // ── Simple iteration with flush ────────────────────────────────────────

    private static async Task<int> IterateItemsWithFlushAsync(
        PipeReader pipeReader,
        JsonReaderState initialState,
        Utf8JsonWriter writer,
        WriteItemDelegate writeItem,
        long flushThreshold,
        Func<CancellationToken, ValueTask>? asyncFlush,
        CancellationToken ct
    )
    {
        var jsonState = initialState;
        int count = 0;
        long lastFlushedAt = 0;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var readResult = await pipeReader.ReadAsync(ct);
            var buffer = readResult.Buffer;
            bool needsFlush = false;

            // Synchronous inner loop — Utf8JsonReader lives only in this scope
            {
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
                    var itemSlice = buffer.Slice(
                        buffer.GetPosition(itemStart),
                        itemLength
                    );

                    long bytesBeforeWrite = writer.BytesCommitted + writer.BytesPending;
                    writeItem(itemSlice, writer);
                    long bytesAfterWrite = writer.BytesCommitted + writer.BytesPending;
                    if (bytesAfterWrite > bytesBeforeWrite)
                        count++;

                    // Checkpoint: save state and advance before potential flush
                    jsonState = reader.CurrentState;
                    pipeReader.AdvanceTo(reader.Position, buffer.End);

                    // Check flush threshold
                    long totalWritten = writer.BytesCommitted + writer.BytesPending;
                    if (flushThreshold > 0 && (totalWritten - lastFlushedAt) >= flushThreshold)
                        needsFlush = true;
                }
                else if (readResult.IsCompleted)
                {
                    pipeReader.AdvanceTo(buffer.End);
                    throw new JsonException(IncompleteJsonMessage);
                }
                else
                {
                    pipeReader.AdvanceTo(buffer.Start, buffer.End);
                }
            }
            // reader is dead here — safe to await

            if (needsFlush)
            {
                writer.Flush();
                lastFlushedAt = writer.BytesCommitted;

                if (asyncFlush is not null)
                    await asyncFlush(ct);
            }
        }

        return count;
    }

    // ── Select-many iteration with flush ───────────────────────────────────

    private enum EachPhase
    {
        InOuterArray,
        NavSuffixSearch,
        NavSuffixExpectObj,
        NavSuffixSkip,
        ExpectInnerArray,
        InInnerArray,
        SkipElement,
        Done,
    }

    private static async Task<int> IterateSelectManyWithFlushAsync(
        PipeReader pipeReader,
        JsonReaderState initialState,
        byte[][] suffixNames,
        Utf8JsonWriter writer,
        WriteItemDelegate writeItem,
        long flushThreshold,
        Func<CancellationToken, ValueTask>? asyncFlush,
        CancellationToken ct
    )
    {
        var jsonState = initialState;
        var phase = EachPhase.InOuterArray;
        var returnPhase = EachPhase.InOuterArray;
        int suffixIndex = 0;
        int skipDepth = 0;
        int elementDepth = 0;
        int count = 0;
        long lastFlushedAt = 0;

        while (phase != EachPhase.Done)
        {
            ct.ThrowIfCancellationRequested();

            var readResult = await pipeReader.ReadAsync(ct);
            var buffer = readResult.Buffer;
            bool needsFlush = false;

            // Synchronous inner loop — reader lives only in this scope
            {
                var reader = new Utf8JsonReader(
                    buffer,
                    isFinalBlock: readResult.IsCompleted,
                    jsonState
                );

                while (reader.Read())
                {
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
                                phase = EachPhase.SkipElement;
                                skipDepth = 0;
                            }
                            break;

                        case EachPhase.InInnerArray:
                            if (reader.TokenType == JsonTokenType.EndArray)
                            {
                                phase = EachPhase.SkipElement;
                                skipDepth = 0;
                            }
                            else
                            {
                                long itemStart = reader.TokenStartIndex;
                                if (reader.TrySkip())
                                {
                                    long itemLength = reader.BytesConsumed - itemStart;
                                    var itemSlice = buffer.Slice(
                                        buffer.GetPosition(itemStart),
                                        itemLength
                                    );
                                    long bytesBeforeWrite =
                                        writer.BytesCommitted + writer.BytesPending;
                                    writeItem(itemSlice, writer);
                                    long bytesAfterWrite =
                                        writer.BytesCommitted + writer.BytesPending;
                                    if (bytesAfterWrite > bytesBeforeWrite)
                                        count++;

                                    // Check flush — checkpoint and break if needed
                                    long totalWritten =
                                        writer.BytesCommitted + writer.BytesPending;
                                    if (
                                        flushThreshold > 0
                                        && (totalWritten - lastFlushedAt) >= flushThreshold
                                    )
                                    {
                                        jsonState = reader.CurrentState;
                                        pipeReader.AdvanceTo(reader.Position, buffer.End);
                                        needsFlush = true;
                                        goto afterInnerLoop;
                                    }
                                }
                                else
                                {
                                    // Incomplete item — need more data
                                    jsonState = reader.CurrentState;
                                    pipeReader.AdvanceTo(
                                        buffer.GetPosition(itemStart),
                                        buffer.End
                                    );
                                    goto afterInnerLoop;
                                }
                            }
                            break;
                    }

                    if (phase == EachPhase.Done)
                        break;
                }

                // Normal exit from inner loop — save state
                jsonState = reader.CurrentState;
                pipeReader.AdvanceTo(reader.Position, buffer.End);
            }

            afterInnerLoop:
            // reader is dead here — safe to await

            if (needsFlush)
            {
                writer.Flush();
                lastFlushedAt = writer.BytesCommitted;

                if (asyncFlush is not null)
                    await asyncFlush(ct);

                // After flush, always continue — there may be more items
                // in the pipe even if the previous ReadResult.IsCompleted was true
                continue;
            }

            if (readResult.IsCompleted && phase != EachPhase.Done)
                throw new JsonException(IncompleteJsonMessage);
        }

        return count;
    }

    // ── Callback-only paths (no flush) ─────────────────────────────────────

    private static async Task<int> ProcessSimpleAsync(
        PipeReader pipeReader,
        JsonPath path,
        Action<ReadOnlySequence<byte>> processItem,
        CancellationToken ct
    )
    {
        var navState = await JsonPathNavigator.NavigateToArrayAsync(pipeReader, path, ct);
        if (navState is null)
            return 0;

        return await IterateItemsAsync(pipeReader, navState.Value, processItem, ct);
    }

    private static async Task<int> ProcessSelectManyAsync(
        PipeReader pipeReader,
        JsonPath path,
        Action<ReadOnlySequence<byte>> processItem,
        CancellationToken ct
    )
    {
        var (prefix, suffix) = JsonPathNavigator.SplitAtEach(path);
        var suffixNames = JsonPathNavigator.ExtractPropertyNames(suffix);

        var outerState = await JsonPathNavigator.NavigateToArrayAsync(pipeReader, prefix, ct);
        if (outerState is null)
            return 0;

        if (suffixNames.Length == 0)
            return await IterateItemsAsync(pipeReader, outerState.Value, processItem, ct);

        return await IterateSelectManyAsync(
            pipeReader,
            outerState.Value,
            suffixNames,
            processItem,
            ct
        );
    }

    internal static async Task<int> IterateItemsAsync(
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
                    throw new JsonException(IncompleteJsonMessage);
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
                pipeReader.AdvanceTo(reader.Position, buffer.End);
                count++;
            }
            else if (readResult.IsCompleted)
            {
                pipeReader.AdvanceTo(buffer.End);
                throw new JsonException(IncompleteJsonMessage);
            }
            else
            {
                pipeReader.AdvanceTo(buffer.Start, buffer.End);
            }
        }

        return count;
    }

    internal static async Task<int> IterateSelectManyAsync(
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
        int elementDepth = 0;
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
                            phase = EachPhase.SkipElement;
                            skipDepth = 0;
                        }
                        break;

                    case EachPhase.InInnerArray:
                        if (reader.TokenType == JsonTokenType.EndArray)
                        {
                            phase = EachPhase.SkipElement;
                            skipDepth = 0;
                        }
                        else
                        {
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
                throw new JsonException(IncompleteJsonMessage);
        }

        return count;
    }

    // Dead code removed: MaybeFlushAsync was unused — flush is inline in iteration loops.
}

/// <summary>
/// Delegate for transforming a JSON item during write-through streaming.
/// The <paramref name="itemBytes"/> are the raw UTF-8 bytes of one array element,
/// valid only for the duration of the call.
/// </summary>
public delegate void WriteItemDelegate(ReadOnlySequence<byte> itemBytes, Utf8JsonWriter writer);

/// <summary>
/// Controls flush behavior for write-through streaming.
/// </summary>
public sealed class WriteOptions
{
    /// <summary>
    /// Default options: flush at 16KB, no async flush callback.
    /// </summary>
    public static WriteOptions Default { get; } = new();

    /// <summary>
    /// Flush threshold in bytes. The writer is flushed when accumulated bytes
    /// since the last flush exceed <c>FlushThreshold * 0.9</c>.
    /// Set to 0 to disable automatic flushing.
    /// Default: 16384 (16KB).
    /// </summary>
    public int FlushThreshold { get; init; } = 16_384;

    /// <summary>
    /// Optional async flush callback invoked after <see cref="Utf8JsonWriter.Flush"/>
    /// when the threshold is exceeded. This is where backpressure happens.
    ///
    /// For HTTP streaming, set this to flush the underlying PipeWriter:
    /// <code>
    /// var options = new WriteOptions
    /// {
    ///     AsyncFlush = async ct => { await pipeWriter.FlushAsync(ct); }
    /// };
    /// </code>
    ///
    /// The iteration loop breaks out of the synchronous parsing loop (destroying
    /// the ref struct Utf8JsonReader), awaits this callback, then reconstructs
    /// the reader from saved JsonReaderState to continue where it left off.
    /// </summary>
    public Func<CancellationToken, ValueTask>? AsyncFlush { get; init; }

    /// <summary>
    /// Matches System.Text.Json's FlushThreshold ratio.
    /// Flush when 90% of the threshold is reached.
    /// </summary>
    internal const float FlushRatio = 0.9f;
}
