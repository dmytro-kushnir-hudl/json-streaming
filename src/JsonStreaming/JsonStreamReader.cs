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
/// Supports <see cref="SegmentKind.Each"/> for select-many: <c>$.responses[*].messages</c>
/// iterates each response and yields all messages across all of them.
/// </summary>
public static class JsonStreamReader
{
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
    /// This is the primary API for HTTP streaming: pass
    /// <c>new Utf8JsonWriter(httpContext.Response.BodyWriter)</c> as the writer.
    /// </summary>
    public static Task<int> WriteArrayAsync(
        PipeReader pipeReader,
        JsonPath path,
        Utf8JsonWriter writer,
        CancellationToken ct = default
    ) =>
        ProcessArrayAsync(
            pipeReader,
            path,
            itemBytes =>
            {
                using var doc = JsonDocument.Parse(itemBytes);
                doc.RootElement.WriteTo(writer);
            },
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
    ) =>
        WriteArrayAsync(pipeReader, JsonPathNavigator.ParseDotPath(path), writer, ct);

    /// <summary>
    /// Navigates to the target array(s) and invokes <paramref name="writeItem"/>
    /// for each item. The delegate receives raw item bytes and the output writer,
    /// enabling selective field copying, transformation, or filtering.
    /// Returns the number of items processed (including items where the delegate
    /// chose not to write anything).
    /// </summary>
    public static Task<int> WriteArrayAsync(
        PipeReader pipeReader,
        JsonPath path,
        Utf8JsonWriter writer,
        WriteItemDelegate writeItem,
        CancellationToken ct = default
    ) =>
        ProcessArrayAsync(pipeReader, path, itemBytes => writeItem(itemBytes, writer), ct);

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
        WriteArrayAsync(pipeReader, JsonPathNavigator.ParseDotPath(path), writer, writeItem, ct);

    // ── Core: simple array iteration ───────────────────────────────────────

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

    // ── Core: simple item iteration ────────────────────────────────────────

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

    // ── Core: select-many state machine ────────────────────────────────────

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
                break;
        }

        return count;
    }
}

/// <summary>
/// Delegate for transforming a JSON item during write-through streaming.
/// The <paramref name="itemBytes"/> are the raw UTF-8 bytes of one array element,
/// valid only for the duration of the call.
/// </summary>
public delegate void WriteItemDelegate(ReadOnlySequence<byte> itemBytes, Utf8JsonWriter writer);
