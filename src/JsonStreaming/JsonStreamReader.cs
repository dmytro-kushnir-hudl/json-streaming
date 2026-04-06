using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;

namespace JsonStreaming;

/// <summary>
/// Incrementally reads a JSON stream, navigates to nested arrays via <see cref="JsonPath"/>,
/// and invokes a callback for each element's raw bytes (zero-copy).
/// Memory usage is bounded by PipeReader buffer size (~8KB) regardless of input size.
///
/// Supports <see cref="SegmentKind.Each"/> for select-many: <c>$.responses[*].messages</c>
/// iterates each response and yields all messages across all of them.
///
/// For <see cref="IAsyncEnumerable{T}"/> consumption, see
/// <see cref="JsonStreamEnumerable.EnumerateArrayAsync(PipeReader, JsonPath, CancellationToken)"/>.
/// </summary>
public static class JsonStreamReader
{
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
            ? ProcessSelectManyCoreAsync(pipeReader, path, processItem, ct)
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

    // ── Simple path (no Each) ──────────────────────────────────────────────

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

    // ── Select-many (Each) ─────────────────────────────────────────────────

    private static async Task<int> ProcessSelectManyCoreAsync(
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

    // ── Core: simple array iteration ───────────────────────────────────────

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
