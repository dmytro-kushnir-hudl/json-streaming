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
///   <item><see cref="EnumerateArrayAsync"/> — yields <see cref="JsonDocument"/> per item</item>
///   <item><see cref="ProcessArrayAsync"/> — invokes a callback with raw bytes per item (zero-copy)</item>
/// </list>
/// </summary>
public static class JsonStreamReader
{
    // ── IAsyncEnumerable API ───────────────────────────────────────────────

    /// <summary>
    /// Navigates to the target array and yields each element as a <see cref="JsonDocument"/>.
    /// The caller must dispose each document.
    /// </summary>
    public static IAsyncEnumerable<JsonDocument> EnumerateArrayAsync(
        PipeReader pipeReader,
        JsonPath path,
        CancellationToken ct = default
    ) => EnumerateArrayCoreAsync(pipeReader, path, ct);

    /// <summary>
    /// Convenience overload accepting a dot-separated path string.
    /// </summary>
    public static IAsyncEnumerable<JsonDocument> EnumerateArrayAsync(
        PipeReader pipeReader,
        string path,
        CancellationToken ct = default
    ) => EnumerateArrayCoreAsync(pipeReader, ParseDotPath(path), ct);

    private static async IAsyncEnumerable<JsonDocument> EnumerateArrayCoreAsync(
        PipeReader pipeReader,
        JsonPath path,
        [EnumeratorCancellation] CancellationToken ct = default
    )
    {
        var navState = await NavigateToArrayAsync(pipeReader, path, ct);
        if (navState is null)
            yield break;

        var jsonState = navState.Value;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var readResult = await pipeReader.ReadAsync(ct);
            var buffer = readResult.Buffer;
            var reader = new Utf8JsonReader(buffer, isFinalBlock: readResult.IsCompleted, jsonState);

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

    // ── Callback API (zero-copy) ───────────────────────────────────────────

    /// <summary>
    /// Navigates to the target array and invokes <paramref name="processItem"/> for each
    /// element's raw bytes. The <see cref="ReadOnlySequence{T}"/> is only valid during
    /// the callback. Returns the number of items processed.
    /// </summary>
    public static Task<int> ProcessArrayAsync(
        PipeReader pipeReader,
        JsonPath path,
        Action<ReadOnlySequence<byte>> processItem,
        CancellationToken ct = default
    ) => ProcessArrayCoreAsync(pipeReader, path, processItem, ct);

    /// <summary>
    /// Convenience overload accepting a dot-separated path string.
    /// </summary>
    public static Task<int> ProcessArrayAsync(
        PipeReader pipeReader,
        string path,
        Action<ReadOnlySequence<byte>> processItem,
        CancellationToken ct = default
    ) => ProcessArrayCoreAsync(pipeReader, ParseDotPath(path), processItem, ct);

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

        var jsonState = navState.Value;
        int count = 0;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var readResult = await pipeReader.ReadAsync(ct);
            var buffer = readResult.Buffer;
            var reader = new Utf8JsonReader(buffer, isFinalBlock: readResult.IsCompleted, jsonState);

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

    // ── Navigation ─────────────────────────────────────────────────────────

    private enum NavPhase
    {
        SearchProperty,
        ExpectObject,
        ExpectArray,
        Skipping,
        Done,
    }

    private static async Task<JsonReaderState?> NavigateToArrayAsync(
        PipeReader pipeReader,
        JsonPath path,
        CancellationToken ct
    )
    {
        // Copy segments to array — ReadOnlySpan can't cross await boundaries
        var segmentsSpan = path.Segments;
        if (segmentsSpan.IsEmpty)
            return await SkipToRootArrayAsync(pipeReader, ct);

        var segmentCount = segmentsSpan.Length;
        var propertyNames = new byte[segmentCount][];
        for (int s = 0; s < segmentCount; s++)
            propertyNames[s] = segmentsSpan[s].Name.ToArray();

        var jsonState = new JsonReaderState();
        var phase = NavPhase.SearchProperty;
        var returnPhase = NavPhase.SearchProperty;
        int segmentIndex = 0;
        int skipDepth = 0;

        while (phase != NavPhase.Done)
        {
            var readResult = await pipeReader.ReadAsync(ct);
            var buffer = readResult.Buffer;
            var reader = new Utf8JsonReader(buffer, isFinalBlock: readResult.IsCompleted, jsonState);

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
                                if (segmentIndex == segmentCount - 1)
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

                if (phase == NavPhase.Done)
                    break;
            }

            jsonState = reader.CurrentState;
            pipeReader.AdvanceTo(reader.Position, buffer.End);

            if (readResult.IsCompleted)
                return null;
        }

        return null;
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
            var reader = new Utf8JsonReader(buffer, isFinalBlock: readResult.IsCompleted, jsonState);

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
