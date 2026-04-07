using System.IO.Pipelines;
using System.Text.Json;

namespace JsonStreaming;

/// <summary>
/// Navigates a JSON stream via <see cref="JsonPath"/> to position the reader
/// just inside the target array's StartArray token. All methods return a
/// <see cref="JsonReaderState"/> that the caller uses to continue reading items.
/// </summary>
internal static class JsonPathNavigator
{
    private enum NavPhase
    {
        SearchProperty,
        ExpectObject,
        ExpectArray,
        Skipping,
    }

    /// <summary>
    /// Advances the <paramref name="pipeReader"/> to the start of the array
    /// identified by <paramref name="path"/>. Returns the reader state positioned
    /// just after StartArray, or null if the path was not found.
    /// </summary>
    internal static async Task<JsonReaderState?> NavigateToArrayAsync(
        PipeReader pipeReader,
        JsonPath path,
        CancellationToken ct
    )
    {
        if (path.Length == 0)
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

    /// <summary>
    /// Splits a path at the first <see cref="SegmentKind.Each"/> into prefix and suffix.
    /// </summary>
    internal static (JsonPath Prefix, JsonPath Suffix) SplitAtEach(JsonPath path)
    {
        var segments = path.Segments;
        var prefix = JsonPath.Root;
        int i = 0;

        while (i < segments.Length && segments[i].Kind != SegmentKind.Each)
        {
            prefix = prefix.Property(segments[i].Name.Span);
            i++;
        }

        // Skip the Each() itself
        if (i < segments.Length)
            i++;

        var suffix = JsonPath.Root;
        while (i < segments.Length)
        {
            suffix = suffix.Property(segments[i].Name.Span);
            i++;
        }

        return (prefix, suffix);
    }

    internal static bool HasEach(JsonPath path)
    {
        var segments = path.Segments;
        for (int i = 0; i < segments.Length; i++)
        {
            if (segments[i].Kind == SegmentKind.Each)
                return true;
        }
        return false;
    }

    internal static byte[][] ExtractPropertyNames(JsonPath path)
    {
        var segments = path.Segments;
        var names = new byte[segments.Length][];
        for (int i = 0; i < segments.Length; i++)
            names[i] = segments[i].Name.ToArray();
        return names;
    }

    internal static JsonPath ParseDotPath(string path)
    {
        if (string.IsNullOrEmpty(path))
            return JsonPath.Root;

        var result = JsonPath.Root;
        foreach (var segment in path.Split('.'))
            result = result.Property(System.Text.Encoding.UTF8.GetBytes(segment));
        return result;
    }

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
}
