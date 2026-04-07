using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;

namespace JsonStreaming;

/// <summary>
/// Streaming JSON transcoding: format, minify, or project arbitrary JSON from a
/// <see cref="PipeReader"/> to a <see cref="PipeWriter"/> without buffering the
/// whole document.
///
/// <list type="bullet">
///   <item><see cref="ProxyFormattedJsonAsync"/> — pretty-print (2-space indent)</item>
///   <item><see cref="ProxyMinifiedJsonAsync"/> — strip whitespace</item>
///   <item><see cref="ProjectNdJsonAsync"/> — extract matched values as NDJSON</item>
/// </list>
///
/// All methods respect backpressure: they flush to the writer when the unflushed
/// buffer exceeds 16 KB, matching <see cref="WriteOptions.FlushThreshold"/>.
/// </summary>
public static class JsonTranscoder
{
    private const int FlushThreshold = 16 * 1024;

    // ── Pretty-print ──────────────────────────────────────────────────────────

    /// <summary>
    /// Reads JSON from <paramref name="reader"/> and writes it formatted with
    /// 2-space indentation to <paramref name="writer"/>.
    /// </summary>
    public static async Task ProxyFormattedJsonAsync(
        this PipeReader reader,
        PipeWriter writer,
        JsonReaderOptions options = default,
        CancellationToken ct = default
    )
    {
        var state = new FormattedState { State = new JsonReaderState(options) };
        ct.ThrowIfCancellationRequested();

        while (true)
        {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;
            var consumed = buffer.Start;

            try
            {
                if (result.IsCanceled)
                    throw new OperationCanceledException(ct);

                var bytesConsumed = WriteFormatted(state, result, writer);
                consumed = buffer.GetPosition(bytesConsumed);

                if (result.IsCompleted || writer.UnflushedBytes >= FlushThreshold)
                    await writer.FlushAsync(ct);

                if (result.IsCompleted)
                    break;
            }
            finally
            {
                reader.AdvanceTo(consumed, buffer.End);
            }
        }
    }

    // ── Minify ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Reads JSON from <paramref name="reader"/> and writes it with all whitespace
    /// stripped to <paramref name="writer"/>.
    /// </summary>
    public static async Task ProxyMinifiedJsonAsync(
        this PipeReader reader,
        PipeWriter writer,
        JsonReaderOptions options = default,
        CancellationToken ct = default
    )
    {
        var state = new MinifiedState { State = new JsonReaderState(options) };
        ct.ThrowIfCancellationRequested();

        while (true)
        {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;
            var consumed = buffer.Start;

            try
            {
                if (result.IsCanceled)
                    throw new OperationCanceledException(ct);

                var bytesConsumed = WriteMinified(state, result, writer);
                consumed = buffer.GetPosition(bytesConsumed);

                if (result.IsCompleted || writer.UnflushedBytes >= FlushThreshold)
                    await writer.FlushAsync(ct);

                if (result.IsCompleted)
                    break;
            }
            finally
            {
                reader.AdvanceTo(consumed, buffer.End);
            }
        }
    }

    // ── NDJSON projection ─────────────────────────────────────────────────────

    /// <summary>
    /// Reads JSON from <paramref name="reader"/>, navigates to each value matching
    /// <paramref name="path"/>, and writes each match as a minified JSON line
    /// (newline-delimited) to <paramref name="writer"/>.
    /// </summary>
    public static async Task ProjectNdJsonAsync(
        this PipeReader reader,
        NdJsonPath path,
        PipeWriter writer,
        JsonReaderOptions readerOptions = default,
        JsonWriterOptions writerOptions = default,
        CancellationToken ct = default
    )
    {
        var jwriter = new Utf8JsonWriter(writer, writerOptions);
        var state = new ProjectionState { ReaderState = new JsonReaderState(readerOptions) };
        ct.ThrowIfCancellationRequested();

        while (true)
        {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;
            var consumed = buffer.Start;

            try
            {
                if (result.IsCanceled)
                    throw new OperationCanceledException(ct);

                var bytesConsumed = WriteProjection(state, result, jwriter, writer, path.Segments);
                consumed = buffer.GetPosition(bytesConsumed);

                if (
                    result.IsCompleted
                    || writer is { CanGetUnflushedBytes: true, UnflushedBytes: >= FlushThreshold }
                )
                {
                    await jwriter.FlushAsync(ct);
                    await writer.FlushAsync(ct);
                }

                if (result.IsCompleted)
                    break;
            }
            finally
            {
                reader.AdvanceTo(consumed, buffer.End);
            }
        }
    }

    /// <summary>
    /// Reads JSON from <paramref name="reader"/>, navigates to each value matching
    /// <paramref name="path"/>, and writes each match as a minified JSON line
    /// (newline-delimited) to <paramref name="writer"/> by copying raw token slices
    /// directly from the input buffer.
    /// </summary>
    public static async Task ProjectNdJsonVerbatimAsync(
        this PipeReader reader,
        NdJsonPath path,
        PipeWriter writer,
        JsonReaderOptions options = default,
        CancellationToken ct = default
    )
    {
        var state = new ProjectionState { ReaderState = new JsonReaderState(options) };
        ct.ThrowIfCancellationRequested();

        while (true)
        {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;
            var consumed = buffer.Start;

            try
            {
                if (result.IsCanceled)
                    throw new OperationCanceledException(ct);

                var bytesConsumed = WriteProjectionDirect(state, result, writer, path.Segments);
                consumed = buffer.GetPosition(bytesConsumed);

                if (result.IsCompleted || writer is { CanGetUnflushedBytes: true, UnflushedBytes: >= FlushThreshold })
                {
                    await writer.FlushAsync(ct);
                }

                if (result.IsCompleted)
                    break;
            }
            finally
            {
                reader.AdvanceTo(consumed, buffer.End);
            }
        }
    }

    // ── WriteFormatted ────────────────────────────────────────────────────────

    private static long WriteFormatted(
        FormattedState state,
        ReadResult readResult,
        PipeWriter pipeWriter
    )
    {
        var reader = new Utf8JsonReader(readResult.Buffer, readResult.IsCompleted, state.State);

        while (reader.Read())
        {
            if (reader.TokenType is JsonTokenType.EndObject or JsonTokenType.EndArray)
            {
                state.Depth--;
                pipeWriter.Write("\n"u8);
                WriteIndent(pipeWriter, state.Depth);
            }
            else if (state.AfterColon)
            {
                pipeWriter.Write(" "u8);
                state.AfterColon = false;
            }
            else if (state.NeedsComma)
            {
                pipeWriter.Write(",\n"u8);
                WriteIndent(pipeWriter, state.Depth);
            }
            else
            {
                WriteIndent(pipeWriter, state.Depth);
            }

            switch (reader.TokenType)
            {
                case JsonTokenType.StartObject:
                    pipeWriter.Write("{\n"u8);
                    state.Depth++;
                    state.NeedsComma = false;
                    break;
                case JsonTokenType.EndObject:
                    pipeWriter.Write("}"u8);
                    state.NeedsComma = true;
                    break;
                case JsonTokenType.StartArray:
                    pipeWriter.Write("[\n"u8);
                    state.Depth++;
                    state.NeedsComma = false;
                    break;
                case JsonTokenType.EndArray:
                    pipeWriter.Write("]"u8);
                    state.NeedsComma = true;
                    break;
                case JsonTokenType.PropertyName:
                    CopyToken(reader, pipeWriter, readResult);
                    state.AfterColon = true;
                    state.NeedsComma = false;
                    break;
                default:
                    CopyToken(reader, pipeWriter, readResult);
                    state.NeedsComma = true;
                    break;
            }
        }

        state.State = reader.CurrentState;
        return reader.BytesConsumed;

        static void WriteIndent(PipeWriter pw, int depth)
        {
            for (int i = 0; i < depth; i++)
                pw.Write("  "u8);
        }
    }

    // ── WriteMinified ─────────────────────────────────────────────────────────

    private static long WriteMinified(
        MinifiedState state,
        ReadResult readResult,
        PipeWriter pipeWriter
    )
    {
        var reader = new Utf8JsonReader(readResult.Buffer, readResult.IsCompleted, state.State);

        while (reader.Read())
        {
            if (
                state.NeedsComma
                && reader.TokenType is not JsonTokenType.EndObject and not JsonTokenType.EndArray
            )
                pipeWriter.Write(","u8);

            state.NeedsComma = reader.TokenType switch
            {
                JsonTokenType.StartObject
                or JsonTokenType.StartArray
                or JsonTokenType.PropertyName => false,
                _ => true,
            };

            CopyToken(reader, pipeWriter, readResult);
        }

        state.State = reader.CurrentState;
        return reader.BytesConsumed;
    }

    // ── WriteProjection ───────────────────────────────────────────────────────

    private static long WriteProjection(
        ProjectionState state,
        ReadResult readResult,
        Utf8JsonWriter jwriter,
        PipeWriter pipeWriter,
        byte[][] pattern
    )
    {
        var reader = new Utf8JsonReader(
            readResult.Buffer,
            readResult.IsCompleted,
            state.ReaderState
        );

        while (reader.Read())
        {
            if (state.CaptureDepth > 0)
            {
                if (reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray)
                    state.CaptureDepth++;
                else if (reader.TokenType is JsonTokenType.EndObject or JsonTokenType.EndArray)
                    state.CaptureDepth--;

                WriteToken(reader, jwriter);

                if (state.CaptureDepth == 0)
                {
                    jwriter.Flush();
                    pipeWriter.Write("\n"u8);
                    jwriter.Reset();
                }
                continue;
            }

            switch (reader.TokenType)
            {
                case JsonTokenType.PropertyName:
                    state.PendingPropertyMatches = state.MatchedDepth == state.Depth
                                                   && MatchesPropertyName(reader, pattern, state.MatchedDepth);
                    break;

                case JsonTokenType.StartObject:
                case JsonTokenType.StartArray:
                {
                    bool isArray = reader.TokenType == JsonTokenType.StartArray;
                    bool parentIsArray = state.Depth >= 0 && state.IsArray[state.Depth];

                    bool seg = state.MatchedDepth == state.Depth
                               && MatchesProjectionSegment(
                                   state.MatchedDepth,
                                   pattern,
                                   parentIsArray,
                                   state.PendingPropertyMatches
                               );
                    
                    state.PendingPropertyMatches = false;

                    state.Depth++;
                    state.IsArray[state.Depth] = isArray;
                    state.MatchedDepthStack[state.Depth] = state.MatchedDepth;

                    if (seg && state.MatchedDepth + 1 == pattern.Length)
                    {
                        if (isArray)
                            jwriter.WriteStartArray();
                        else
                            jwriter.WriteStartObject();
                        
                        state.CaptureDepth = 1;
                        state.Depth--;
                        state.MatchedDepth = state.MatchedDepthStack[state.Depth + 1];
                    }
                    else if (seg)
                    {
                        state.MatchedDepth++;
                    }
                    break;
                }

                case JsonTokenType.EndObject:
                case JsonTokenType.EndArray:
                    state.PendingPropertyMatches = false;
                    if (state.Depth >= 0)
                    {
                        state.MatchedDepth = state.MatchedDepthStack[state.Depth];
                        state.Depth--;
                    }
                    break;

                default:
                {
                    bool parentIsArray = state.Depth >= 0 && state.IsArray[state.Depth];

                    bool seg =
                        state.MatchedDepth == state.Depth
                        && MatchesProjectionSegment(
                            state.MatchedDepth,
                            pattern,
                            parentIsArray,
                            state.PendingPropertyMatches
                        );
                    state.PendingPropertyMatches = false;

                    if (seg && state.MatchedDepth + 1 == pattern.Length)
                    {
                        WriteToken(reader, jwriter);
                        jwriter.Flush();
                        pipeWriter.Write("\n"u8);
                        jwriter.Reset();
                    }
                    break;
                }
            }
        }

        state.ReaderState = reader.CurrentState;
        return reader.BytesConsumed;

        static void WriteToken(Utf8JsonReader r, Utf8JsonWriter w)
        {
            switch (r.TokenType)
            {
                case JsonTokenType.StartObject: w.WriteStartObject(); break;
                case JsonTokenType.EndObject:   w.WriteEndObject(); break;
                case JsonTokenType.StartArray:  w.WriteStartArray(); break;
                case JsonTokenType.EndArray:    w.WriteEndArray(); break;
                case JsonTokenType.True:        w.WriteBooleanValue(true); break;
                case JsonTokenType.False:       w.WriteBooleanValue(false); break;
                case JsonTokenType.Null:        w.WriteNullValue(); break;
                
                case JsonTokenType.Comment
                  or JsonTokenType.PropertyName
                  or JsonTokenType.String
                  or JsonTokenType.Number: WriteTokenSequence(r, w); break;
                
                case JsonTokenType.None: break;
            }
            
            static void WriteTokenSequence(Utf8JsonReader r, Utf8JsonWriter w)
            {
                if (!r.HasValueSequence)
                {
                    switch (r.TokenType)
                    {
                        case JsonTokenType.PropertyName: w.WritePropertyName(r.ValueSpan); break;
                        case JsonTokenType.String: w.WriteStringValue(r.ValueSpan); break;
                        case JsonTokenType.Number: w.WriteRawValue(r.ValueSpan, skipInputValidation: true); break;
                    }
                }
                else
                {
                    int len = (int)r.ValueSequence.Length;
                    byte[] rented = ArrayPool<byte>.Shared.Rent(len);
                
                    try
                    {
                        r.ValueSequence.CopyTo(rented);
                        ReadOnlySpan<byte> span = rented.AsSpan(0, len);
                        switch (r.TokenType)
                        {
                            case JsonTokenType.PropertyName: w.WritePropertyName(span); break;
                            case JsonTokenType.String: w.WriteStringValue(span); break;
                            case JsonTokenType.Number: w.WriteRawValue(span, skipInputValidation: true); break;
                        }
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(rented);
                    }
                }
            }
        }
    }

    private static long WriteProjectionDirect(
        ProjectionState state,
        ReadResult readResult,
        PipeWriter pipeWriter,
        byte[][] pattern
    )
    {
        var reader = new Utf8JsonReader(
            readResult.Buffer,
            readResult.IsCompleted,
            state.ReaderState
        );

        while (reader.Read())
        {
            if (state.CaptureDepth > 0)
            {
                if (state.CaptureNeedsComma && reader.TokenType is not JsonTokenType.EndObject and not JsonTokenType.EndArray)
                    pipeWriter.Write(","u8);

                state.CaptureNeedsComma = reader.TokenType switch
                {
                    JsonTokenType.StartObject or JsonTokenType.StartArray or JsonTokenType.PropertyName => false,
                    _ => true,
                };

                switch (reader.TokenType)
                {
                    case JsonTokenType.StartObject or JsonTokenType.StartArray:
                        state.CaptureDepth++;
                        break;
                    case JsonTokenType.EndObject or JsonTokenType.EndArray:
                        state.CaptureDepth--;
                        break;
                }

                CopyToken(reader, pipeWriter, readResult);

                if (state.CaptureDepth == 0)
                {
                    pipeWriter.Write("\n"u8);
                    state.CaptureNeedsComma = false;
                }

                continue;
            }

            switch (reader.TokenType)
            {
                case JsonTokenType.PropertyName:
                    state.PendingPropertyMatches = state.MatchedDepth == state.Depth
                                                   && MatchesPropertyName(reader, pattern, state.MatchedDepth);
                    break;

                case JsonTokenType.StartObject:
                case JsonTokenType.StartArray:
                {
                    bool isArray = reader.TokenType == JsonTokenType.StartArray;
                    bool parentIsArray = state.Depth >= 0 && state.IsArray[state.Depth];

                    bool seg =
                        state.MatchedDepth == state.Depth
                        && MatchesProjectionSegment(
                            state.MatchedDepth,
                            pattern,
                            parentIsArray,
                            state.PendingPropertyMatches
                        );
                    state.PendingPropertyMatches = false;

                    state.Depth++;
                    state.IsArray[state.Depth] = isArray;
                    state.MatchedDepthStack[state.Depth] = state.MatchedDepth;

                    if (seg && state.MatchedDepth + 1 == pattern.Length)
                    {
                        CopyToken(reader, pipeWriter, readResult);
                        
                        state.CaptureDepth = 1;
                        state.CaptureNeedsComma = false;
                        state.Depth--;
                        state.MatchedDepth = state.MatchedDepthStack[state.Depth + 1];
                    }
                    else if (seg)
                    {
                        state.MatchedDepth++;
                    }
                    break;
                }

                case JsonTokenType.EndObject:
                case JsonTokenType.EndArray:
                    state.PendingPropertyMatches = false;
                    if (state.Depth >= 0)
                    {
                        state.MatchedDepth = state.MatchedDepthStack[state.Depth];
                        state.Depth--;
                    }
                    break;

                default:
                {
                    bool parentIsArray = state.Depth >= 0 && state.IsArray[state.Depth];

                    bool seg = state.MatchedDepth == state.Depth
                               && MatchesProjectionSegment(
                                   state.MatchedDepth,
                                   pattern,
                                   parentIsArray,
                                   state.PendingPropertyMatches
                               );
                    state.PendingPropertyMatches = false;

                    if (seg && state.MatchedDepth + 1 == pattern.Length)
                    {
                        CopyToken(reader, pipeWriter, readResult);
                        pipeWriter.Write("\n"u8);
                    }
                    break;
                }
            }
        }

        state.ReaderState = reader.CurrentState;
        return reader.BytesConsumed;
    }

    // ── Shared helpers ────────────────────────────────────────────────────────

    private static void CopyToken(
        Utf8JsonReader reader,
        PipeWriter pipeWriter,
        ReadResult readResult
    )
    {
        var slice = readResult.Buffer.Slice(
            reader.TokenStartIndex,
            reader.BytesConsumed - reader.TokenStartIndex
        );
        
        if (slice.IsSingleSegment)
            pipeWriter.Write(slice.FirstSpan);
        else
            foreach (var seg in slice)
                pipeWriter.Write(seg.Span);
    }

    private static bool MatchesProjectionSegment(
        int matchedDepth,
        byte[][] pattern,
        bool parentIsArray,
        bool pendingPropertyMatches
    )
    {
        if (matchedDepth >= pattern.Length)
            return false;

        var seg = pattern[matchedDepth];
        
        if (seg.Length == 0)
            return parentIsArray;

        return !parentIsArray && pendingPropertyMatches;
    }

    private static bool MatchesPropertyName(
        Utf8JsonReader reader,
        byte[][] pattern,
        int matchedDepth
    )
    {
        if (matchedDepth >= pattern.Length)
            return false;

        var expected = pattern[matchedDepth];
        if (expected.Length == 0)
            return false;

        if (reader.HasValueSequence)
        {
            var remaining = expected.AsSpan();
            foreach (var slice in reader.ValueSequence)
            {
                if (slice.Length > remaining.Length)
                    return false;
                if (!slice.Span.SequenceEqual(remaining[..slice.Length]))
                    return false;
                remaining = remaining[slice.Length..];
            }

            return remaining.IsEmpty;
        }

        return reader.ValueSpan.SequenceEqual(expected);
    }

    // ── State classes ─────────────────────────────────────────────────────────

    private sealed class FormattedState
    {
        public int Depth;
        public bool NeedsComma;
        public bool AfterColon;
        public JsonReaderState State;
    }

    private sealed class MinifiedState
    {
        public bool NeedsComma;
        public JsonReaderState State;
    }

    private sealed class ProjectionState
    {
        public int Depth = -1;
        public int MatchedDepth;
        public readonly bool[] IsArray = new bool[64];
        public readonly int[] MatchedDepthStack = new int[64];
        public bool PendingPropertyMatches;
        public int CaptureDepth;
        public bool CaptureNeedsComma;
        public JsonReaderState ReaderState;
    }
}
