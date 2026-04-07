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
/// buffer exceeds 16 KB.
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
        var renderer = new MinifiedRenderer(jwriter);
        var framer = new NdJsonFramer();
        ct.ThrowIfCancellationRequested();

        framer.BeginDocument(writer);

        while (true)
        {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;
            var consumed = buffer.Start;

            try
            {
                if (result.IsCanceled)
                    throw new OperationCanceledException(ct);

                var bytesConsumed = WriteProjection(state, result, writer, path.Segments, ref renderer, ref framer);
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

        framer.EndDocument(writer);
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
        var renderer = new VerbatimRenderer();
        var framer = new NdJsonFramer();
        ct.ThrowIfCancellationRequested();

        framer.BeginDocument(writer);

        while (true)
        {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;
            var consumed = buffer.Start;

            try
            {
                if (result.IsCanceled)
                    throw new OperationCanceledException(ct);

                var bytesConsumed = WriteProjection(state, result, writer, path.Segments, ref renderer, ref framer);
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

        framer.EndDocument(writer);
    }

    // ── Item projection (raw-bytes callback) ───────────────────────────────

    /// <summary>
    /// Reads JSON from <paramref name="reader"/>, navigates to each value matching
    /// <paramref name="path"/>, and invokes <paramref name="processItem"/> with the
    /// raw bytes of each matched value. The callback receives the item bytes as a
    /// <see cref="ReadOnlySequence{T}"/> (valid only during the call) and the
    /// <paramref name="writer"/> for output.
    /// </summary>
    public static async Task ProjectItemsAsync(
        this PipeReader reader,
        NdJsonPath path,
        PipeWriter writer,
        Func<ReadOnlySequence<byte>, PipeWriter, ValueTask> processItem,
        JsonReaderOptions readerOptions = default,
        CancellationToken ct = default)
    {
        var state = new ProjectionState { ReaderState = new JsonReaderState(readerOptions) };
        ct.ThrowIfCancellationRequested();

        byte[]? accumulator = null;
        int accumulatedLength = 0;
        long captureStartIndex = -1;

        try
        {
            while (true)
            {
                var result = await reader.ReadAsync(ct);
                var buffer = result.Buffer;

                if (result.IsCanceled)
                    throw new OperationCanceledException(ct);

                var jsonReader = new Utf8JsonReader(
                    buffer, result.IsCompleted, state.ReaderState);

                long consumedUpTo = 0;
                bool needsAsyncBreak = false;
                ValueTask pendingTask = default;

                bool hasToken = jsonReader.Read();
                while (hasToken)
                {
                    byte[]? rentedName = null;
                    ParserDirective directive;
                    try
                    {
                        ReadOnlySpan<byte> name = jsonReader.TokenType == JsonTokenType.PropertyName
                                ? GetPropertyName(ref jsonReader, ref rentedName)
                                : default;
                        directive = state.Advance(jsonReader.TokenType, path.Segments, name);
                    }
                    finally
                    {
                        if (rentedName != null)
                            ArrayPool<byte>.Shared.Return(rentedName);
                    }

                    ReadOnlySequence<byte> itemSlice;

                    switch (directive)
                    {
                        case ParserDirective.Skip:
                            hasToken = jsonReader.Read();
                            continue;

                        case ParserDirective.YieldValue:
                        {
                            long start = jsonReader.TokenStartIndex;
                            long length = jsonReader.BytesConsumed - start;
                            itemSlice = buffer.Slice(buffer.GetPosition(start), length);
                            break;
                        }

                        case ParserDirective.BeginCapture:
                            captureStartIndex = jsonReader.TokenStartIndex;
                            continue; // don't advance reader

                        case ParserDirective.Capture:
                            hasToken = jsonReader.Read();
                            continue;

                        case ParserDirective.EndCapture:
                        {
                            long endPos = jsonReader.BytesConsumed;

                            if (accumulatedLength > 0)
                            {
                                int finalLen = (int)(endPos - captureStartIndex);
                                EnsureAccumulator(ref accumulator, accumulatedLength + finalLen);
                                buffer.Slice(buffer.GetPosition(captureStartIndex), finalLen)
                                      .CopyTo(accumulator.AsSpan(accumulatedLength));
                                accumulatedLength += finalLen;

                                itemSlice = new ReadOnlySequence<byte>(
                                    accumulator!, 0, accumulatedLength);
                            }
                            else
                            {
                                itemSlice = buffer.Slice(
                                    buffer.GetPosition(captureStartIndex),
                                    endPos - captureStartIndex);
                            }

                            captureStartIndex = -1;
                            break;
                        }

                        default:
                            hasToken = jsonReader.Read();
                            continue;
                    }

                    // ── Item found — call processItem ─────────────────────
                    var task = processItem(itemSlice, writer);
                    accumulatedLength = 0;

                    if (task.IsCompletedSuccessfully)
                    {
                        // Fast path: callback completed synchronously.
                        // Check if PipeWriter needs flushing (backpressure).
                        if (writer is { CanGetUnflushedBytes: true, UnflushedBytes: >= FlushThreshold })
                        {
                            consumedUpTo = jsonReader.BytesConsumed;
                            state.ReaderState = jsonReader.CurrentState;
                            needsAsyncBreak = true;
                            break;
                        }

                        // Stay in the sync loop — process next token from same buffer.
                        hasToken = jsonReader.Read();
                        continue;
                    }

                    // Slow path: callback is truly async.
                    // Save state and break out to await.
                    consumedUpTo = jsonReader.BytesConsumed;
                    state.ReaderState = jsonReader.CurrentState;
                    pendingTask = task;
                    needsAsyncBreak = true;
                    break;
                }

                if (needsAsyncBreak)
                {
                    // Await the async callback, then advance and continue
                    await pendingTask;
                    reader.AdvanceTo(buffer.GetPosition(consumedUpTo), buffer.End);

                    if (writer is { CanGetUnflushedBytes: true, UnflushedBytes: >= FlushThreshold })
                        await writer.FlushAsync(ct);
                    continue;
                }

                // End of tokens in this chunk — no pending item
                if (captureStartIndex >= 0)
                {
                    int captureLen = (int)(jsonReader.BytesConsumed - captureStartIndex);
                    if (captureLen > 0)
                    {
                        EnsureAccumulator(ref accumulator, accumulatedLength + captureLen);
                        buffer.Slice(buffer.GetPosition(captureStartIndex), captureLen)
                              .CopyTo(accumulator.AsSpan(accumulatedLength));
                        accumulatedLength += captureLen;
                    }
                    captureStartIndex = 0;
                }

                state.ReaderState = jsonReader.CurrentState;
                consumedUpTo = jsonReader.BytesConsumed;
                reader.AdvanceTo(buffer.GetPosition(consumedUpTo), buffer.End);

                if (result.IsCompleted)
                    break;
            }
        }
        finally
        {
            if (accumulator != null)
                ArrayPool<byte>.Shared.Return(accumulator);
        }
    }

    private static void EnsureAccumulator(ref byte[]? buffer, int needed)
    {
        if (buffer == null || buffer.Length < needed)
        {
            var old = buffer;
            buffer = ArrayPool<byte>.Shared.Rent(Math.Max(needed, 4096));
            if (old != null)
            {
                old.AsSpan().CopyTo(buffer);
                ArrayPool<byte>.Shared.Return(old);
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

    // ── WriteProjection (unified generic) ──────────────────────────────────────

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
                    ArrayPool<byte>.Shared.Return(rentedBuffer);
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

    // ── Shared helpers ────────────────────────────────────────────────────────

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

    private static void CopyToken(
        Utf8JsonReader reader,
        PipeWriter pipeWriter,
        ReadResult readResult
    )
    {
        int start = (int)reader.TokenStartIndex;
        int length = (int)(reader.BytesConsumed - reader.TokenStartIndex);

        if (readResult.Buffer.IsSingleSegment)
        {
            pipeWriter.Write(readResult.Buffer.FirstSpan.Slice(start, length));
        }
        else
        {
            var slice = readResult.Buffer.Slice(start, length);
            foreach (var seg in slice)
                pipeWriter.Write(seg.Span);
        }
    }

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

    // ── Strategy implementations ────────────────────────────────────────

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
                byte[] rented = ArrayPool<byte>.Shared.Rent(len);
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
                    ArrayPool<byte>.Shared.Return(rented);
                }
            }
        }
    }

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
        private int _depth = -1;
        private int _matchedDepth;
        private readonly bool[] _isArray = new bool[64];
        private readonly int[] _matchedDepthStack = new int[64];
        private bool _pendingPropertyMatches;
        private bool _isCapturing;
        private int _captureDepth;
        public JsonReaderState ReaderState;

        public ParserDirective Advance(
            JsonTokenType tokenType,
            byte[][] pattern,
            ReadOnlySpan<byte> propertyName = default)
        {
            // ── CAPTURE PHASE ─────────────────────────────────────────
            if (_isCapturing)
            {
                if (tokenType is JsonTokenType.StartObject or JsonTokenType.StartArray)
                    _captureDepth++;
                else if (tokenType is JsonTokenType.EndObject or JsonTokenType.EndArray)
                    _captureDepth--;

                if (_captureDepth == 0)
                {
                    _isCapturing = false;
                    return ParserDirective.EndCapture;
                }

                return ParserDirective.Capture;
            }

            // ── SEARCH PHASE ──────────────────────────────────────────
            switch (tokenType)
            {
                case JsonTokenType.PropertyName:
                    _pendingPropertyMatches = _matchedDepth == _depth
                        && MatchesPropertyName(pattern, _matchedDepth, propertyName);
                    return ParserDirective.Skip;

                case JsonTokenType.StartObject:
                case JsonTokenType.StartArray:
                {
                    bool isArray = tokenType == JsonTokenType.StartArray;
                    bool parentIsArray = _depth >= 0 && _isArray[_depth];

                    bool seg = _matchedDepth == _depth
                        && MatchesSegment(_matchedDepth, pattern, parentIsArray, _pendingPropertyMatches);
                    _pendingPropertyMatches = false;

                    _depth++;
                    _isArray[_depth] = isArray;
                    _matchedDepthStack[_depth] = _matchedDepth;

                    if (seg && _matchedDepth + 1 == pattern.Length)
                    {
                        _depth--;
                        _matchedDepth = _matchedDepthStack[_depth + 1];
                        _isCapturing = true;
                        _captureDepth = 0;
                        return ParserDirective.BeginCapture;
                    }

                    if (seg)
                        _matchedDepth++;

                    return ParserDirective.Skip;
                }

                case JsonTokenType.EndObject:
                case JsonTokenType.EndArray:
                    _pendingPropertyMatches = false;
                    if (_depth >= 0)
                    {
                        _matchedDepth = _matchedDepthStack[_depth];
                        _depth--;
                    }
                    return ParserDirective.Skip;

                default:
                {
                    bool parentIsArray = _depth >= 0 && _isArray[_depth];

                    bool seg = _matchedDepth == _depth
                        && MatchesSegment(_matchedDepth, pattern, parentIsArray, _pendingPropertyMatches);
                    _pendingPropertyMatches = false;

                    if (seg && _matchedDepth + 1 == pattern.Length)
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
}
