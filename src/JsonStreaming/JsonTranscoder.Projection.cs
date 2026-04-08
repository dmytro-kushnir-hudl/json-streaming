using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;

namespace JsonStreaming;

public delegate void Transformer(ReadOnlySequence<byte> itemBytes, Writers output);

public static partial class JsonTranscoder
{
    /// <summary>
    /// Reads JSON from <paramref name="input"/>, navigates to each value matching
    /// <paramref name="selector"/>, and invokes <paramref name="transformer"/> with the
    /// raw bytes of each matched value. Items are written as newline-delimited JSON.
    /// </summary>
    public static Task TransformItemsAsync(
        this PipeReader input,
        PipeWriter output,
        JsonPath selector,
        Transformer transformer,
        JsonReaderOptions options = default,
        CancellationToken ct = default)
        => TransformItemsCoreAsync(input, output, selector, transformer, new NdJsonFramer(), options, ct);

    /// <summary>
    /// Like <see cref="TransformItemsAsync"/> but wraps the output in a JSON array
    /// (<c>[item,item,…]</c>).
    /// </summary>
    public static Task TransformItemsAsArrayAsync(
        this PipeReader input,
        PipeWriter output,
        JsonPath selector,
        Transformer transformer,
        JsonReaderOptions options = default,
        CancellationToken ct = default)
        => TransformItemsCoreAsync(input, output, selector, transformer, new JsonArrayFramer(), options, ct);

    /// <summary>
    /// Like <see cref="TransformItemsAsync"/> but wraps the output in a
    /// <c>{"results":[…],"count":N}</c> envelope.
    /// </summary>
    public static Task TransformItemsWithEnvelopeAsync(
        this PipeReader input,
        PipeWriter output,
        JsonPath selector,
        Transformer transformer,
        JsonReaderOptions options = default,
        CancellationToken ct = default)
        => TransformItemsCoreAsync(input, output, selector, transformer, new JsonEnvelopeFramer(), options, ct);

    private static async Task TransformItemsCoreAsync<TFramer>(
        PipeReader input,
        PipeWriter output,
        JsonPath selector,
        Transformer transformer,
        TFramer framer,
        JsonReaderOptions options,
        CancellationToken ct)
        where TFramer : struct, IItemFramer
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(selector);

        var state = new FilterStateMachine(new JsonReaderState(options));
        ct.ThrowIfCancellationRequested();
        await using var utf8Json = new Utf8JsonWriter(output);
        var writers = new Writers(output, utf8Json);
        framer.BeginDocument(writers);

        while (true)
        {
            var result = await input.ReadAsync(ct);
            var buffer = result.Buffer;
            var consumed = buffer.Start;

            try
            {
                if (result.IsCanceled)
                    throw new OperationCanceledException(ct);

                var bytesConsumed = WriteTransformation(state, result, output, selector, transformer, ref framer, writers);
                consumed = buffer.GetPosition(bytesConsumed);

                if (result.IsCompleted || output is { CanGetUnflushedBytes: true, UnflushedBytes: >= FlushThreshold })
                {
                    await output.FlushAsync(ct);
                }

                if (result.IsCompleted)
                    break;
            }
            finally
            {
                input.AdvanceTo(consumed, buffer.End);
            }
        }

        framer.EndDocument(writers);
    }

    private static long WriteTransformation<TFramer>(
        FilterStateMachine state,
        ReadResult readResult,
        PipeWriter output,
        JsonPath pattern,
        Transformer transformer,
        ref TFramer framer,
        Writers writers)
        where TFramer : struct, IItemFramer
    {
        // When a cross-buffer capture is in progress the pipe has been anchored at the
        // capture start, so the new buffer starts exactly there — re-anchor to 0.
        state.BeginSegment();

        var reader = new Utf8JsonReader(
            readResult.Buffer.Slice(state.UnconsumedParsedBytes),
            readResult.IsCompleted,
            state.ReaderState
        );

        bool hasToken = reader.Read();

        while (hasToken)
        {
            var directive = state.Advance(reader, pattern.Segments);

            switch (directive)
            {
                case ParserDirective.Skip:
                case ParserDirective.Capture:
                    break;

                case ParserDirective.BeginCapture:
                    // CaptureStartOffset recorded inside Advance; re-present StartObject/StartArray
                    // to the capture phase so _captureDepth increments from 0 to 1.
                    continue;

                case ParserDirective.YieldValue:
                {
                    var transformerInput = state.CalculateValueSlice(readResult.Buffer);
                    framer.FinishItem(writers);
                    transformer(transformerInput, writers);
                    break;
                }

                case ParserDirective.EndCapture:
                {
                    var transformerInput = state.CalculateValueSlice(readResult.Buffer);
                    framer.FinishItem(writers);
                    transformer(transformerInput, writers);
                    break;
                }
            }

            hasToken = reader.Read();
        }

        return state.CompleteSegment(reader.CurrentState, reader.BytesConsumed);
    }

    private sealed class FilterStateMachine
    {
        private int _depth = -1;
        private int _matchedDepth;
        private readonly bool[] _isArray = new bool[64];
        private readonly int[] _matchedDepthStack = new int[64];
        private bool _pendingPropertyMatches;
        private int _captureDepth;
        // Absolute offsets relative to the current readResult.Buffer.
        // _captureStartOffset: where the current capture began (set on BeginCapture / re-anchored on BeginSegment).
        // _valueStart/_valueEnd: bounds of the most recently yielded value (set on YieldValue / EndCapture).
        private long _captureStartOffset;
        private long _valueStart;
        private long _valueEnd;

        public bool IsCapturing { get; private set; }
        public JsonReaderState ReaderState { get; private set; }
        public long UnconsumedParsedBytes { get; private set; }

        public FilterStateMachine(JsonReaderState readerState) => ReaderState = readerState;

        /// <summary>
        /// Called once per pipe-read iteration, before the <see cref="Utf8JsonReader"/> loop.
        /// When a cross-buffer capture is in progress the pipe has been anchored at the capture
        /// start, so the new buffer begins exactly there — re-anchor <c>_captureStartOffset</c>
        /// to 0 so <see cref="CalculateValueSlice"/> stays correct.
        /// </summary>
        public void BeginSegment()
        {
            if (IsCapturing)
                _captureStartOffset = 0;
        }

        /// <summary>
        /// Returns the slice of <paramref name="buffer"/> that corresponds to the value most
        /// recently signalled by <see cref="Advance"/> via <c>YieldValue</c> or <c>EndCapture</c>.
        /// </summary>
        public ReadOnlySequence<byte> CalculateValueSlice(ReadOnlySequence<byte> buffer)
            => buffer.Slice(_valueStart, _valueEnd - _valueStart);

        /// <summary>
        /// Full variant for byte-range capture callers.  Saves reader state, then returns the
        /// position callers should pass to <c>PipeReader.AdvanceTo</c>.  When a capture is in
        /// progress the method anchors at <c>_captureStartOffset</c> so those bytes are
        /// re-presented in the next read; <see cref="UnconsumedParsedBytes"/> is set accordingly
        /// so the next segment skips re-parsing them.
        /// </summary>
        /// <param name="readerBytesConsumed">
        /// <c>reader.BytesConsumed</c> — relative to the slice passed to <see cref="Utf8JsonReader"/>,
        /// i.e. relative to <see cref="UnconsumedParsedBytes"/> within the current buffer.
        /// </param>
        public long CompleteSegment(JsonReaderState readerState, long readerBytesConsumed)
        {
            long absoluteConsumed = UnconsumedParsedBytes + readerBytesConsumed;
            ReaderState = readerState;
            if (IsCapturing)
            {
                UnconsumedParsedBytes = absoluteConsumed - _captureStartOffset;
                return _captureStartOffset;
            }
            UnconsumedParsedBytes = 0;
            return absoluteConsumed;
        }

        public ParserDirective Advance(Utf8JsonReader reader, byte[][] pattern)
        {
            var tokenType = reader.TokenType;

            // ── CAPTURE PHASE ─────────────────────────────────────────
            if (IsCapturing)
            {
                if (tokenType is JsonTokenType.StartObject or JsonTokenType.StartArray)
                    _captureDepth++;
                else if (tokenType is JsonTokenType.EndObject or JsonTokenType.EndArray)
                    _captureDepth--;

                if (_captureDepth == 0)
                {
                    IsCapturing = false;
                    _valueStart = _captureStartOffset;
                    _valueEnd   = UnconsumedParsedBytes + reader.BytesConsumed;
                    return ParserDirective.EndCapture;
                }

                return ParserDirective.Capture;
            }

            // ── SEARCH PHASE ──────────────────────────────────────────
            switch (tokenType)
            {
                case JsonTokenType.PropertyName:
                    _pendingPropertyMatches = _matchedDepth == _depth
                        && MatchesPropertyName(pattern, _matchedDepth, reader);
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
                        IsCapturing = true;
                        _captureDepth = 0;
                        _captureStartOffset = UnconsumedParsedBytes + reader.TokenStartIndex;
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
                    {
                        _valueStart = UnconsumedParsedBytes + reader.TokenStartIndex;
                        _valueEnd   = UnconsumedParsedBytes + reader.BytesConsumed;
                        return ParserDirective.YieldValue;
                    }

                    return ParserDirective.Skip;
                }
            }
        }

        private static bool MatchesSegment(int matchedDepth, byte[][] pattern, bool parentIsArray, bool pendingPropertyMatches)
        {
            if (matchedDepth >= pattern.Length) return false;
            var seg = pattern[matchedDepth];
            if (seg.Length == 0) return parentIsArray;
            return !parentIsArray && pendingPropertyMatches;
        }

        private static bool MatchesPropertyName(byte[][] pattern, int matchedDepth, Utf8JsonReader reader)
        {
            if (matchedDepth >= pattern.Length) return false;
            var expected = pattern[matchedDepth];
            if (expected.Length == 0) return false;
            return reader.HasValueSequence
                ? reader.ValueSequence.SequenceEqual(expected)
                : reader.ValueSpan.SequenceEqual(expected);
        }
    }

    private enum ParserDirective
    {
        Skip,
        YieldValue,
        BeginCapture,
        Capture,
        EndCapture,
    }
}
