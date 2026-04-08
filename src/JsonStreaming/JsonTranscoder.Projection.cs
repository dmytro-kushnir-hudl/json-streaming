using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;

namespace JsonStreaming;

public delegate void Transformer(ReadOnlySequence<byte> itemBytes, PooledByteBufferWriter output);

public static partial class JsonTranscoder
{
    /// <summary>
    /// Reads JSON from <paramref name="reader"/>, navigates to each value matching
    /// <paramref name="path"/>, and invokes <paramref name="processItem"/> with the
    /// raw bytes of each matched value. The callback receives the item bytes as a
    /// <see cref="ReadOnlySequence{T}"/> (valid only during the call) and the
    /// <paramref name="writer"/> for output.
    /// </summary>
    public static async Task TransformItemsAsync(
        this PipeReader input,
        PipeWriter output,
        JsonPath selector,
        Transformer transformer,
        JsonReaderOptions options = default,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(selector);

        var state = new FilterStateMachine { ReaderState = new JsonReaderState(options) };
        var renderer = new VerbatimRenderer();
        var framer = new NdJsonFramer();
        ct.ThrowIfCancellationRequested();

        framer.BeginDocument(output);

        while (true)
        {
            var result = await input.ReadAsync(ct);
            var buffer = result.Buffer;
            var consumed = buffer.Start;

            try
            {
                if (result.IsCanceled)
                    throw new OperationCanceledException(ct);

                var bytesConsumed = WriteTransformation(state, result, output, selector, transformer, ref framer);
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

        framer.EndDocument(output);
    }
    
    private static long WriteTransformation<TFramer>(
        FilterStateMachine state,
        ReadResult readResult,
        PipeWriter output,
        JsonPath pattern,
        Transformer transformer,
        ref TFramer framer)
        where TFramer : struct, IItemFramer
    {
        long alreadyParsed = state.UnconsumedParsedBytes;
        var unparsedBuffer = readResult.Buffer.Slice(alreadyParsed);

        var reader = new Utf8JsonReader(
            unparsedBuffer,
            readResult.IsCompleted,
            state.ReaderState
        );

        // If we entered this method already capturing, the capture started in a previous read.
        // Therefore, relative to the current readResult.Buffer, it starts exactly at index 0.
        long captureStartOffset = state.IsCapturing ? 0 : -1;

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
                    // Record absolute start position relative to the entire ReadResult buffer,
                    // then re-present StartObject/StartArray to the capture phase so _captureDepth
                    // increments from 0 to 1 — matching the EndObject/EndArray that closes it.
                    captureStartOffset = alreadyParsed + reader.TokenStartIndex;
                    continue;

                case ParserDirective.YieldValue:
                {
                    long start = alreadyParsed + reader.TokenStartIndex;
                    long end = alreadyParsed + reader.BytesConsumed;
                    var slice = readResult.Buffer.Slice(start, end - start);
                    
                    var pooledWriter = new PooledByteBufferWriter(output);
                    transformer(slice, pooledWriter);
                    framer.FinishItem(output);
                    break;
                }

                case ParserDirective.EndCapture:
                {
                    long start = captureStartOffset;
                    long end = alreadyParsed + reader.BytesConsumed;
                    var sequence = readResult.Buffer.Slice(start, end - start);

                    transformer(sequence, new PooledByteBufferWriter(output));
                    
                    framer.FinishItem(output);
                    
                    captureStartOffset = -1;
                    break;
                }
            }

            hasToken = reader.Read();
        }

        state.ReaderState = reader.CurrentState;
        long absoluteConsumed = alreadyParsed + reader.BytesConsumed;

        // --- THE PIPE FRAMING TRICK ---
        if (state.IsCapturing)
        {
            // We cannot consume past captureStartOffset because we need those bytes to construct 
            // the full ReadOnlySequence next time. Tell the caller to anchor 'consumed' here.
            long consumeUpTo = captureStartOffset;
            
            // The next buffer will start at 'consumeUpTo'. We have already successfully parsed 
            // up to 'absoluteConsumed'. So next time, we can safely skip re-parsing the difference.
            state.UnconsumedParsedBytes = absoluteConsumed - consumeUpTo;
            
            return consumeUpTo;
        }

        // We aren't capturing, so it is safe to consume everything we've successfully parsed.
        state.UnconsumedParsedBytes = 0;
        return absoluteConsumed;

    }

    private sealed class FilterStateMachine
    {
        private int _depth = -1;
        private int _matchedDepth;
        private readonly bool[] _isArray = new bool[64];
        private readonly int[] _matchedDepthStack = new int[64];
        private bool _pendingPropertyMatches;
        private int _captureDepth;
        
        public bool IsCapturing { get; private set; }
        public JsonReaderState ReaderState;
        
        // Tracks how many bytes in the pipe's current buffer we've already parsed.
        // This prevents Utf8JsonReader from re-parsing the same object chunks over and over.
        public long UnconsumedParsedBytes; 

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

        // Keep MatchesSegment and MatchesPropertyName exactly as they were...
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
            return reader.HasValueSequence ? reader.ValueSequence.SequenceEqual(expected) : reader.ValueSpan.SequenceEqual(expected);
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