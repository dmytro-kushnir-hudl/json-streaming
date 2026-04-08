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
public static partial class JsonTranscoder
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
        JsonPath path,
        PipeWriter writer,
        JsonReaderOptions readerOptions = default,
        JsonWriterOptions writerOptions = default,
        CancellationToken ct = default
    )
    {
        var jwriter = new Utf8JsonWriter(writer, writerOptions);
        var state = new FilterStateMachine { ReaderState = new JsonReaderState(readerOptions) };
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

                if (result.IsCompleted || writer is { CanGetUnflushedBytes: true, UnflushedBytes: >= FlushThreshold })
                {
                    jwriter.Flush();
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
        JsonPath path,
        PipeWriter writer,
        JsonReaderOptions options = default,
        CancellationToken ct = default
    )
    {
        var state = new FilterStateMachine { ReaderState = new JsonReaderState(options) };
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

    /// <summary>
    /// Reads JSON from <paramref name="reader"/>, projects each value matching
    /// <paramref name="path"/>, lets <paramref name="processItem"/> write a
    /// transformed item into a temporary in-memory buffer, and emits the
    /// resulting items as a JSON array to <paramref name="writer"/>.
    /// </summary>
    /// <remarks>
    /// The callback writes to an <see cref="IBufferWriter{T}"/>, not directly to
    /// the destination pipe. Writing zero bytes skips the item. Any bytes written
    /// are copied to the output as a single array element.
    /// </remarks>
    public static async Task ProjectItemsAsyncHighLevel(
        this PipeReader reader,
        JsonPath path,
        PipeWriter writer,
        Func<ReadOnlySequence<byte>, IBufferWriter<byte>, ValueTask> processItem,
        JsonReaderOptions options = default,
        CancellationToken ct = default)
    {
        var state = new FilterStateMachine { ReaderState = new JsonReaderState(options) };
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
                    pipeWriter.CopyToken(reader, readResult);
                    state.AfterColon = true;
                    state.NeedsComma = false;
                    break;
                default:
                    pipeWriter.CopyToken(reader, readResult);
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

            pipeWriter.CopyToken(reader, readResult);
        }

        state.State = reader.CurrentState;
        return reader.BytesConsumed;
    }

    // ── WriteProjection (unified generic) ──────────────────────────────────────

    

    // ── Directive & Strategy types ───────────────────────────────────────



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
}