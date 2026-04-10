using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;

namespace JsonStreaming;

/// <summary>
///     Streaming JSON transcoding: format, minify, or project arbitrary JSON from a
///     <see cref="PipeReader" /> to a <see cref="PipeWriter" /> without buffering the
///     whole document.
///     <list type="bullet">
///         <item><see cref="ProxyFormattedJsonAsync" /> — pretty-print (2-space indent)</item>
///         <item><see cref="ProxyMinifiedJsonAsync" /> — strip whitespace</item>
///         <item><see cref="TransformItemsAsync" /> — extract matched values with a per-item callback</item>
///     </list>
///     All methods respect backpressure: they flush to the writer when the unflushed
///     buffer exceeds 16 KB.
/// </summary>
public static partial class JsonTranscoder
{
    private const int FlushThreshold = 16 * 1024;

    // ── Pretty-print ──────────────────────────────────────────────────────────

    /// <summary>
    ///     Reads JSON from <paramref name="reader" /> and writes it formatted with
    ///     2-space indentation to <paramref name="writer" />.
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
    ///     Reads JSON from <paramref name="reader" /> and writes it with all whitespace
    ///     stripped to <paramref name="writer" />.
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
            for (var i = 0; i < depth; i++)
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
                _ => true
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
        public bool AfterColon;
        public int Depth;
        public bool NeedsComma;
        public JsonReaderState State;
    }

    private sealed class MinifiedState
    {
        public bool NeedsComma;
        public JsonReaderState State;
    }
}