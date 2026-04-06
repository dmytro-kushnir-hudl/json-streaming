using System.IO.Pipelines;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace JsonStreaming;

/// <summary>
/// High-level pipeline that handles the full streaming flow:
/// read from input → navigate → transform → write envelope → flush.
///
/// Wraps the output in a JSON envelope: <c>{"&lt;arrayName&gt;": [...], "count": N}</c>,
/// with error handling that produces valid JSON even on failure.
/// </summary>
public static class JsonStreamPipeline
{
    /// <summary>
    /// Reads items from <paramref name="input"/>, transforms each from
    /// <typeparamref name="TIn"/> to <typeparamref name="TOut"/>, and writes
    /// a complete JSON envelope to <paramref name="output"/>.
    ///
    /// Handles: writer setup, JSON envelope, threshold-based flush with
    /// backpressure, error recovery (always produces valid JSON), and cleanup.
    ///
    /// Returns the number of items written.
    /// </summary>
    public static Task<int> TransformArrayAsync<TIn, TOut>(
        PipeReader input,
        string sourcePath,
        PipeWriter output,
        string outputArrayName,
        JsonTypeInfo<TIn> inputType,
        JsonTypeInfo<TOut> outputType,
        Func<TIn, TOut?> transform,
        CancellationToken ct = default
    ) =>
        TransformArrayAsync(
            input,
            JsonPathNavigator.ParseDotPath(sourcePath),
            output,
            outputArrayName,
            inputType,
            outputType,
            transform,
            ct
        );

    /// <summary>
    /// Overload accepting a <see cref="JsonPath"/> for navigation.
    /// </summary>
    public static async Task<int> TransformArrayAsync<TIn, TOut>(
        PipeReader input,
        JsonPath sourcePath,
        PipeWriter output,
        string outputArrayName,
        JsonTypeInfo<TIn> inputType,
        JsonTypeInfo<TOut> outputType,
        Func<TIn, TOut?> transform,
        CancellationToken ct = default
    )
    {
        await using var writer = new Utf8JsonWriter(output);
        var options = new WriteOptions
        {
            AsyncFlush = async flushCt =>
            {
                await output.FlushAsync(flushCt);
            },
        };

        int written = 0;
        string? error = null;

        try
        {
            writer.WriteStartObject();
            writer.WriteStartArray(outputArrayName);

            // Track actual writes, not total items processed — transform may return null (filter)
            await JsonStreamReaderTyped.WriteArrayAsync(
                input,
                sourcePath,
                writer,
                inputType,
                outputType,
                item =>
                {
                    var result = transform(item);
                    if (result is not null)
                        written++;
                    return result;
                },
                options,
                ct
            );

            writer.WriteEndArray();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Client disconnected — best-effort close
            TryCloseTokens(writer);
            return written;
        }
        catch (Exception ex)
        {
            error = ex.Message;
            TryCloseTokens(writer);
        }

        writer.WriteNumber("count"u8, written);
        if (error is not null)
            writer.WriteString("error"u8, error);
        writer.WriteEndObject();

        await writer.FlushAsync(ct);
        await output.FlushAsync(ct);

        return written;
    }

    /// <summary>
    /// Verbatim passthrough: reads items from <paramref name="input"/> and writes
    /// them unmodified to <paramref name="output"/> inside a JSON envelope.
    /// </summary>
    public static async Task<int> PassthroughArrayAsync(
        PipeReader input,
        string sourcePath,
        PipeWriter output,
        string outputArrayName,
        CancellationToken ct = default
    )
    {
        await using var writer = new Utf8JsonWriter(output);
        var options = new WriteOptions
        {
            AsyncFlush = async flushCt =>
            {
                await output.FlushAsync(flushCt);
            },
        };

        int written = 0;
        string? error = null;

        try
        {
            writer.WriteStartObject();
            writer.WriteStartArray(outputArrayName);

            await JsonStreamReader.WriteArrayAsync(
                input,
                sourcePath,
                writer,
                (itemBytes, w) =>
                {
                    using var doc = JsonDocument.Parse(itemBytes);
                    doc.RootElement.WriteTo(w);
                    written++;
                },
                options,
                ct
            );

            writer.WriteEndArray();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            TryCloseTokens(writer);
            return written;
        }
        catch (Exception ex)
        {
            error = ex.Message;
            TryCloseTokens(writer);
        }

        writer.WriteNumber("count"u8, written);
        if (error is not null)
            writer.WriteString("error"u8, error);
        writer.WriteEndObject();

        await writer.FlushAsync(ct);
        await output.FlushAsync(ct);

        return written;
    }

    /// <summary>
    /// Overload accepting a <see cref="JsonPath"/> for navigation.
    /// </summary>
    public static async Task<int> PassthroughArrayAsync(
        PipeReader input,
        JsonPath sourcePath,
        PipeWriter output,
        string outputArrayName,
        CancellationToken ct = default
    )
    {
        await using var writer = new Utf8JsonWriter(output);
        var options = new WriteOptions
        {
            AsyncFlush = async flushCt =>
            {
                await output.FlushAsync(flushCt);
            },
        };

        int written = 0;
        string? error = null;

        try
        {
            writer.WriteStartObject();
            writer.WriteStartArray(outputArrayName);

            await JsonStreamReader.WriteArrayAsync(
                input,
                sourcePath,
                writer,
                (itemBytes, w) =>
                {
                    using var doc = JsonDocument.Parse(itemBytes);
                    doc.RootElement.WriteTo(w);
                    written++;
                },
                options,
                ct
            );

            writer.WriteEndArray();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            TryCloseTokens(writer);
            return written;
        }
        catch (Exception ex)
        {
            error = ex.Message;
            TryCloseTokens(writer);
        }

        writer.WriteNumber("count"u8, written);
        if (error is not null)
            writer.WriteString("error"u8, error);
        writer.WriteEndObject();

        await writer.FlushAsync(ct);
        await output.FlushAsync(ct);

        return written;
    }

    private static void TryCloseTokens(Utf8JsonWriter writer)
    {
        for (int i = 0; i < 10 && writer.CurrentDepth > 1; i++)
        {
            try { writer.WriteEndArray(); continue; }
            catch (InvalidOperationException) { }
            try { writer.WriteEndObject(); continue; }
            catch (InvalidOperationException) { }
            try { writer.WriteNullValue(); continue; }
            catch (InvalidOperationException) { }
            break;
        }
    }
}
