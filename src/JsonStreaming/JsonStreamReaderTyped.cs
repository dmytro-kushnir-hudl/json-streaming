using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace JsonStreaming;

/// <summary>
/// Type-safe overloads for <see cref="JsonStreamReader"/> using
/// <c>System.Text.Json</c> source generators. These eliminate the
/// boilerplate of constructing <see cref="Utf8JsonReader"/> and calling
/// <see cref="JsonSerializer"/> manually.
///
/// All overloads accept <see cref="JsonTypeInfo{T}"/> for AOT-compatible,
/// zero-reflection deserialization and serialization.
/// </summary>
public static class JsonStreamReaderTyped
{
    // ── Typed callback ─────────────────────────────────────────────────────

    /// <summary>
    /// Navigates to the target array and invokes <paramref name="processItem"/>
    /// with each element deserialized as <typeparamref name="T"/>.
    /// </summary>
    public static Task<int> ProcessArrayAsync<T>(
        PipeReader pipeReader,
        JsonPath path,
        JsonTypeInfo<T> typeInfo,
        Action<T> processItem,
        CancellationToken ct = default
    ) =>
        JsonStreamReader.ProcessArrayAsync(
            pipeReader,
            path,
            itemBytes =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                var item = JsonSerializer.Deserialize(ref reader, typeInfo);
                if (item is not null)
                    processItem(item);
            },
            ct
        );

    /// <summary>
    /// Convenience overload accepting a dot-separated path string.
    /// </summary>
    public static Task<int> ProcessArrayAsync<T>(
        PipeReader pipeReader,
        string path,
        JsonTypeInfo<T> typeInfo,
        Action<T> processItem,
        CancellationToken ct = default
    ) =>
        ProcessArrayAsync(
            pipeReader,
            JsonPathNavigator.ParseDotPath(path),
            typeInfo,
            processItem,
            ct
        );

    // ── Typed write-through (TIn → TOut) ───────────────────────────────────

    /// <summary>
    /// Deserializes each item as <typeparamref name="TIn"/>, transforms it to
    /// <typeparamref name="TOut"/> via <paramref name="transform"/>, and serializes
    /// the result to <paramref name="writer"/>.
    ///
    /// If <paramref name="transform"/> returns <c>null</c>, the item is skipped
    /// (filtered out). This enables type-safe streaming with filtering.
    /// </summary>
    public static Task<int> WriteArrayAsync<TIn, TOut>(
        PipeReader pipeReader,
        JsonPath path,
        Utf8JsonWriter writer,
        JsonTypeInfo<TIn> inputType,
        JsonTypeInfo<TOut> outputType,
        Func<TIn, TOut?> transform,
        CancellationToken ct = default
    ) =>
        WriteArrayAsync(pipeReader, path, writer, inputType, outputType, transform, WriteOptions.Default, ct);

    /// <summary>
    /// Typed write-through with explicit <see cref="WriteOptions"/>.
    /// </summary>
    public static Task<int> WriteArrayAsync<TIn, TOut>(
        PipeReader pipeReader,
        JsonPath path,
        Utf8JsonWriter writer,
        JsonTypeInfo<TIn> inputType,
        JsonTypeInfo<TOut> outputType,
        Func<TIn, TOut?> transform,
        WriteOptions options,
        CancellationToken ct = default
    ) =>
        JsonStreamReader.WriteArrayAsync(
            pipeReader,
            path,
            writer,
            (itemBytes, w) =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                var input = JsonSerializer.Deserialize(ref reader, inputType);
                if (input is null)
                    return;

                var output = transform(input);
                if (output is not null)
                    JsonSerializer.Serialize(w, output, outputType);
            },
            options,
            ct
        );

    /// <summary>
    /// Convenience overload accepting a dot-separated path string.
    /// </summary>
    public static Task<int> WriteArrayAsync<TIn, TOut>(
        PipeReader pipeReader,
        string path,
        Utf8JsonWriter writer,
        JsonTypeInfo<TIn> inputType,
        JsonTypeInfo<TOut> outputType,
        Func<TIn, TOut?> transform,
        CancellationToken ct = default
    ) =>
        WriteArrayAsync(pipeReader, JsonPathNavigator.ParseDotPath(path), writer, inputType, outputType, transform, ct);

    /// <summary>
    /// Convenience overload: dot-separated path + explicit options.
    /// </summary>
    public static Task<int> WriteArrayAsync<TIn, TOut>(
        PipeReader pipeReader,
        string path,
        Utf8JsonWriter writer,
        JsonTypeInfo<TIn> inputType,
        JsonTypeInfo<TOut> outputType,
        Func<TIn, TOut?> transform,
        WriteOptions options,
        CancellationToken ct = default
    ) =>
        WriteArrayAsync(pipeReader, JsonPathNavigator.ParseDotPath(path), writer, inputType, outputType, transform, options, ct);

    // ── Typed direct-write (TIn → write directly, no TOut allocation) ──────

    /// <summary>
    /// Deserializes each item as <typeparamref name="T"/> and passes it with the
    /// <see cref="Utf8JsonWriter"/> to <paramref name="writeItem"/>. The caller
    /// writes directly to the writer — no output type allocation.
    ///
    /// This is the sweet spot: type-safe input + zero output allocation.
    /// </summary>
    public static Task<int> WriteArrayAsync<T>(
        PipeReader pipeReader,
        JsonPath path,
        Utf8JsonWriter writer,
        JsonTypeInfo<T> inputType,
        Action<T, Utf8JsonWriter> writeItem,
        CancellationToken ct = default
    ) =>
        WriteArrayAsync(pipeReader, path, writer, inputType, writeItem, WriteOptions.Default, ct);

    /// <summary>
    /// Typed direct-write with explicit <see cref="WriteOptions"/>.
    /// </summary>
    public static Task<int> WriteArrayAsync<T>(
        PipeReader pipeReader,
        JsonPath path,
        Utf8JsonWriter writer,
        JsonTypeInfo<T> inputType,
        Action<T, Utf8JsonWriter> writeItem,
        WriteOptions options,
        CancellationToken ct = default
    ) =>
        JsonStreamReader.WriteArrayAsync(
            pipeReader,
            path,
            writer,
            (itemBytes, w) =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                var input = JsonSerializer.Deserialize(ref reader, inputType);
                if (input is not null)
                    writeItem(input, w);
            },
            options,
            ct
        );

    /// <summary>
    /// Convenience overload accepting a dot-separated path string.
    /// </summary>
    public static Task<int> WriteArrayAsync<T>(
        PipeReader pipeReader,
        string path,
        Utf8JsonWriter writer,
        JsonTypeInfo<T> inputType,
        Action<T, Utf8JsonWriter> writeItem,
        CancellationToken ct = default
    ) =>
        WriteArrayAsync(pipeReader, JsonPathNavigator.ParseDotPath(path), writer, inputType, writeItem, ct);

    /// <summary>
    /// Convenience overload: dot-separated path + explicit options.
    /// </summary>
    public static Task<int> WriteArrayAsync<T>(
        PipeReader pipeReader,
        string path,
        Utf8JsonWriter writer,
        JsonTypeInfo<T> inputType,
        Action<T, Utf8JsonWriter> writeItem,
        WriteOptions options,
        CancellationToken ct = default
    ) =>
        WriteArrayAsync(pipeReader, JsonPathNavigator.ParseDotPath(path), writer, inputType, writeItem, options, ct);

    // ── Typed verbatim (passthrough raw bytes, no deserialize) ─────────────

    /// <summary>
    /// Writes each item's raw bytes directly to <paramref name="writer"/>
    /// without deserialization. Use when you just need verbatim passthrough
    /// with type-safe API consistency.
    ///
    /// Previously this deserialized + re-serialized — now it skips the
    /// round-trip entirely and copies raw bytes via <see cref="JsonDocument"/>.
    /// </summary>
    public static Task<int> WriteArrayAsync<T>(
        PipeReader pipeReader,
        JsonPath path,
        Utf8JsonWriter writer,
        JsonTypeInfo<T> typeInfo,
        CancellationToken ct = default
    ) =>
        // Skip deserialize/serialize round-trip — just copy raw bytes
        JsonStreamReader.WriteArrayAsync(pipeReader, path, writer, ct);

    /// <summary>
    /// Convenience overload accepting a dot-separated path string.
    /// </summary>
    public static Task<int> WriteArrayAsync<T>(
        PipeReader pipeReader,
        string path,
        Utf8JsonWriter writer,
        JsonTypeInfo<T> typeInfo,
        CancellationToken ct = default
    ) =>
        WriteArrayAsync(pipeReader, JsonPathNavigator.ParseDotPath(path), writer, typeInfo, ct);
}
