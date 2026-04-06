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
    ) => ProcessArrayAsync(pipeReader, JsonPathNavigator.ParseDotPath(path), typeInfo, processItem, ct);

    // ── Typed write-through (transform) ────────────────────────────────────

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
    ) => WriteArrayAsync(pipeReader, path, writer, inputType, outputType, transform, WriteOptions.Default, ct);

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
    ) => WriteArrayAsync(pipeReader, JsonPathNavigator.ParseDotPath(path), writer, inputType, outputType, transform, ct);

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
    ) => WriteArrayAsync(pipeReader, JsonPathNavigator.ParseDotPath(path), writer, inputType, outputType, transform, options, ct);

    // ── Typed verbatim write (same type in and out) ────────────────────────

    /// <summary>
    /// Deserializes each item as <typeparamref name="T"/> and serializes it
    /// back to <paramref name="writer"/>. Useful for re-serializing with
    /// different <see cref="JsonSerializerOptions"/> (e.g. camelCase output
    /// from PascalCase source).
    /// </summary>
    public static Task<int> WriteArrayAsync<T>(
        PipeReader pipeReader,
        JsonPath path,
        Utf8JsonWriter writer,
        JsonTypeInfo<T> typeInfo,
        CancellationToken ct = default
    ) => WriteArrayAsync(pipeReader, path, writer, typeInfo, typeInfo, item => item, ct);

    /// <summary>
    /// Convenience overload accepting a dot-separated path string.
    /// </summary>
    public static Task<int> WriteArrayAsync<T>(
        PipeReader pipeReader,
        string path,
        Utf8JsonWriter writer,
        JsonTypeInfo<T> typeInfo,
        CancellationToken ct = default
    ) => WriteArrayAsync(pipeReader, JsonPathNavigator.ParseDotPath(path), writer, typeInfo, ct);
}
