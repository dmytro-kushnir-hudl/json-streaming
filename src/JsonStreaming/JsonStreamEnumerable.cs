using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace JsonStreaming;

/// <summary>
/// <see cref="IAsyncEnumerable{T}"/> wrappers over <see cref="JsonStreamReader"/>.
/// Each method navigates to the target array and yields items as <see cref="JsonDocument"/>.
/// The caller must dispose each document.
///
/// These are convenience wrappers — for zero-copy processing, use
/// <see cref="JsonStreamReader.ProcessArrayAsync(PipeReader, JsonPath, Action{System.Buffers.ReadOnlySequence{byte}}, CancellationToken)"/> directly.
/// </summary>
public static class JsonStreamEnumerable
{
    /// <summary>
    /// Navigates to the target array(s) and yields each element as a <see cref="JsonDocument"/>.
    /// </summary>
    public static IAsyncEnumerable<JsonDocument> EnumerateArrayAsync(
        PipeReader pipeReader,
        JsonPath path,
        CancellationToken ct = default
    ) =>
        JsonPathNavigator.HasEach(path)
            ? EnumerateSelectManyAsync(pipeReader, path, ct)
            : EnumerateSimpleAsync(pipeReader, path, ct);

    /// <summary>
    /// Convenience overload accepting a dot-separated path string.
    /// </summary>
    public static IAsyncEnumerable<JsonDocument> EnumerateArrayAsync(
        PipeReader pipeReader,
        string path,
        CancellationToken ct = default
    ) => EnumerateSimpleAsync(pipeReader, JsonPathNavigator.ParseDotPath(path), ct);

    private static async IAsyncEnumerable<JsonDocument> EnumerateSimpleAsync(
        PipeReader pipeReader,
        JsonPath path,
        [EnumeratorCancellation] CancellationToken ct = default
    )
    {
        var navState = await JsonPathNavigator.NavigateToArrayAsync(pipeReader, path, ct);
        if (navState is null)
            yield break;

        await foreach (var doc in CollectAndYieldAsync(
            (processItem) => JsonStreamReader.IterateItemsAsync(pipeReader, navState.Value, processItem, ct)))
            yield return doc;
    }

    private static async IAsyncEnumerable<JsonDocument> EnumerateSelectManyAsync(
        PipeReader pipeReader,
        JsonPath path,
        [EnumeratorCancellation] CancellationToken ct = default
    )
    {
        var (prefix, suffix) = JsonPathNavigator.SplitAtEach(path);
        var suffixNames = JsonPathNavigator.ExtractPropertyNames(suffix);

        var outerState = await JsonPathNavigator.NavigateToArrayAsync(pipeReader, prefix, ct);
        if (outerState is null)
            yield break;

        if (suffixNames.Length == 0)
        {
            await foreach (var doc in CollectAndYieldAsync(
                (processItem) => JsonStreamReader.IterateItemsAsync(pipeReader, outerState.Value, processItem, ct)))
                yield return doc;
            yield break;
        }

        await foreach (var doc in CollectAndYieldAsync(
            (processItem) => JsonStreamReader.IterateSelectManyAsync(pipeReader, outerState.Value, suffixNames, processItem, ct)))
            yield return doc;
    }

    /// <summary>
    /// Runs the core callback-based iteration, collecting parsed JsonDocuments
    /// into a list, then yields them. The callback is synchronous — it just
    /// parses the byte span into a JsonDocument and adds it to the list.
    /// </summary>
    private static async IAsyncEnumerable<JsonDocument> CollectAndYieldAsync(
        Func<System.Action<System.Buffers.ReadOnlySequence<byte>>, Task<int>> iterate,
        [EnumeratorCancellation] CancellationToken ct = default
    )
    {
        var items = new List<JsonDocument>();
        await iterate(itemBytes => items.Add(JsonDocument.Parse(itemBytes)));

        foreach (var doc in items)
        {
            ct.ThrowIfCancellationRequested();
            yield return doc;
        }
    }
}
