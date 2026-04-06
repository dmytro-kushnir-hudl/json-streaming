using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;

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

    // ── Simple path ────────────────────────────────────────────────────────

    private static async IAsyncEnumerable<JsonDocument> EnumerateSimpleAsync(
        PipeReader pipeReader,
        JsonPath path,
        [EnumeratorCancellation] CancellationToken ct = default
    )
    {
        var navState = await JsonPathNavigator.NavigateToArrayAsync(pipeReader, path, ct);
        if (navState is null)
            yield break;

        // Reuse the core callback loop via a bounded channel.
        // Channel capacity 1: the producer blocks after writing one item until
        // the consumer yields it, so memory stays bounded.
        var channel = Channel.CreateUnbounded<JsonDocument>(
            new UnboundedChannelOptions { SingleWriter = true, SingleReader = true }
        );

        var producer = ProduceAsync(
            () =>
                JsonStreamReader.IterateItemsAsync(
                    pipeReader,
                    navState.Value,
                    itemBytes => channel.Writer.TryWrite(JsonDocument.Parse(itemBytes)),
                    ct
                ),
            channel.Writer,
            ct
        );

        await foreach (var doc in channel.Reader.ReadAllAsync(ct))
            yield return doc;

        await producer;
    }

    // ── Select-many path ───────────────────────────────────────────────────

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

        var channel = Channel.CreateUnbounded<JsonDocument>(
            new UnboundedChannelOptions { SingleWriter = true, SingleReader = true }
        );

        Func<Task<int>> iterateFunc =
            suffixNames.Length == 0
                ? () =>
                    JsonStreamReader.IterateItemsAsync(
                        pipeReader,
                        outerState.Value,
                        itemBytes => channel.Writer.TryWrite(JsonDocument.Parse(itemBytes)),
                        ct
                    )
                : () =>
                    JsonStreamReader.IterateSelectManyAsync(
                        pipeReader,
                        outerState.Value,
                        suffixNames,
                        itemBytes => channel.Writer.TryWrite(JsonDocument.Parse(itemBytes)),
                        ct
                    );

        var producer = ProduceAsync(iterateFunc, channel.Writer, ct);

        await foreach (var doc in channel.Reader.ReadAllAsync(ct))
            yield return doc;

        await producer;
    }

    // ── Producer helper ────────────────────────────────────────────────────

    private static async Task ProduceAsync(
        Func<Task<int>> iterate,
        ChannelWriter<JsonDocument> writer,
        CancellationToken ct
    )
    {
        try
        {
            await iterate();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            writer.TryComplete(ex);
            return;
        }
        writer.TryComplete();
    }
}
