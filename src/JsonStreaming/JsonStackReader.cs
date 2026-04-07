using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;
using System.Threading.Channels;

namespace JsonStreaming;

/// <summary>
/// Streaming JSON reader with stack-based path tracking.
///
/// A readable alternative to <see cref="JsonStreamReader"/>. Rather than pre-navigating
/// to a fixed array path, the caller supplies a predicate that receives the current JSON
/// path at each value token and decides whether to capture the upcoming value.
///
/// The path is a list of segments — <see cref="string"/> for property names,
/// <see cref="int"/> for array indices. Examples:
/// <list type="bullet">
///   <item><c>["users", 0]</c> — first element of the top-level "users" array</item>
///   <item><c>["data", "messages", 2]</c> — third element of data.messages</item>
///   <item><c>["responses", 1, "items", 0]</c> — select-many, no special code needed</item>
/// </list>
///
/// Backpressure: the async delegate passed to <see cref="ReadItemsAsync"/> is the pause point.
/// Iteration halts at each await, propagating consumer backpressure back to the <see cref="PipeReader"/>.
/// No flush loop or threshold required.
/// </summary>
public static class JsonStackReader
{
    // ── Public API ────────────────────────────────────────────────────────────

    /// <summary>
    /// Reads JSON from <paramref name="pipeReader"/> and calls <paramref name="onItem"/>
    /// for each value whose path the <paramref name="predicate"/> accepts.
    ///
    /// The captured bytes are sliced from the pipe's internal buffer — zero-copy and valid
    /// only until <paramref name="onItem"/> returns. Returns the number of items captured.
    /// </summary>
    public static Task<int> ReadItemsAsync(
        PipeReader pipeReader,
        Func<IReadOnlyList<object>, bool> predicate,
        Func<ReadOnlySequence<byte>, ValueTask> onItem,
        CancellationToken ct = default)
        => CoreLoopAsync(pipeReader, predicate, onItem, ct);

    /// <summary>
    /// Returns matching values as an <see cref="IAsyncEnumerable{T}"/> of owned byte arrays.
    ///
    /// Built on <see cref="ReadItemsAsync"/> via a bounded channel — no second state machine.
    /// Items are copied to <c>byte[]</c> so the caller owns the memory.
    /// Channel capacity (default 1) controls how many items may be queued ahead of the consumer;
    /// capacity 1 means the reader pauses as soon as the consumer falls one item behind.
    /// </summary>
    public static IAsyncEnumerable<byte[]> StreamItemsAsync(
        PipeReader pipeReader,
        Func<IReadOnlyList<object>, bool> predicate,
        int channelCapacity = 1,
        CancellationToken ct = default)
    {
        var channel = Channel.CreateBounded<byte[]>(channelCapacity);

        _ = ReadItemsAsync(
            pipeReader,
            predicate,
            item => channel.Writer.WriteAsync(item.ToArray(), ct),
            ct
        ).ContinueWith(
            t => channel.Writer.TryComplete(t.Exception?.InnerException),
            TaskScheduler.Default
        );

        return channel.Reader.ReadAllAsync(ct);
    }

    // ── Core loop ─────────────────────────────────────────────────────────────

    private static async Task<int> CoreLoopAsync(
        PipeReader pipeReader,
        Func<IReadOnlyList<object>, bool> predicate,
        Func<ReadOnlySequence<byte>, ValueTask> onItem,
        CancellationToken ct)
    {
        // path: one entry per entered container, representing the navigation path so far.
        //   string = property key, int = array element index.
        var path = new List<object>();

        // One frame per entered container (StartObject / StartArray not captured):
        //   isArray     — true for arrays, false for objects
        //   pushedEntry — whether this frame added an entry to `path`
        var containers = new Stack<(bool isArray, bool pushedEntry)>();

        // Current next-element index for each frame.
        // null for object frames (no numeric index); non-null for array frames.
        // Depth-matched to `containers`.
        var indices = new Stack<int?>();

        string? pendingKey = null;  // set by PropertyName token, consumed by the next value token
        var jsonState = new JsonReaderState();
        int count = 0;

        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var readResult = await pipeReader.ReadAsync(ct);
            var buffer = readResult.Buffer;

            // Snapshot the full stack state before processing this buffer.
            // If TrySkip fails mid-buffer, all tokens read so far will have
            // modified the stacks — we restore the snapshot and rewind the pipe
            // so the next ReadAsync re-reads from the same point with more data.
            var pathSnap = new List<object>(path);
            var containersSnap = containers.ToArray(); // top-first
            var indicesSnap = indices.ToArray();       // top-first
            var pendingKeySnap = pendingKey;

            bool hasCaptured = false;
            bool incomplete = false;
            ReadOnlySequence<byte> capturedBytes = default;
            SequencePosition resumePos = default;

            {   
                // ── Reader scope ──────────────────────────────────────────────────────
                //    Utf8JsonReader is a ref struct and cannot cross an await boundary.
                //    All state is checkpointed into jsonState before leaving this block.

                var reader = new Utf8JsonReader(buffer, isFinalBlock: readResult.IsCompleted, jsonState);
                bool stop = false;

                while (!stop && reader.Read())
                {
                    switch (reader.TokenType)
                    {
                        // Record property names; consumed by the next value token.
                        case JsonTokenType.PropertyName:
                            pendingKey = reader.GetString();
                            break;

                        // ── Value tokens: check predicate, then capture or enter ───────
                        case JsonTokenType.StartObject:
                        case JsonTokenType.StartArray:
                        case JsonTokenType.String:
                        case JsonTokenType.Number:
                        case JsonTokenType.True:
                        case JsonTokenType.False:
                        case JsonTokenType.Null:
                        {
                            // Compute this value's path segment without modifying state yet:
                            //   inside an array → current element index
                            //   inside an object / at root → property key (null at root)
                            object? segment = indices.TryPeek(out int? idx) && idx.HasValue
                                ? idx.Value
                                : pendingKey;

                            // Temporarily extend path so the predicate sees the full address.
                            if (segment is not null) path.Add(segment);
                            bool capture = predicate(path);
                            if (segment is not null) path.RemoveAt(path.Count - 1);

                            if (capture)
                            {
                                long start = reader.TokenStartIndex;

                                if (!reader.TrySkip())
                                {
                                    // Item straddles the current buffer boundary.
                                    // Earlier tokens in this buffer already modified the stacks,
                                    // so restore the snapshot taken at the top of this iteration.
                                    // Rewind the pipe to buffer.Start: next ReadAsync returns the
                                    // same data plus more, and all tokens are re-processed cleanly.
                                    path.Clear(); path.AddRange(pathSnap);
                                    RestoreStack(containers, containersSnap);
                                    RestoreStack(indices, indicesSnap);
                                    pendingKey = pendingKeySnap;
                                    pipeReader.AdvanceTo(buffer.Start, buffer.End);
                                    incomplete = true;
                                    stop = true;
                                    break;
                                }

                                // Slice raw bytes directly from the pipe buffer.
                                // The buffer is NOT advanced yet — bytes stay valid until
                                // after onItem() returns (see below).
                                capturedBytes = buffer.Slice(
                                    buffer.GetPosition(start),
                                    reader.BytesConsumed - start
                                );
                                hasCaptured = true;
                                resumePos = reader.Position;
                                jsonState = reader.CurrentState;
                                pendingKey = null;
                                IncrementIndex(indices); // one element consumed from parent array
                                stop = true;
                            }
                            else
                            {
                                pendingKey = null;

                                if (reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray)
                                {
                                    // Enter the container: commit the path entry and push a frame.
                                    bool pushed = segment is not null;
                                    if (pushed) path.Add(segment!);
                                    containers.Push((reader.TokenType == JsonTokenType.StartArray, pushed));
                                    indices.Push(reader.TokenType == JsonTokenType.StartArray ? 0 : null);
                                }
                                else
                                {
                                    // Primitive not captured: advance parent array's index.
                                    IncrementIndex(indices);
                                }
                            }
                            break;
                        }

                        // ── Container exits ───────────────────────────────────────────
                        case JsonTokenType.EndObject:
                        case JsonTokenType.EndArray:
                            if (containers.Count > 0)
                            {
                                var (_, pushedEntry) = containers.Pop();
                                indices.Pop();
                                if (pushedEntry) path.RemoveAt(path.Count - 1);
                                IncrementIndex(indices); // advance parent array's index
                            }
                            break;
                    }
                }

                if (!stop)
                {
                    // Normal end: consumed all tokens in this buffer.
                    jsonState = reader.CurrentState;
                    pipeReader.AdvanceTo(reader.Position, buffer.End);
                }
            }
            // ── Reader is out of scope — safe to await ────────────────────────────

            if (hasCaptured)
            {
                // Pipe buffer still held (no AdvanceTo yet).
                // capturedBytes points into the buffer and is valid for this call.
                await onItem(capturedBytes);
                count++;
                pipeReader.AdvanceTo(resumePos, buffer.End);
                continue;
            }

            if (readResult.IsCompleted)
            {
                if (incomplete)
                    throw new JsonException("Incomplete or truncated JSON: item spans past end of stream.");
                break;
            }
        }

        return count;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Increments the top-of-stack array index. Does nothing for object frames (null index).
    /// </summary>
    private static void IncrementIndex(Stack<int?> indices)
    {
        if (indices.TryPeek(out int? top) && top.HasValue)
        {
            indices.Pop();
            indices.Push(top.Value + 1);
        }
    }

    /// <summary>
    /// Restores a stack from a snapshot produced by <see cref="Stack{T}.ToArray()"/>,
    /// which returns elements top-first. Pushes in reverse so the original top ends up on top.
    /// </summary>
    private static void RestoreStack<T>(Stack<T> stack, T[] snapshot)
    {
        stack.Clear();
        for (int i = snapshot.Length - 1; i >= 0; i--)
            stack.Push(snapshot[i]);
    }
}
