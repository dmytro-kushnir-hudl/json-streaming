using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;

namespace JsonStreaming;

internal static class JsonTranscoderExtensions
{
    public static bool SequenceEqual(this ReadOnlySequence<byte> a, ReadOnlySpan<byte> b)
    {
        // 1. Fast length check
        if (a.Length != b.Length)
            return false;

        // 2. Fast path: contiguous memory
        if (a.IsSingleSegment)
            return a.FirstSpan.SequenceEqual(b);

        // 3. Slow path: multi-segment sequence
        var offset = 0;
        foreach (var segment in a)
        {
            var segmentSpan = segment.Span;

            // Slice the target span to match the current segment's length and compare
            if (!segmentSpan.SequenceEqual(b.Slice(offset, segmentSpan.Length)))
                return false;

            offset += segmentSpan.Length;
        }

        return true;
    }

    public static void CopyToken(
        this PipeWriter pipeWriter,
        Utf8JsonReader reader,
        ReadResult readResult
    )
    {
        var start = (int)reader.TokenStartIndex;
        var length = (int)(reader.BytesConsumed - reader.TokenStartIndex);

        if (readResult.Buffer.IsSingleSegment)
        {
            pipeWriter.Write(readResult.Buffer.FirstSpan.Slice(start, length));
        }
        else
        {
            var slice = readResult.Buffer.Slice(start, length);
            foreach (var seg in slice)
                pipeWriter.Write(seg.Span);
        }
    }
}