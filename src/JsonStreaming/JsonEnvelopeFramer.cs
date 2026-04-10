using System.Buffers.Text;

namespace JsonStreaming;

/// <summary>Frames items as <c>{"results":[item,item,…],"count":N}</c>.</summary>
public struct JsonEnvelopeFramer : IItemFramer
{
    private bool _needsComma;
    private int _count;

    /// <inheritdoc />
    public void BeginDocument(Writers output)
    {
        output.Write("{\"results\":["u8);
    }

    /// <inheritdoc />
    public void FinishItem(Writers output)
    {
        if (_needsComma)
            output.Write(","u8);
        _needsComma = true;
        _count++;
    }

    /// <inheritdoc />
    public void EndDocument(Writers output)
    {
        output.Write("],\"count\":"u8);
        Span<byte> buf = stackalloc byte[20];
        if (Utf8Formatter.TryFormat(_count, buf, out var written))
            output.Write(buf[..written]);
        output.Write("}"u8);
    }
}