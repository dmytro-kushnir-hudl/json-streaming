using System.Buffers.Text;

namespace JsonStreaming;

public struct JsonEnvelopeFramer : IItemFramer
{
    private bool _needsComma;
    private int _count;

    public void BeginDocument(Writers output) => output.Write("{\"results\":["u8);

    public void FinishItem(Writers output)
    {
        if (_needsComma)
            output.Write(","u8);
        _needsComma = true;
        _count++;
    }

    public void EndDocument(Writers output)
    {
        output.Write("],\"count\":"u8);
        Span<byte> buf = stackalloc byte[20];
        if (Utf8Formatter.TryFormat(_count, buf, out int written))
            output.Write(buf[..written]);
        output.Write("}"u8);
    }
}
