using System.Buffers;
using System.Buffers.Text;
using System.IO.Pipelines;

namespace JsonStreaming;

public struct JsonEnvelopeFramer : IItemFramer
{
    private bool _needsComma;
    private int _count;

    public void BeginDocument(PipeWriter pipeWriter) => pipeWriter.Write("{\"results\":["u8);

    public void FinishItem(PipeWriter pipeWriter)
    {
        if (_needsComma)
            pipeWriter.Write(","u8);
        _needsComma = true;
        _count++;
    }

    public void EndDocument(PipeWriter pipeWriter)
    {
        pipeWriter.Write("],\"count\":"u8);
        Span<byte> buf = stackalloc byte[20];
        if (Utf8Formatter.TryFormat(_count, buf, out int written))
            pipeWriter.Write(buf[..written]);
        pipeWriter.Write("}"u8);
    }
}