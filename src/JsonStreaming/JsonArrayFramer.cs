using System.Buffers;
using System.IO.Pipelines;

namespace JsonStreaming;

public struct JsonArrayFramer : IItemFramer
{
    private bool _needsComma;

    public void BeginDocument(PipeWriter pipeWriter) => pipeWriter.Write("["u8);

    public void FinishItem(PipeWriter pipeWriter)
    {
        if (_needsComma)
            pipeWriter.Write(","u8);
        _needsComma = true;
    }

    public void EndDocument(PipeWriter pipeWriter) => pipeWriter.Write("]"u8);
}