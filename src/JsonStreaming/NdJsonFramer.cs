using System.Buffers;
using System.IO.Pipelines;

namespace JsonStreaming;

public struct NdJsonFramer : IItemFramer
{
    public void BeginDocument(PipeWriter pipeWriter)
    {
    }

    public void FinishItem(PipeWriter pipeWriter) => pipeWriter.Write("\n"u8);

    public void EndDocument(PipeWriter pipeWriter)
    {
    }
}