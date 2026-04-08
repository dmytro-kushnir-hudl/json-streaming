using System.IO.Pipelines;

namespace JsonStreaming;

public interface IItemFramer
{
    void BeginDocument(PipeWriter pipeWriter);
    void FinishItem(PipeWriter pipeWriter);
    void EndDocument(PipeWriter pipeWriter);
}