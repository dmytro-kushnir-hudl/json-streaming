using System.IO.Pipelines;
using System.Text.Json;

namespace JsonStreaming;

public interface ITokenRenderer
{
    void WriteToken(ref Utf8JsonReader reader, PipeWriter pipeWriter, ReadResult readResult);
    void Reset();
}