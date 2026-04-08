using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;

namespace JsonStreaming;

public struct VerbatimRenderer : ITokenRenderer
{
    private bool _needsComma;

    public void WriteToken(ref Utf8JsonReader reader, PipeWriter pipeWriter, ReadResult readResult)
    {
        if (_needsComma && reader.TokenType is not JsonTokenType.EndObject and not JsonTokenType.EndArray)
            pipeWriter.Write(","u8);

        _needsComma = reader.TokenType switch
        {
            JsonTokenType.StartObject or JsonTokenType.StartArray or JsonTokenType.PropertyName => false,
            _ => true,
        };

        pipeWriter.CopyToken(reader, readResult);
    }

    public void Reset() => _needsComma = false;
    
}