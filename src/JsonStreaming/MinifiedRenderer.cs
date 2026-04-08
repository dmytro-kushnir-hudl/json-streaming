using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;

namespace JsonStreaming;

public struct MinifiedRenderer : ITokenRenderer
{
    private Utf8JsonWriter _jwriter;

    public MinifiedRenderer(Utf8JsonWriter jwriter) => _jwriter = jwriter;

    public void WriteToken(ref Utf8JsonReader reader, PipeWriter pipeWriter, ReadResult readResult)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.StartObject: _jwriter.WriteStartObject(); break;
            case JsonTokenType.EndObject: _jwriter.WriteEndObject(); break;
            case JsonTokenType.StartArray: _jwriter.WriteStartArray(); break;
            case JsonTokenType.EndArray: _jwriter.WriteEndArray(); break;
            case JsonTokenType.True: _jwriter.WriteBooleanValue(true); break;
            case JsonTokenType.False: _jwriter.WriteBooleanValue(false); break;
            case JsonTokenType.Null: _jwriter.WriteNullValue(); break;

            case JsonTokenType.PropertyName:
            case JsonTokenType.String:
            case JsonTokenType.Number:
                WriteValueToken(ref reader);
                break;

            case JsonTokenType.Comment:
            case JsonTokenType.None:
                break;
        }
    }

    public void Reset()
    {
        _jwriter.Flush();
        _jwriter.Reset();
    }

    private void WriteValueToken(ref Utf8JsonReader reader)
    {
        if (!reader.HasValueSequence)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.PropertyName: _jwriter.WritePropertyName(reader.ValueSpan); break;
                case JsonTokenType.String: _jwriter.WriteStringValue(reader.ValueSpan); break;
                case JsonTokenType.Number: _jwriter.WriteRawValue(reader.ValueSpan, skipInputValidation: true); break;
            }
            return;
        }
        
        int len = (int)reader.ValueSequence.Length;
        byte[] rented = ArrayPool<byte>.Shared.Rent(len);
        try
        {
            reader.ValueSequence.CopyTo(rented);
            ReadOnlySpan<byte> span = rented.AsSpan(0, len);
            switch (reader.TokenType)
            {
                case JsonTokenType.PropertyName: _jwriter.WritePropertyName(span); break;
                case JsonTokenType.String: _jwriter.WriteStringValue(span); break;
                case JsonTokenType.Number: _jwriter.WriteRawValue(span, skipInputValidation: true); break;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }
}