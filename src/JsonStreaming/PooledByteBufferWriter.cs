using System.Buffers;
using System.Text.Json;

namespace JsonStreaming;

public readonly struct Writers : IBufferWriter<byte>
{
    private readonly IBufferWriter<byte> _bufferWriterImplementation;
    private readonly Utf8JsonWriter _utf8JsonWriter;
    
    internal Writers(IBufferWriter<byte> bufferWriterImplementation, Utf8JsonWriter utf8JsonWriter)
    {
        _bufferWriterImplementation = bufferWriterImplementation;
        _utf8JsonWriter = utf8JsonWriter;
    }

    public void Write(ReadOnlySpan<byte> value)
    {
        _bufferWriterImplementation.Write(value);
    }

    public void Write(ReadOnlySequence<byte> value)
    {
        var length = (int)value.Length;
        var span = _bufferWriterImplementation.GetSpan(length);
        value.CopyTo(span);
        _bufferWriterImplementation.Advance(length);
    }
    
    public Utf8JsonWriter Json => _utf8JsonWriter;
    public IBufferWriter<byte> Bytes => _bufferWriterImplementation;

    void IBufferWriter<byte>.Advance(int count) => _bufferWriterImplementation.Advance(count);
    Memory<byte> IBufferWriter<byte>.GetMemory(int sizeHint = 0) => _bufferWriterImplementation.GetMemory(sizeHint);
    Span<byte> IBufferWriter<byte>.GetSpan(int sizeHint = 0) => _bufferWriterImplementation.GetSpan(sizeHint);
}