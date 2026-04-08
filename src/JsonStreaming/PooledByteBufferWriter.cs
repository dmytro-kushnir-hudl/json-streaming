using System.Buffers;
using System.IO.Pipelines;

namespace JsonStreaming;

public readonly struct PooledByteBufferWriter 
{
    private readonly IBufferWriter<byte> _bufferWriterImplementation;
    internal PooledByteBufferWriter(IBufferWriter<byte> bufferWriterImplementation)
    {
        _bufferWriterImplementation = bufferWriterImplementation;
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
}