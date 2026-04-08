using System.Buffers;

namespace JsonStreaming;

public readonly struct PooledByteBufferWriter : IBufferWriter<byte>
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

    void IBufferWriter<byte>.Advance(int count) => _bufferWriterImplementation.Advance(count);
    Memory<byte> IBufferWriter<byte>.GetMemory(int sizeHint = 0) => _bufferWriterImplementation.GetMemory(sizeHint);
    Span<byte> IBufferWriter<byte>.GetSpan(int sizeHint = 0) => _bufferWriterImplementation.GetSpan(sizeHint);
}