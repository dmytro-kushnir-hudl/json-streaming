using System.Buffers;
using System.Text.Json;

namespace JsonStreaming;

/// <summary>
/// Dual-channel output handle passed to <see cref="Transformer"/> callbacks.
/// Use <see cref="Json"/> for structured JSON writing via <see cref="Utf8JsonWriter"/>,
/// or <see cref="Bytes"/> / <see cref="Write(ReadOnlySpan{byte})"/> for raw byte copies.
/// </summary>
public readonly struct Writers : IBufferWriter<byte>
{
    private readonly IBufferWriter<byte> _bufferWriterImplementation;
    private readonly Utf8JsonWriter _utf8JsonWriter;

    internal Writers(IBufferWriter<byte> bufferWriterImplementation, Utf8JsonWriter utf8JsonWriter)
    {
        _bufferWriterImplementation = bufferWriterImplementation;
        _utf8JsonWriter = utf8JsonWriter;
    }

    /// <summary>Write raw bytes directly to the output pipe.</summary>
    public void Write(ReadOnlySpan<byte> value)
    {
        _bufferWriterImplementation.Write(value);
    }

    /// <summary>Write a multi-segment byte sequence directly to the output pipe.</summary>
    public void Write(ReadOnlySequence<byte> value)
    {
        var length = (int)value.Length;
        var span = _bufferWriterImplementation.GetSpan(length);
        value.CopyTo(span);
        _bufferWriterImplementation.Advance(length);
    }

    /// <summary>Structured JSON writer backed by the output pipe.</summary>
    public Utf8JsonWriter Json => _utf8JsonWriter;
    /// <summary>Raw byte writer backed by the output pipe.</summary>
    public IBufferWriter<byte> Bytes => _bufferWriterImplementation;

    void IBufferWriter<byte>.Advance(int count) => _bufferWriterImplementation.Advance(count);
    Memory<byte> IBufferWriter<byte>.GetMemory(int sizeHint) => _bufferWriterImplementation.GetMemory(sizeHint);
    Span<byte> IBufferWriter<byte>.GetSpan(int sizeHint) => _bufferWriterImplementation.GetSpan(sizeHint);
}