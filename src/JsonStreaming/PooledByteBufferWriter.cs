using System.Buffers;
using System.Text.Json;

namespace JsonStreaming;

/// <summary>
///     Dual-channel output handle passed to <see cref="Transformer" /> callbacks.
///     Use <see cref="Json" /> for structured JSON writing via <see cref="Utf8JsonWriter" />,
///     or <see cref="Bytes" /> / <see cref="Write(ReadOnlySpan{byte})" /> for raw byte copies.
/// </summary>
public readonly struct Writers : IBufferWriter<byte>
{
    internal Writers(IBufferWriter<byte> bufferWriterImplementation, Utf8JsonWriter utf8JsonWriter)
    {
        Bytes = bufferWriterImplementation;
        Json = utf8JsonWriter;
    }

    /// <summary>Write raw bytes directly to the output pipe.</summary>
    public void Write(ReadOnlySpan<byte> value)
    {
        Bytes.Write(value);
    }

    /// <summary>Write a multi-segment byte sequence directly to the output pipe.</summary>
    public void Write(ReadOnlySequence<byte> value)
    {
        var length = (int)value.Length;
        var span = Bytes.GetSpan(length);
        value.CopyTo(span);
        Bytes.Advance(length);
    }

    /// <summary>Structured JSON writer backed by the output pipe.</summary>
    public Utf8JsonWriter Json { get; }

    /// <summary>Raw byte writer backed by the output pipe.</summary>
    public IBufferWriter<byte> Bytes { get; }

    void IBufferWriter<byte>.Advance(int count)
    {
        Bytes.Advance(count);
    }

    Memory<byte> IBufferWriter<byte>.GetMemory(int sizeHint)
    {
        return Bytes.GetMemory(sizeHint);
    }

    Span<byte> IBufferWriter<byte>.GetSpan(int sizeHint)
    {
        return Bytes.GetSpan(sizeHint);
    }
}