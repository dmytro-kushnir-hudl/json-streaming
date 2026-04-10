namespace JsonStreaming;

/// <summary>Frames items as a JSON array: <c>[item,item,…]</c>.</summary>
public struct JsonArrayFramer : IItemFramer
{
    private bool _needsComma;

    /// <inheritdoc />
    public void BeginDocument(Writers output)
    {
        output.Write("["u8);
    }

    /// <inheritdoc />
    public void FinishItem(Writers output)
    {
        if (_needsComma)
            output.Write(","u8);
        _needsComma = true;
    }

    /// <inheritdoc />
    public void EndDocument(Writers output)
    {
        output.Write("]"u8);
    }
}