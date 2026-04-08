namespace JsonStreaming;

public struct JsonArrayFramer : IItemFramer
{
    private bool _needsComma;

    public void BeginDocument(Writers output) => output.Write("["u8);

    public void FinishItem(Writers output)
    {
        if (_needsComma)
            output.Write(","u8);
        _needsComma = true;
    }

    public void EndDocument(Writers output) => output.Write("]"u8);
}
