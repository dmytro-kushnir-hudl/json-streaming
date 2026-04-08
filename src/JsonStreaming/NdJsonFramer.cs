namespace JsonStreaming;

public struct NdJsonFramer : IItemFramer
{
    public void FinishItem(Writers output) => output.Write("\n"u8);
}
