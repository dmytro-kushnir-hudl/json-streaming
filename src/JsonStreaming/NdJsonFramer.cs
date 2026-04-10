namespace JsonStreaming;

/// <summary>Frames items as newline-delimited JSON (NDJSON): one item per line.</summary>
public struct NdJsonFramer : IItemFramer
{
    /// <inheritdoc/>
    public void FinishItem(Writers output) => output.Write("\n"u8);
}
