namespace JsonStreaming;

/// <summary>
/// Controls how matched items are delimited within the output stream.
/// Implement to produce custom framing (e.g. JSON array, NDJSON, custom envelope).
/// </summary>
public interface IItemFramer
{
    /// <summary>Called once before any items are written.</summary>
    void BeginDocument(Writers output) { }
    /// <summary>Called before each item is written. Use to emit separators or opening tokens.</summary>
    void FinishItem(Writers output) { }
    /// <summary>Called once after all items have been written.</summary>
    void EndDocument(Writers output) { }
}
