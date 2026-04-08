namespace JsonStreaming;

public interface IItemFramer
{
    void BeginDocument(Writers output) { }
    void FinishItem(Writers output) { }
    void EndDocument(Writers output) { }
}
