using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using System.Text.Json;

namespace JsonStreaming.Tests;

public class NiveHumanTests(ITestOutputHelper output)
{
    private static readonly HttpClient Client = new();

    // language=JSON
    const string JsonObject = """
        { "name"   : "Alice Brown",
          "sku"    : "54321",
          "price"  : 199.95,
          "shipTo" : { "name" : "Bob Brown",
                       "address" : "456 Oak Lane",
                       "city" : "Pretendville",
                       "state" : "HI",
                       "zip"   : "98999" },
          "billTo" : { "name" : "Alice Brown",
                       "address" : "456 Oak Lane",
                       "city" : "Pretendville",
                       "state" : "HI",
                       "zip"   : "98999" }
        }
        """;

    // language=JSON
    const string JsonArray = """
        [
            {
            "name": "Adeel Solangi",
            "language": "Sindhi",
            "id": "V59OF92YF627HFY0",
            "bio": "Donec lobortis eleifend condimentum. Cras dictum dolor lacinia lectus vehicula rutrum. Maecenas quis nisi nunc. Nam tristique feugiat est vitae mollis. Maecenas quis nisi nunc.",
            "version": 6.1
            },
            {
            "name": "Afzal Ghaffar",
            "language": "Sindhi",
            "id": "ENTOCR13RSCLZ6KU",
            "bio": "Aliquam sollicitudin ante ligula, eget malesuada nibh efficitur et. Pellentesque massa sem, scelerisque sit amet odio id, cursus tempor urna. Etiam congue dignissim volutpat. Vestibulum pharetra libero et velit gravida euismod.",
            "version": 1.88
            },
            {
            "name": "Aamir Solangi",
            "language": "Sindhi",
            "id": "IAKPO3R4761JDRVG",
            "bio": "Vestibulum pharetra libero et velit gravida euismod. Quisque mauris ligula, efficitur porttitor sodales ac, lacinia non ex. Fusce eu ultrices elit, vel posuere neque.",
            "version": 7.27
            }
        ]
        """;

    [Theory]
    [InlineData(JsonObject)]
    [InlineData(JsonArray)]
    public void Go(string source)
    {
        var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(source));
        var sb = new StringBuilder();
        int depth = 0;
        bool needsComma = false;
        bool afterColon = false;

        while (reader.Read())
        {
            // ── prefix: newline/comma/indent before the token ──────────────
            if (reader.TokenType is JsonTokenType.EndObject or JsonTokenType.EndArray)
            {
                depth--;
                sb.Append('\n').Append(' ', depth);
            }
            else if (afterColon)
            {
                sb.Append(' '); // space between ':' and value
                afterColon = false;
            }
            else if (needsComma)
            {
                sb.Append(',').Append('\n').Append(' ', depth);
            }
            else
            {
                sb.Append(' ', depth); // first item in container
            }

            // ── token ───────────────────────────────────────────────────────
            switch (reader.TokenType)
            {
                case JsonTokenType.StartObject:
                    sb.Append('{').Append('\n');
                    depth++;
                    needsComma = false;
                    break;
                case JsonTokenType.EndObject:
                    sb.Append('}');
                    needsComma = true;
                    break;

                case JsonTokenType.StartArray:
                    sb.Append('[').Append('\n');
                    depth++;
                    needsComma = false;
                    break;
                case JsonTokenType.EndArray:
                    sb.Append(']');
                    needsComma = true;
                    break;

                case JsonTokenType.PropertyName:
                    sb.Append(JsonSerializer.Serialize(reader.GetString())).Append(':');
                    afterColon = true;
                    needsComma = false;
                    break;

                case JsonTokenType.String:
                case JsonTokenType.None:
                    sb.Append(JsonSerializer.Serialize(reader.GetString()));
                    needsComma = true;
                    break;

                case JsonTokenType.Number:
                case JsonTokenType.True:
                case JsonTokenType.False:
                case JsonTokenType.Null:
                default:
                    sb.Append(Encoding.UTF8.GetString(reader.ValueSpan));
                    needsComma = true;
                    break;
            }
        }

        output.WriteLine(sb.ToString());
    }

    [Fact]
    public async Task PipeIt()
    {
        var ct = TestContext.Current.CancellationToken;

        const string kbJson = "64KB.json";
        var request = new HttpRequestMessage(
            HttpMethod.Get,
            $"https://microsoftedge.github.io/Demos/json-dummy-data/{kbJson}"
        );
        var response = await Client.SendAsync(
            request,
            HttpCompletionOption.ResponseHeadersRead,
            ct
        );
        var stream = await response.Content.ReadAsStreamAsync(ct);
        var reader = PipeReader.Create(stream);
        if (File.Exists(kbJson))
            File.Delete(kbJson);
        var writer = PipeWriter.Create(File.OpenWrite(kbJson));

        while (true)
        {
            ReadResult result = await reader.ReadAsync(ct);
            ReadOnlySequence<byte> buffer = result.Buffer;

            if (!buffer.IsEmpty)
            {
                foreach (var segment in buffer)
                    writer.Write(segment.Span);

                var flush = await writer.FlushAsync(ct);
                if (flush.IsCompleted || flush.IsCanceled)
                    break;
            }

            // Mark everything as consumed
            reader.AdvanceTo(buffer.End);

            if (result.IsCompleted || result.IsCanceled)
                break;
        }

        await writer.CompleteAsync();

        Console.WriteLine(await File.ReadAllTextAsync(kbJson, ct));
    }

    [Fact]
    public async Task PipeIt_smart()
    {
        var ct = TestContext.Current.CancellationToken;

        const string kbJson = "64KB.json";
        var request = new HttpRequestMessage(
            HttpMethod.Get,
            $"https://microsoftedge.github.io/Demos/json-dummy-data/{kbJson}"
        );
        var response = await Client.SendAsync(
            request,
            HttpCompletionOption.ResponseHeadersRead,
            ct
        );
        var stream = await response.Content.ReadAsStreamAsync(ct);
        var reader = PipeReader.Create(stream);
        if (File.Exists(kbJson))
            File.Delete(kbJson);
        var writer = PipeWriter.Create(File.OpenWrite(kbJson));

        await reader.CopyToAsync(writer, ct);
        await writer.CompleteAsync();

        Console.WriteLine(await File.ReadAllTextAsync(kbJson, ct));
    }
    
    [Fact]
    public async Task PipeIt_Handrolled()
    {
        var ct = TestContext.Current.CancellationToken;

        const string kbJson = "64KB.json";
        var request = new HttpRequestMessage(
            HttpMethod.Get,
            $"https://microsoftedge.github.io/Demos/json-dummy-data/{kbJson}"
        );
        var response = await Client.SendAsync(
            request,
            HttpCompletionOption.ResponseHeadersRead,
            ct
        );
        var stream = await response.Content.ReadAsStreamAsync(ct);
        var reader = PipeReader.Create(stream);
        if (File.Exists(kbJson))
            File.Delete(kbJson);
        var writer = PipeWriter.Create(File.OpenWrite(kbJson));

        await Logic.ProxyFormattedJsonAsync(reader, writer, ct);
        await writer.CompleteAsync();

        Console.WriteLine(await File.ReadAllTextAsync(kbJson, ct));
    }
}

public class CustomState
{
    public int Depth;
    public bool NeedsComma;
    public bool AfterColon;
    public JsonReaderState State;
}

public static class Logic
{
    public static async Task ProxyFormattedJsonAsync(
        this PipeReader reader,
        PipeWriter writer,
        CancellationToken ct
    )
    {
        var options = new JsonReaderOptions();
        var readerState = new JsonReaderState(options);
        var customState = new CustomState
        {
            Depth = 0,
            NeedsComma = false,
            AfterColon = false,
            State = readerState,
        };

        if (ct.IsCancellationRequested)
            return await Task.FromCanceled(ct);

        while (true)
        {
            var result = await reader.ReadAsync(ct);
            var consumed = Write(customState, result, writer);
            
            reader.AdvanceTo(result.Buffer.End);
            await writer.FlushAsync(ct);
            
            if (result.IsCompleted)
                break;
        }
    }

    private static long Write(CustomState state, ReadResult readResult, PipeWriter pipeWriter)
    {
        var reader = new Utf8JsonReader(readResult.Buffer, readResult.IsCompleted, state.State);

        while (reader.Read())
        {
            // ── prefix: newline/comma/indent before the token ──────────────
            if (reader.TokenType is JsonTokenType.EndObject or JsonTokenType.EndArray)
            {
                state.Depth--;
                pipeWriter.Write("\n"u8);
                WriteIndent(pipeWriter, state.Depth);
            }
            else if (state.AfterColon)
            {
                pipeWriter.Write(" "u8); // space between ':' and value
                state.AfterColon = false;
            }
            else if (state.NeedsComma)
            {
                pipeWriter.Write(",\n"u8);
                WriteIndent(pipeWriter, state.Depth);
            }
            else
            {
                WriteIndent(pipeWriter, state.Depth);
            }

            // ── token ───────────────────────────────────────────────────────
            switch (reader.TokenType)
            {
                case JsonTokenType.StartObject:
                    pipeWriter.Write("{\n"u8);
                    state.Depth++;
                    state.NeedsComma = false;
                    break;

                case JsonTokenType.EndObject:
                    pipeWriter.Write("}"u8);
                    state.NeedsComma = true;
                    break;

                case JsonTokenType.StartArray:
                    pipeWriter.Write("[\n"u8);
                    state.Depth++;
                    state.NeedsComma = false;
                    break;

                case JsonTokenType.EndArray:
                    pipeWriter.Write("]"u8);
                    state.NeedsComma = true;
                    break;

                case JsonTokenType.PropertyName:
                    {
                        CopyToken(reader, pipeWriter, readResult);
                        pipeWriter.Write(":"u8);
                        state.AfterColon = true;
                        state.NeedsComma = false;
                    }
                    break;

                case JsonTokenType.Comment:
                case JsonTokenType.String:
                case JsonTokenType.None:
                case JsonTokenType.Number:
                case JsonTokenType.True:
                case JsonTokenType.False:
                case JsonTokenType.Null:
                default:
                    CopyToken(reader, pipeWriter, readResult);
                    state.NeedsComma = true;
                    break;
            }
        }

        state.State = reader.CurrentState;
        return reader.BytesConsumed;

        static void CopyToken(Utf8JsonReader reader, PipeWriter pipeWriter, ReadResult readResult)
        {
            var quotedString = readResult.Buffer.Slice(
                reader.TokenStartIndex,
                reader.BytesConsumed - reader.TokenStartIndex
            );
            if (quotedString.IsSingleSegment)
                pipeWriter.Write(quotedString.FirstSpan);
            else
                foreach (var seg in quotedString)
                    pipeWriter.Write(seg.Span);
        }

        static void WriteIndent(PipeWriter pipeWriter, int indent)
        {
            for (int i = 0; i < indent; i++)
                pipeWriter.Write("  "u8);
        }
    }

    public static Task CopyToAsync2(
        this PipeReader source,
        PipeWriter destination,
        CancellationToken ct = default
    )
    {
        if (destination is null)
        {
            ThrowHelper.ThrowArgumentNullException();
        }

        if (ct.IsCancellationRequested)
        {
            return Task.FromCanceled(ct);
        }

        return source.CopyToAsyncCore(destination, ct);
    }

    private static async Task CopyToAsyncCore(
        this PipeReader reader,
        PipeWriter destination,
        CancellationToken ct
    )
    {
        while (true)
        {
            ReadResult result = await reader.ReadAsync(ct).ConfigureAwait(false);
            ReadOnlySequence<byte> buffer = result.Buffer;
            SequencePosition position = buffer.Start;
            SequencePosition consumed = position;

            try
            {
                if (result.IsCanceled)
                {
                    ThrowHelper.ThrowOperationCanceledException_ReadCanceled();
                }

                while (buffer.TryGet(ref position, out ReadOnlyMemory<byte> memory))
                {
                    if (memory.IsEmpty)
                    {
                        // advance tracking only (to account for any boundary scenarios)
                        consumed = position;
                    }
                    else
                    {
                        // write and advance
                        FlushResult flushResult = await destination
                            .WriteAsync(memory, ct)
                            .ConfigureAwait(false);

                        if (flushResult.IsCanceled)
                            ThrowHelper.ThrowOperationCanceledException_FlushCanceled();

                        consumed = position;

                        if (flushResult.IsCompleted)
                            return;
                    }
                }

                // The while loop completed successfully, so we've consumed the entire buffer.
                consumed = buffer.End;

                if (result.IsCompleted)
                    break;
            }
            finally
            {
                // Advance even if WriteAsync throws so the PipeReader is not left in the
                // currently reading state
                reader.AdvanceTo(consumed);
            }
        }
    }
}

internal static class ThrowHelper
{
    public static void ThrowOperationCanceledException_FlushCanceled()
    {
        throw new NotImplementedException();
    }

    public static void ThrowOperationCanceledException_ReadCanceled()
    {
        throw new NotImplementedException();
    }

    public static void ThrowArgumentNullException()
    {
        throw new NotImplementedException();
    }
}
