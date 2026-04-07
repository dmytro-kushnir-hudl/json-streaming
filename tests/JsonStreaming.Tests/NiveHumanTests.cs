using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using System.Text.Encodings.Web;
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

    [Theory]
    [InlineData("64KB-min.json")]
    [InlineData("128KB-min.json")]
    [InlineData("256KB-min.json")]
    [InlineData("512KB-min.json")]
    [InlineData("1MB-min.json")]
    [InlineData("5MB-min.json")]
    [InlineData("64KB.json")]
    [InlineData("128KB.json")]
    [InlineData("256KB.json")]
    [InlineData("512KB.json")]
    [InlineData("1MB.json")]
    [InlineData("5MB.json")]
    public async Task PipeIt_Handrolled(string fileName)
    {
        var ct = TestContext.Current.CancellationToken;
        var uri = $"https://microsoftedge.github.io/Demos/json-dummy-data/{fileName}";
        var rawBytes = await Client.GetByteArrayAsync(
            uri,
            ct
        );

        // Expected: round-trip through JsonDocument + JsonSerializer with indentation
        var expected = JsonSerializer.Serialize(
            JsonDocument.Parse(rawBytes).RootElement,
            new JsonSerializerOptions { WriteIndented = true, Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping }
        );

        // Actual: stream through ProxyFormattedJsonAsync
        var inputPipe = PipeReader.Create(await Client.GetStreamAsync(uri, ct));
        var outputStream = new MemoryStream();
        var outputPipe = PipeWriter.Create(outputStream);
        await inputPipe.ProxyFormattedJsonAsync(outputPipe, default, ct);
        await outputPipe.CompleteAsync();
        var actual = Encoding.UTF8.GetString(outputStream.ToArray());

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData("64KB-min.json")]
    [InlineData("128KB-min.json")]
    [InlineData("256KB-min.json")]
    [InlineData("512KB-min.json")]
    [InlineData("1MB-min.json")]
    [InlineData("5MB-min.json")]
    [InlineData("64KB.json")]
    [InlineData("128KB.json")]
    [InlineData("256KB.json")]
    [InlineData("512KB.json")]
    [InlineData("1MB.json")]
    [InlineData("5MB.json")]
    public async Task PipeIt_Handrolled_Min(string fileName)
    {
        var ct = TestContext.Current.CancellationToken;
        var uri = $"https://microsoftedge.github.io/Demos/json-dummy-data/{fileName}";
        var rawBytes = await Client.GetByteArrayAsync(
            uri,
            ct
        );

        // Expected: round-trip through JsonDocument + JsonSerializer with indentation
        var expected = JsonSerializer.Serialize(
            JsonDocument.Parse(rawBytes).RootElement,
            new JsonSerializerOptions { WriteIndented = false, Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping }
        );

        // Actual: stream through ProxyFormattedJsonAsync
        var inputPipe = PipeReader.Create(await Client.GetStreamAsync(uri, ct));
        var outputStream = new MemoryStream();
        var outputPipe = PipeWriter.Create(outputStream);
        await inputPipe.ProxyMinifiedJsonAsync(outputPipe, default, ct);
        await outputPipe.CompleteAsync();
        var actual = Encoding.UTF8.GetString(outputStream.ToArray());

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData("missing-colon.json")]
    [InlineData("unterminated.json")]
    [InlineData("binary-data.json")]
    public async Task PipeIt_Handrolled_Malformed(string fileName)
    {
        var ct = TestContext.Current.CancellationToken;

        var rawBytes = await Client.GetByteArrayAsync(
            $"https://microsoftedge.github.io/Demos/json-dummy-data/{fileName}",
            ct
        );

        var inputPipe = PipeReader.Create(new MemoryStream(rawBytes));
        var outputPipe = PipeWriter.Create(new MemoryStream());

        await Assert.ThrowsAnyAsync<JsonException>(() => inputPipe.ProxyFormattedJsonAsync(outputPipe, default, ct)
        );
    }
}

public static class Logic
{
    public static async Task ProxyMinifiedJsonAsync(
        this PipeReader reader,
        PipeWriter writer,
        JsonReaderOptions options = default,
        CancellationToken ct = default)
    {
        var readerState = new JsonReaderState(options);
        var customState = new MinifiedState
        {
            State = readerState,
        };

        ct.ThrowIfCancellationRequested();

        while (true)
        {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;
            var consumed = buffer.Start;

            try
            {
                ct.ThrowIfCancellationRequested();

                var bytesConsumed = WriteMinified(customState, result, writer);
                consumed = buffer.GetPosition(bytesConsumed);
                if (result.IsCompleted || writer.UnflushedBytes >= 16 * 1024)
                    await writer.FlushAsync(ct);

                if (result.IsCompleted)
                    break;
            }
            finally
            {
                reader.AdvanceTo(consumed, buffer.End);
            }
        }
    }

    public static async Task ProxyFormattedJsonAsync(
        this PipeReader reader,
        PipeWriter writer,
        JsonReaderOptions options = default,
        CancellationToken ct = default)
    {
        var readerState = new JsonReaderState(options);
        var customState = new FormattedState
        {
            Depth = 0,
            NeedsComma = false,
            AfterColon = false,
            State = readerState,
        };

        ct.ThrowIfCancellationRequested();

        while (true)
        {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;
            var consumed = buffer.Start;

            try
            {
                ct.ThrowIfCancellationRequested();

                var bytesConsumed = WriteFormatted(customState, result, writer);
                consumed = buffer.GetPosition(bytesConsumed);
                if (result.IsCompleted || writer.UnflushedBytes >= 16 * 1024)
                    await writer.FlushAsync(ct);

                if (result.IsCompleted)
                    break;
            }
            finally
            {
                reader.AdvanceTo(consumed, buffer.End);
            }
        }
    }

    private static long WriteFormatted(FormattedState state, ReadResult readResult, PipeWriter pipeWriter)
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
                    CopyToken(reader, pipeWriter, readResult);
                    state.AfterColon = true;
                    state.NeedsComma = false;
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

        static void WriteIndent(PipeWriter pipeWriter, int indent)
        {
            for (int i = 0; i < indent; i++)
                pipeWriter.Write("  "u8);
        }
    }

    private static long WriteMinified(MinifiedState state, ReadResult readResult, PipeWriter pipeWriter)
    {
        var reader = new Utf8JsonReader(readResult.Buffer, readResult.IsCompleted, state.State);

        while (reader.Read())
        {
            if (state.NeedsComma && reader.TokenType is not JsonTokenType.EndObject and not JsonTokenType.EndArray)
                pipeWriter.Write(","u8);

            state.NeedsComma = reader.TokenType switch
            {
                JsonTokenType.StartObject or JsonTokenType.StartArray or JsonTokenType.PropertyName => false,
                _ => true
            };

            CopyToken(reader, pipeWriter, readResult);
        }

        state.State = reader.CurrentState;
        return reader.BytesConsumed;
    }

    private static void CopyToken(Utf8JsonReader reader, PipeWriter pipeWriter, ReadResult readResult)
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

    private class FormattedState
    {
        public int Depth;
        public bool NeedsComma;
        public bool AfterColon;
        public JsonReaderState State;
    }

    private class MinifiedState
    {
        public bool NeedsComma;
        public JsonReaderState State;
    }
}