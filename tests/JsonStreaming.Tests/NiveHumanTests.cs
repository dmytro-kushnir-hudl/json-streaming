using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace JsonStreaming.Tests;

public class NiveHumanTests
{
    private static readonly HttpClient Client = new();

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
        var rawBytes = await Client.GetByteArrayAsync(uri, ct);

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

        await Assert.ThrowsAnyAsync<JsonException>(() => inputPipe.ProxyFormattedJsonAsync(outputPipe, default, ct));
    }

    // ── ProjectNdJsonAsync ────────────────────────────────────────────────────

    // language=JSON
    const string OrderJson = """
        { "name"   : "Alice Brown",
          "sku"    : "54321",
          "price"  : 199.95,
          "shipTo" : { "name" : "Bob Brown", "city" : "Pretendville", "zip" : "98999" },
          "billTo" : { "name" : "Alice Brown", "city" : "Pretendville", "zip" : "98999" }
        }
        """;

    // language=JSON
    const string PeopleJson = """
        [
          { "name": "Adeel Solangi",  "language": "Sindhi", "version": 6.1  },
          { "name": "Afzal Ghaffar",  "language": "Sindhi", "version": 1.88 },
          { "name": "Aamir Solangi",  "language": "Sindhi", "version": 7.27 }
        ]
        """;

    private static string[] Project(string json, NdJsonPath path)
    {
        var bytes = Encoding.UTF8.GetBytes(json);
        var pipe = PipeReader.Create(new MemoryStream(bytes));
        var output = new MemoryStream();
        var writer = PipeWriter.Create(output);
        pipe.ProjectNdJsonAsync(path, writer).GetAwaiter().GetResult();
        writer.CompleteAsync().GetAwaiter().GetResult();
        return Encoding.UTF8.GetString(output.ToArray())
            .Split('\n', StringSplitOptions.RemoveEmptyEntries);
    }

    [Fact]
    public void Project_TopLevelPrimitive()
    {
        var lines = Project(OrderJson, NdJsonPath.At("price"));
        Assert.Equal(["199.95"], lines);
    }

    [Fact]
    public void Project_TopLevelObject()
    {
        var lines = Project(OrderJson, NdJsonPath.At("shipTo"));
        Assert.Single(lines);
        var obj = JsonDocument.Parse(lines[0]).RootElement;
        Assert.Equal("Bob Brown", obj.GetProperty("name").GetString());
        Assert.Equal("Pretendville", obj.GetProperty("city").GetString());
    }

    [Fact]
    public void Project_NestedPrimitive()
    {
        var lines = Project(OrderJson, NdJsonPath.At("shipTo").Key("city"));
        Assert.Equal(["\"Pretendville\""], lines);
    }

    [Fact]
    public void Project_ArrayElements()
    {
        var lines = Project(PeopleJson, NdJsonPath.Each());
        Assert.Equal(3, lines.Length);
        Assert.All(lines, line => JsonDocument.Parse(line)); // each line is valid JSON
        Assert.Equal("Adeel Solangi", JsonDocument.Parse(lines[0]).RootElement.GetProperty("name").GetString());
        Assert.Equal("Aamir Solangi", JsonDocument.Parse(lines[2]).RootElement.GetProperty("name").GetString());
    }

    [Fact]
    public void Project_PropertyOfEachArrayElement()
    {
        var lines = Project(PeopleJson, NdJsonPath.Each().Key("name"));
        Assert.Equal(["\"Adeel Solangi\"", "\"Afzal Ghaffar\"", "\"Aamir Solangi\""], lines);
    }

    [Fact]
    public void Project_NumberOfEachArrayElement()
    {
        var lines = Project(PeopleJson, NdJsonPath.Each().Key("version"));
        Assert.Equal(["6.1", "1.88", "7.27"], lines);
    }

    [Fact]
    public void Project_NoMatch_ReturnsEmpty()
    {
        var lines = Project(OrderJson, NdJsonPath.At("nonexistent"));
        Assert.Empty(lines);
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

    // ── NDJSON projection ─────────────────────────────────────────────────────

    public static async Task ProjectNdJsonAsync(
        this PipeReader reader,
        NdJsonPath path,
        PipeWriter writer,
        JsonReaderOptions options = default,
        CancellationToken ct = default)
    {
        var jwriter = new Utf8JsonWriter(writer, new JsonWriterOptions { SkipValidation = true });
        var state = new ProjectionState { ReaderState = new JsonReaderState(options) };
        ct.ThrowIfCancellationRequested();

        while (true)
        {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;
            var consumed = buffer.Start;

            try
            {
                if (result.IsCanceled) throw new OperationCanceledException(ct);

                var bytesConsumed = WriteProjection(state, result, jwriter, writer, path.Segments);
                consumed = buffer.GetPosition(bytesConsumed);

                if (result.IsCompleted || writer.UnflushedBytes >= 16 * 1024)
                {
                    await jwriter.FlushAsync(ct);
                    await writer.FlushAsync(ct);
                }

                if (result.IsCompleted) break;
            }
            finally
            {
                reader.AdvanceTo(consumed, buffer.End);
            }
        }

        ReturnPendingKey(state);
    }

    private static long WriteProjection(
        ProjectionState state,
        ReadResult readResult,
        Utf8JsonWriter jwriter,
        PipeWriter pipeWriter,
        byte[][] pattern)
    {
        var reader = new Utf8JsonReader(readResult.Buffer, readResult.IsCompleted, state.ReaderState);

        while (reader.Read())
        {
            // ── inside a captured value: re-emit tokens via Utf8JsonWriter (minified) ─
            if (state.CaptureDepth > 0)
            {
                if (reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray)
                    state.CaptureDepth++;
                else if (reader.TokenType is JsonTokenType.EndObject or JsonTokenType.EndArray)
                    state.CaptureDepth--;

                WriteToken(reader, jwriter);

                if (state.CaptureDepth == 0)
                {
                    jwriter.Flush();
                    pipeWriter.Write("\n"u8);
                    jwriter.Reset();
                }
                continue;
            }

            // ── path tracking + pattern matching ─────────────────────────
            switch (reader.TokenType)
            {
                case JsonTokenType.PropertyName:
                    ReturnPendingKey(state);
                    state.PendingKeyLength = reader.ValueSpan.Length;
                    state.PendingKey = ArrayPool<byte>.Shared.Rent(state.PendingKeyLength);
                    reader.ValueSpan.CopyTo(state.PendingKey);
                    break;

                case JsonTokenType.StartObject:
                case JsonTokenType.StartArray:
                {
                    bool isArray = reader.TokenType == JsonTokenType.StartArray;
                    bool parentIsArray = state.Depth >= 0 && state.IsArray[state.Depth];
                    if (parentIsArray) state.ArrayIndex[state.Depth]++;

                    bool seg = state.MatchedDepth == state.Depth
                               && MatchesSegment(state.MatchedDepth, pattern, parentIsArray, state.PendingKey, state.PendingKeyLength);
                    ReturnPendingKey(state);

                    state.Depth++;
                    state.IsArray[state.Depth] = isArray;
                    state.ArrayIndex[state.Depth] = 0;
                    state.MatchedDepthStack[state.Depth] = state.MatchedDepth;

                    if (seg && state.MatchedDepth + 1 == pattern.Length)
                    {
                        if (isArray) jwriter.WriteStartArray(); else jwriter.WriteStartObject();
                        state.CaptureDepth = 1;
                        state.Depth--;
                        state.MatchedDepth = state.MatchedDepthStack[state.Depth + 1];
                    }
                    else if (seg)
                    {
                        state.MatchedDepth++;
                    }
                    break;
                }

                case JsonTokenType.EndObject:
                case JsonTokenType.EndArray:
                    if (state.Depth >= 0)
                    {
                        state.MatchedDepth = state.MatchedDepthStack[state.Depth];
                        state.Depth--;
                    }
                    break;

                default:
                {
                    bool parentIsArray = state.Depth >= 0 && state.IsArray[state.Depth];
                    if (parentIsArray) state.ArrayIndex[state.Depth]++;

                    bool seg = state.MatchedDepth == state.Depth
                               && MatchesSegment(state.MatchedDepth, pattern, parentIsArray, state.PendingKey, state.PendingKeyLength);
                    ReturnPendingKey(state);

                    if (seg && state.MatchedDepth + 1 == pattern.Length)
                    {
                        WriteToken(reader, jwriter);
                        jwriter.Flush();
                        pipeWriter.Write("\n"u8);
                        jwriter.Reset();
                    }
                    break;
                }
            }
        }

        state.ReaderState = reader.CurrentState;
        return reader.BytesConsumed;

        static bool MatchesSegment(int matchedDepth, byte[][] pattern, bool parentIsArray, byte[]? pendingKey, int pendingKeyLength)
        {
            if (matchedDepth >= pattern.Length) return false;
            var seg = pattern[matchedDepth];
            if (seg.Length == 0) return parentIsArray;
            return !parentIsArray && pendingKey is not null
                   && pendingKey.AsSpan(0, pendingKeyLength).SequenceEqual(seg);
        }

        // Rents a buffer, copies the multi-segment value sequence into it,
        // then dispatches to the correct writer method based on TokenType.
        // No closure — writer is passed as explicit state.
        static void WriteTokenSequence(Utf8JsonReader r, Utf8JsonWriter w)
        {
            int len = (int)r.ValueSequence.Length;
            byte[] rented = ArrayPool<byte>.Shared.Rent(len);
            try
            {
                r.ValueSequence.CopyTo(rented);
                ReadOnlySpan<byte> span = rented.AsSpan(0, len);
                switch (r.TokenType)
                {
                    case JsonTokenType.PropertyName: w.WritePropertyName(span); break;
                    case JsonTokenType.String:       w.WriteStringValue(span); break;
                    case JsonTokenType.Number:       w.WriteRawValue(span, skipInputValidation: true); break;
                }
            }
            finally { ArrayPool<byte>.Shared.Return(rented); }
        }

        static void WriteToken(Utf8JsonReader r, Utf8JsonWriter w)
        {
            switch (r.TokenType)
            {
                case JsonTokenType.StartObject:  w.WriteStartObject(); break;
                case JsonTokenType.EndObject:    w.WriteEndObject(); break;
                case JsonTokenType.StartArray:   w.WriteStartArray(); break;
                case JsonTokenType.EndArray:     w.WriteEndArray(); break;
                case JsonTokenType.PropertyName:
                    if (r.HasValueSequence) WriteTokenSequence(r, w);
                    else w.WritePropertyName(r.ValueSpan);
                    break;
                case JsonTokenType.String:
                    if (r.HasValueSequence) WriteTokenSequence(r, w);
                    else w.WriteStringValue(r.ValueSpan);
                    break;
                case JsonTokenType.Number:
                    if (r.HasValueSequence) WriteTokenSequence(r, w);
                    else w.WriteRawValue(r.ValueSpan, skipInputValidation: true);
                    break;
                case JsonTokenType.True:         w.WriteBooleanValue(true); break;
                case JsonTokenType.False:        w.WriteBooleanValue(false); break;
                case JsonTokenType.Null:         w.WriteNullValue(); break;
            }
        }
    }

    private class ProjectionState
    {
        public int Depth = -1;
        public int MatchedDepth;
        public readonly bool[] IsArray = new bool[64];
        public readonly int[] ArrayIndex = new int[64];
        public readonly int[] MatchedDepthStack = new int[64];
        public byte[]? PendingKey;
        public int PendingKeyLength;
        public int CaptureDepth;
        public JsonReaderState ReaderState;
    }

    private static void ReturnPendingKey(ProjectionState state)
    {
        if (state.PendingKey is not null)
        {
            ArrayPool<byte>.Shared.Return(state.PendingKey);
            state.PendingKey = null;
            state.PendingKeyLength = 0;
        }
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

// ── JSON path DSL ─────────────────────────────────────────────────────────────

/// <summary>
/// Root-anchored, compile-time encoded JSON path pattern.
/// Segments: non-null = UTF-8 property name, null = array wildcard (any index).
/// Max depth 64 matches Utf8JsonReader's default limit.
/// </summary>
public sealed class NdJsonPath
{
    /// <summary>Start path from root with a named property key.</summary>
    public static Builder At(string key) => new Builder().Key(key);

    /// <summary>Start path from root with an array wildcard (root is an array).</summary>
    public static Builder Each() => new Builder().Each();

    public readonly byte[][] Segments;
    private NdJsonPath(byte[][] segments) => Segments = segments;

    // Empty array is the wildcard sentinel (any array index).
    public static readonly byte[] Wildcard = [];

    public sealed class Builder
    {
        private readonly List<byte[]> _segments = [];

        /// <summary>Descend into a named object property.</summary>
        public Builder Key(string name)
        {
            _segments.Add(Encoding.UTF8.GetBytes(name));
            return this;
        }

        /// <summary>Descend into every element of an array (wildcard index).</summary>
        public Builder Each()
        {
            _segments.Add(Wildcard);
            return this;
        }

        public NdJsonPath Build() => new([.. _segments]);
        public static implicit operator NdJsonPath(Builder b) => b.Build();
    }
}

// ── Usage sketches ────────────────────────────────────────────────────────────
//
//   NdJsonPath.At("users").Each()
//   NdJsonPath.At("users").Each().Key("address").Key("city")
//   NdJsonPath.At("responses").Each().Key("items").Each()
//
// In a test:
//
//   NdJsonPath path = NdJsonPath.At("users").Each();
//   var out = new MemoryStream();
//   await pipeReader.ProjectNdJsonAsync(path, PipeWriter.Create(out), ct);
//   var lines = Encoding.UTF8.GetString(out.ToArray())
//       .Split('\n', StringSplitOptions.RemoveEmptyEntries);
//   Assert.Equal(expectedCount, lines.Length);
//   Assert.All(lines, line => JsonDocument.Parse(line));  // each line is valid JSON