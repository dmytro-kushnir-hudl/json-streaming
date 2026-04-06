using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using JsonStreaming;
using Microsoft.AspNetCore.Http.Features;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

// Simulates an upstream JSON API response (e.g. SumoLogic, Elasticsearch, etc.)
// In production this would be HttpClient.SendAsync with HttpCompletionOption.ResponseHeadersRead
PipeReader SimulateUpstream(int itemCount)
{
    var sb = new StringBuilder();
    sb.Append("""{"metadata":{"query":"test","took_ms":42},"results":[""");
    for (int i = 0; i < itemCount; i++)
    {
        if (i > 0)
            sb.Append(',');
        sb.Append($$"""{"id":{{i}},"message":"Log entry #{{i}}","level":"INFO","host":"prod-{{i % 4}}","timestamp":"2026-04-06T10:00:00Z"}""");
    }
    sb.Append("]}");
    var bytes = Encoding.UTF8.GetBytes(sb.ToString());
    return PipeReader.Create(new MemoryStream(bytes), new StreamPipeReaderOptions(bufferSize: 8192));
}

// ── GET /stream — verbatim passthrough ─────────────────────────────────
// Streams all items from upstream JSON array directly to the HTTP response.
app.MapGet(
    "/stream",
    async (HttpContext ctx, int count = 100) =>
    {
        ctx.Response.ContentType = "application/json";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        var upstream = SimulateUpstream(count);
        var pipeWriter = ctx.Response.BodyWriter;
        var writer = new Utf8JsonWriter(pipeWriter);
        var options = new WriteOptions
        {
            AsyncFlush = async ct => { await pipeWriter.FlushAsync(ct); },
        };

        writer.WriteStartObject();
        writer.WriteStartArray("results"u8);

        var itemCount = await JsonStreamReader.WriteArrayAsync(
            upstream,
            "results",
            writer,
            options
        );

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, itemCount);
        writer.WriteEndObject();
        writer.Flush();

        await pipeWriter.FlushAsync();
        await upstream.CompleteAsync();
    }
);

// ── GET /transform — selective field streaming ─────────────────────────
// Streams items but only keeps id, message, level (drops host, timestamp).
app.MapGet(
    "/transform",
    async (HttpContext ctx, int count = 100) =>
    {
        ctx.Response.ContentType = "application/json";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        var upstream = SimulateUpstream(count);
        var writer = new Utf8JsonWriter(ctx.Response.BodyWriter);

        writer.WriteStartObject();
        writer.WriteStartArray("results"u8);

        var itemCount = await JsonStreamReader.WriteArrayAsync(
            upstream,
            "results",
            writer,
            (itemBytes, w) =>
            {
                using var doc = JsonDocument.Parse(itemBytes);
                var root = doc.RootElement;

                w.WriteStartObject();
                w.WriteNumber("id"u8, root.GetProperty("id").GetInt32());
                w.WriteString("message"u8, root.GetProperty("message").GetString());
                w.WriteString("level"u8, root.GetProperty("level").GetString());
                w.WriteEndObject();
            }
        );

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, itemCount);
        writer.WriteEndObject();
        writer.Flush();

        await ctx.Response.BodyWriter.FlushAsync();
        await upstream.CompleteAsync();
    }
);

// ── GET /filter — streaming with predicate ─────────────────────────────
// Only streams items where id is even (demonstrates caller-side filtering).
app.MapGet(
    "/filter",
    async (HttpContext ctx, int count = 100) =>
    {
        ctx.Response.ContentType = "application/json";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        var upstream = SimulateUpstream(count);
        var writer = new Utf8JsonWriter(ctx.Response.BodyWriter);

        writer.WriteStartObject();
        writer.WriteStartArray("results"u8);

        int written = 0;
        await JsonStreamReader.ProcessArrayAsync(
            upstream,
            "results",
            itemBytes =>
            {
                using var doc = JsonDocument.Parse(itemBytes);
                var id = doc.RootElement.GetProperty("id").GetInt32();
                if (id % 2 == 0)
                {
                    doc.RootElement.WriteTo(writer);
                    written++;
                }
            }
        );

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, written);
        writer.WriteEndObject();
        writer.Flush();

        await ctx.Response.BodyWriter.FlushAsync();
        await upstream.CompleteAsync();
    }
);

// ── GET /select-many — flatten nested arrays ───────────────────────────
// Upstream has pages[*].items — flatten all items across pages.
app.MapGet(
    "/select-many",
    async (HttpContext ctx) =>
    {
        ctx.Response.ContentType = "application/json";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        var json = """
        {
            "pages": [
                {"items": [{"id":1,"text":"first"},{"id":2,"text":"second"}]},
                {"items": [{"id":3,"text":"third"}]},
                {"items": [{"id":4,"text":"fourth"},{"id":5,"text":"fifth"}]}
            ]
        }
        """;
        var pipe = PipeReader.Create(
            new MemoryStream(Encoding.UTF8.GetBytes(json)),
            new StreamPipeReaderOptions(bufferSize: 8192)
        );
        var path = JsonPath.Root.Property("pages"u8).Each().Property("items"u8);

        var writer = new Utf8JsonWriter(ctx.Response.BodyWriter);

        writer.WriteStartObject();
        writer.WriteStartArray("results"u8);

        var count = await JsonStreamReader.WriteArrayAsync(pipe, path, writer);

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, count);
        writer.WriteEndObject();
        writer.Flush();

        await ctx.Response.BodyWriter.FlushAsync();
        await pipe.CompleteAsync();
    }
);

app.Run();
