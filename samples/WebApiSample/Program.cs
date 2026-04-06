using System.IO.Pipelines;
using System.Text.Json;
using JsonStreaming;
using Microsoft.AspNetCore.Http.Features;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHttpClient();
var app = builder.Build();

// ── GET /stream/comments — stream 500 comments from JSONPlaceholder ────
// Root-level array → flat path. Verbatim passthrough with backpressure.
app.MapGet(
    "/stream/comments",
    async (HttpContext ctx, IHttpClientFactory httpFactory) =>
    {
        ctx.Response.ContentType = "application/json";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        using var http = httpFactory.CreateClient();
        using var upstream = await http.SendAsync(
            new HttpRequestMessage(
                HttpMethod.Get,
                "https://jsonplaceholder.typicode.com/comments"
            ),
            HttpCompletionOption.ResponseHeadersRead
        );
        var stream = await upstream.Content.ReadAsStreamAsync();
        var pipe = PipeReader.Create(stream, new StreamPipeReaderOptions(bufferSize: 8192));

        var pipeWriter = ctx.Response.BodyWriter;
        using var writer = new Utf8JsonWriter(pipeWriter);
        var options = new WriteOptions
        {
            AsyncFlush = async ct => { await pipeWriter.FlushAsync(ct); },
        };

        writer.WriteStartObject();
        writer.WriteStartArray("results"u8);

        var count = await JsonStreamReader.WriteArrayAsync(
            pipe,
            JsonPath.Root, // root-level array
            writer,
            options
        );

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, count);
        writer.WriteEndObject();
        writer.Flush();

        await pipeWriter.FlushAsync();
        await pipe.CompleteAsync();
    }
);

// ── GET /stream/products — stream products from DummyJSON ──────────────
// Nested array at $.products. Transform: keep only id, title, price, rating.
app.MapGet(
    "/stream/products",
    async (HttpContext ctx, IHttpClientFactory httpFactory, int limit = 100) =>
    {
        ctx.Response.ContentType = "application/json";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        using var http = httpFactory.CreateClient();
        using var upstream = await http.SendAsync(
            new HttpRequestMessage(
                HttpMethod.Get,
                $"https://dummyjson.com/products?limit={limit}"
            ),
            HttpCompletionOption.ResponseHeadersRead
        );
        var stream = await upstream.Content.ReadAsStreamAsync();
        var pipe = PipeReader.Create(stream, new StreamPipeReaderOptions(bufferSize: 8192));

        var pipeWriter = ctx.Response.BodyWriter;
        using var writer = new Utf8JsonWriter(pipeWriter);
        var options = new WriteOptions
        {
            AsyncFlush = async ct => { await pipeWriter.FlushAsync(ct); },
        };

        writer.WriteStartObject();
        writer.WriteStartArray("products"u8);

        var count = await JsonStreamReader.WriteArrayAsync(
            pipe,
            "products",
            writer,
            (itemBytes, w) =>
            {
                using var doc = JsonDocument.Parse(itemBytes);
                var root = doc.RootElement;

                w.WriteStartObject();
                w.WriteNumber("id"u8, root.GetProperty("id").GetInt32());
                w.WriteString("title"u8, root.GetProperty("title").GetString());
                w.WriteNumber("price"u8, root.GetProperty("price").GetDouble());
                w.WriteNumber("rating"u8, root.GetProperty("rating").GetDouble());
                w.WriteEndObject();
            },
            options
        );

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, count);
        writer.WriteEndObject();
        writer.Flush();

        await pipeWriter.FlushAsync();
        await pipe.CompleteAsync();
    }
);

// ── GET /stream/photos — stream 5000 photos, filter by albumId ─────────
// Root-level array, caller-side filtering. Only emit photos from album 1.
app.MapGet(
    "/stream/photos",
    async (HttpContext ctx, IHttpClientFactory httpFactory, int albumId = 1) =>
    {
        ctx.Response.ContentType = "application/json";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        using var http = httpFactory.CreateClient();
        using var upstream = await http.SendAsync(
            new HttpRequestMessage(
                HttpMethod.Get,
                "https://jsonplaceholder.typicode.com/photos"
            ),
            HttpCompletionOption.ResponseHeadersRead
        );
        var stream = await upstream.Content.ReadAsStreamAsync();
        var pipe = PipeReader.Create(stream, new StreamPipeReaderOptions(bufferSize: 8192));

        var pipeWriter = ctx.Response.BodyWriter;
        using var writer = new Utf8JsonWriter(pipeWriter);
        var options = new WriteOptions
        {
            AsyncFlush = async ct => { await pipeWriter.FlushAsync(ct); },
        };

        writer.WriteStartObject();
        writer.WriteStartArray("photos"u8);

        int written = 0;
        await JsonStreamReader.ProcessArrayAsync(
            pipe,
            JsonPath.Root,
            itemBytes =>
            {
                using var doc = JsonDocument.Parse(itemBytes);
                if (doc.RootElement.GetProperty("albumId").GetInt32() == albumId)
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

        await pipeWriter.FlushAsync();
        await pipe.CompleteAsync();
    }
);

// ── GET /stream/todos — stream todos, select-many across pages ─────────
// Simulates paginated upstream by fetching multiple pages and flattening.
app.MapGet(
    "/stream/todos",
    async (HttpContext ctx, IHttpClientFactory httpFactory) =>
    {
        ctx.Response.ContentType = "application/json";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        // Fetch 3 pages from DummyJSON and wrap into a select-many structure
        using var http = httpFactory.CreateClient();
        var pages = new List<string>();
        for (int skip = 0; skip < 90; skip += 30)
        {
            var resp = await http.GetStringAsync(
                $"https://dummyjson.com/todos?limit=30&skip={skip}"
            );
            pages.Add(resp);
        }

        // Build a wrapper: {"pages":[{page1},{page2},{page3}]}
        var combined = $$"""{"pages":[{{string.Join(",", pages)}}]}""";
        var pipe = PipeReader.Create(
            new MemoryStream(System.Text.Encoding.UTF8.GetBytes(combined)),
            new StreamPipeReaderOptions(bufferSize: 8192)
        );

        var path = JsonPath.Root.Property("pages"u8).Each().Property("todos"u8);
        var pipeWriter = ctx.Response.BodyWriter;
        using var writer = new Utf8JsonWriter(pipeWriter);
        var options = new WriteOptions
        {
            AsyncFlush = async ct => { await pipeWriter.FlushAsync(ct); },
        };

        writer.WriteStartObject();
        writer.WriteStartArray("todos"u8);

        var count = await JsonStreamReader.WriteArrayAsync(pipe, path, writer, options);

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, count);
        writer.WriteEndObject();
        writer.Flush();

        await pipeWriter.FlushAsync();
        await pipe.CompleteAsync();
    }
);

app.Run();
