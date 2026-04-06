using System.IO.Pipelines;
using System.Text.Json;
using System.Text.Json.Serialization;
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
        var ct = ctx.RequestAborted;
        ctx.Response.ContentType = "application/json";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        using var http = httpFactory.CreateClient();
        using var upstream = await http.SendAsync(
            new HttpRequestMessage(
                HttpMethod.Get,
                "https://jsonplaceholder.typicode.com/comments"
            ),
            HttpCompletionOption.ResponseHeadersRead,
            ct
        );
        await using var stream = await upstream.Content.ReadAsStreamAsync(ct);
        var pipe = PipeReader.Create(stream, new StreamPipeReaderOptions(bufferSize: 8192));

        var pipeWriter = ctx.Response.BodyWriter;
        await using var writer = new Utf8JsonWriter(pipeWriter);
        var options = new WriteOptions
        {
            AsyncFlush = async flushCt =>
            {
                await pipeWriter.FlushAsync(flushCt);
            },
        };

        writer.WriteStartObject();
        writer.WriteStartArray("results"u8);

        var count = await JsonStreamReader.WriteArrayAsync(
            pipe,
            JsonPath.Root,
            writer,
            options,
            ct
        );

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, count);
        writer.WriteEndObject();
        writer.Flush();

        await pipeWriter.FlushAsync(ct);
        await pipe.CompleteAsync();
    }
);

// ── GET /stream/products — typed deserialization + business logic ───────
// Uses source-generated JsonSerializer for zero-reflection deserialization,
// computes a discounted price, and serializes a different output shape.
app.MapGet(
    "/stream/products",
    async (HttpContext ctx, IHttpClientFactory httpFactory, int limit = 100) =>
    {
        var ct = ctx.RequestAborted;
        ctx.Response.ContentType = "application/json";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        using var http = httpFactory.CreateClient();
        using var upstream = await http.SendAsync(
            new HttpRequestMessage(
                HttpMethod.Get,
                $"https://dummyjson.com/products?limit={limit}"
            ),
            HttpCompletionOption.ResponseHeadersRead,
            ct
        );
        await using var stream = await upstream.Content.ReadAsStreamAsync(ct);
        var pipe = PipeReader.Create(stream, new StreamPipeReaderOptions(bufferSize: 8192));

        var pipeWriter = ctx.Response.BodyWriter;
        await using var writer = new Utf8JsonWriter(pipeWriter);
        var options = new WriteOptions
        {
            AsyncFlush = async flushCt =>
            {
                await pipeWriter.FlushAsync(flushCt);
            },
        };

        writer.WriteStartObject();
        writer.WriteStartArray("products"u8);

        var count = await JsonStreamReaderTyped.WriteArrayAsync(
            pipe,
            "products",
            writer,
            SampleJsonContext.Default.ProductInput,
            SampleJsonContext.Default.ProductOutput,
            product => new ProductOutput
            {
                Id = product.Id,
                Title = product.Title,
                Brand = product.Brand ?? "Unknown",
                OriginalPrice = product.Price,
                SalePrice = Math.Round(
                    product.Price * (1 - product.DiscountPercentage / 100),
                    2
                ),
                Rating = product.Rating,
                InStock = product.Stock > 0,
            },
            options,
            ct
        );

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, count);
        writer.WriteEndObject();
        writer.Flush();

        await pipeWriter.FlushAsync(ct);
        await pipe.CompleteAsync();
    }
);

// ── GET /stream/photos — stream 5000 photos, filter by albumId ─────────
// Root-level array, caller-side filtering via WriteArrayAsync transform.
// The delegate writes only matching items — skipped items produce no output.
app.MapGet(
    "/stream/photos",
    async (HttpContext ctx, IHttpClientFactory httpFactory, int albumId = 1) =>
    {
        var ct = ctx.RequestAborted;
        ctx.Response.ContentType = "application/json";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        using var http = httpFactory.CreateClient();
        using var upstream = await http.SendAsync(
            new HttpRequestMessage(
                HttpMethod.Get,
                "https://jsonplaceholder.typicode.com/photos"
            ),
            HttpCompletionOption.ResponseHeadersRead,
            ct
        );
        await using var stream = await upstream.Content.ReadAsStreamAsync(ct);
        var pipe = PipeReader.Create(stream, new StreamPipeReaderOptions(bufferSize: 8192));

        var pipeWriter = ctx.Response.BodyWriter;
        await using var writer = new Utf8JsonWriter(pipeWriter);
        var options = new WriteOptions
        {
            AsyncFlush = async flushCt =>
            {
                await pipeWriter.FlushAsync(flushCt);
            },
        };

        writer.WriteStartObject();
        writer.WriteStartArray("photos"u8);

        int written = 0;
        await JsonStreamReader.WriteArrayAsync(
            pipe,
            JsonPath.Root,
            writer,
            (itemBytes, w) =>
            {
                using var doc = JsonDocument.Parse(itemBytes);
                if (doc.RootElement.GetProperty("albumId").GetInt32() == albumId)
                {
                    doc.RootElement.WriteTo(w);
                    written++;
                }
            },
            options,
            ct
        );

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, written);
        writer.WriteEndObject();
        writer.Flush();

        await pipeWriter.FlushAsync(ct);
        await pipe.CompleteAsync();
    }
);

// ── GET /stream/todos — stream todos across multiple pages ─────────────
// Fetches 3 pages from DummyJSON sequentially, streaming each page's
// $.todos array directly to the client. No buffering of full pages.
app.MapGet(
    "/stream/todos",
    async (HttpContext ctx, IHttpClientFactory httpFactory) =>
    {
        var ct = ctx.RequestAborted;
        ctx.Response.ContentType = "application/json";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        var pipeWriter = ctx.Response.BodyWriter;
        await using var writer = new Utf8JsonWriter(pipeWriter);
        var options = new WriteOptions
        {
            AsyncFlush = async flushCt =>
            {
                await pipeWriter.FlushAsync(flushCt);
            },
        };

        writer.WriteStartObject();
        writer.WriteStartArray("todos"u8);

        using var http = httpFactory.CreateClient();
        int totalCount = 0;

        for (int skip = 0; skip < 90; skip += 30)
        {
            using var upstream = await http.SendAsync(
                new HttpRequestMessage(
                    HttpMethod.Get,
                    $"https://dummyjson.com/todos?limit=30&skip={skip}"
                ),
                HttpCompletionOption.ResponseHeadersRead,
                ct
            );
            await using var stream = await upstream.Content.ReadAsStreamAsync(ct);
            var pipe = PipeReader.Create(
                stream,
                new StreamPipeReaderOptions(bufferSize: 8192)
            );

            totalCount += await JsonStreamReader.WriteArrayAsync(
                pipe,
                "todos",
                writer,
                options,
                ct
            );

            await pipe.CompleteAsync();
        }

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, totalCount);
        writer.WriteEndObject();
        writer.Flush();

        await pipeWriter.FlushAsync(ct);
    }
);

app.Run();

// ── Source-generated JSON types ────────────────────────────────────────
// Zero reflection, AOT-compatible. Used by /stream/products.

public sealed record ProductInput
{
    public int Id { get; init; }
    public string Title { get; init; } = "";
    public string? Brand { get; init; }
    public double Price { get; init; }
    public double DiscountPercentage { get; init; }
    public double Rating { get; init; }
    public int Stock { get; init; }
}

public sealed record ProductOutput
{
    public int Id { get; init; }
    public string Title { get; init; } = "";
    public string Brand { get; init; } = "";
    public double OriginalPrice { get; init; }
    public double SalePrice { get; init; }
    public double Rating { get; init; }
    public bool InStock { get; init; }
}

[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
)]
[JsonSerializable(typeof(ProductInput))]
[JsonSerializable(typeof(ProductOutput))]
public partial class SampleJsonContext : JsonSerializerContext;
