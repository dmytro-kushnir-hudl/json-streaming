using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;
using System.Text.Json.Serialization;
using JsonStreaming;
using Microsoft.AspNetCore.Http.Features;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHttpClient();
var app = builder.Build();

// ════════════════════════════════════════════════════════════════════════
// LEVEL 1 — Highest abstraction: JsonStreamPipeline
//
// One call does everything: envelope, flush, backpressure, error recovery.
// You provide: input pipe, path, output pipe, types, transform lambda.
// Trade-off: least control, but fewest ways to get it wrong.
// ════════════════════════════════════════════════════════════════════════

// Passthrough — items flow verbatim from upstream to client.
// CancellationToken is injected by ASP.NET minimal API from ctx.RequestAborted.
// It flows through: HttpClient.SendAsync → PipeReader.ReadAsync → WriteArrayAsync → AsyncFlush.
// If the client disconnects, the entire pipeline cancels.
app.MapGet(
    "/level1/passthrough",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct) =>
    {
        await using var upstream = await ctx.StreamFrom(httpFactory, "https://jsonplaceholder.typicode.com/comments", ct);

        await JsonStreamPipeline.PassthroughArrayAsync(
            upstream.Pipe,
            JsonPath.Root,
            ctx.Response.BodyWriter,
            "comments",
            ct
        );
    }
);

// Typed transform — CancellationToken flows the same way.
app.MapGet(
    "/level1/transform",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct, int limit = 30) =>
    {
        await using var upstream = await ctx.StreamFrom(
            httpFactory,
            $"https://dummyjson.com/products?limit={limit}",
            ct
        );

        await JsonStreamPipeline.TransformArrayAsync(
            upstream.Pipe,
            "products",
            ctx.Response.BodyWriter,
            "products",
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
            ct
        );
    }
);

// Filter — return null to skip items.
app.MapGet(
    "/level1/filter",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct, int albumId = 1) =>
    {
        await using var upstream = await ctx.StreamFrom(
            httpFactory,
            "https://jsonplaceholder.typicode.com/photos",
            ct
        );

        await JsonStreamPipeline.TransformArrayAsync(
            upstream.Pipe,
            JsonPath.Root,
            ctx.Response.BodyWriter,
            "photos",
            SampleJsonContext.Default.Photo,
            SampleJsonContext.Default.Photo,
            photo => photo.AlbumId == albumId ? photo : null,
            ct
        );
    }
);

// ════════════════════════════════════════════════════════════════════════
// LEVEL 2 — Mid-level: JsonStreamReaderTyped
//
// You own the Utf8JsonWriter and the JSON envelope. The library handles
// deserialization/serialization via source-gen JsonTypeInfo<T>.
// Trade-off: more control over output structure, but you manage
// writer lifecycle, envelope, and flush yourself.
// ════════════════════════════════════════════════════════════════════════

// Typed transform with custom envelope — add metadata fields, nest differently.
// Use when: Pipeline doesn't match your output shape.
app.MapGet(
    "/level2/typed",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct, int limit = 30) =>
    {
        await using var upstream = await ctx.StreamFrom(
            httpFactory,
            $"https://dummyjson.com/products?limit={limit}",
            ct
        );

        var pipeWriter = ctx.Response.BodyWriter;
        await using var writer = new Utf8JsonWriter(pipeWriter);
        var options = new WriteOptions
        {
            AsyncFlush = async flushCt =>
            {
                await pipeWriter.FlushAsync(flushCt);
            },
        };

        // Custom envelope: add request metadata alongside results
        writer.WriteStartObject();
        writer.WriteString("source"u8, "dummyjson.com");
        writer.WriteNumber("limit"u8, limit);
        writer.WriteStartArray("products"u8);

        var count = await JsonStreamReaderTyped.WriteArrayAsync(
            upstream.Pipe,
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
        writer.WriteBoolean("complete"u8, true);
        writer.WriteEndObject();
        writer.Flush();

        await pipeWriter.FlushAsync(ct);
    }
);

// ════════════════════════════════════════════════════════════════════════
// LEVEL 3 — Low-level: JsonStreamReader.WriteArrayAsync + WriteItemDelegate
//
// You get raw bytes per item. You decide what to parse and what to write.
// Trade-off: full control, but you handle Utf8JsonReader/JsonDocument yourself.
// Use when: you need partial field extraction, or mixed parsing strategies.
// ════════════════════════════════════════════════════════════════════════

// Manual field extraction — pick specific fields without deserializing the full object.
// Faster than typed deserialization when you only need 2-3 fields from a 20-field object.
app.MapGet(
    "/level3/manual",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct) =>
    {
        await using var upstream = await ctx.StreamFrom(
            httpFactory,
            "https://jsonplaceholder.typicode.com/comments"
        );

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
        writer.WriteStartArray("comments"u8);

        var count = await JsonStreamReader.WriteArrayAsync(
            upstream.Pipe,
            JsonPath.Root,
            writer,
            (itemBytes, w) =>
            {
                // Parse only what we need — skip 3 of 5 fields
                using var doc = JsonDocument.Parse(itemBytes);
                var root = doc.RootElement;

                w.WriteStartObject();
                w.WriteString("email"u8, root.GetProperty("email").GetString());
                w.WriteString("body"u8, root.GetProperty("body").GetString());
                w.WriteEndObject();
            },
            options,
            ct
        );

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, count);
        writer.WriteEndObject();
        writer.Flush();

        await pipeWriter.FlushAsync(ct);
    }
);

// ════════════════════════════════════════════════════════════════════════
// LEVEL 4 — Lowest level: JsonStreamReader.ProcessArrayAsync + raw callback
//
// Zero-copy: you get ReadOnlySequence<byte> per item. No writer involved.
// Trade-off: maximum performance and flexibility, but you handle everything.
// Use when: aggregation, side effects, or non-JSON output.
// ════════════════════════════════════════════════════════════════════════

// Aggregation — count items by category without writing any JSON items.
app.MapGet(
    "/level4/aggregate",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct) =>
    {
        await using var upstream = await ctx.StreamFrom(
            httpFactory,
            "https://dummyjson.com/products?limit=1000"
        );

        // Aggregate in the callback — no output streaming needed
        var brandCounts = new Dictionary<string, int>();
        double totalValue = 0;

        await JsonStreamReader.ProcessArrayAsync(
            upstream.Pipe,
            "products",
            itemBytes =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                var product = JsonSerializer.Deserialize(
                    ref reader,
                    SampleJsonContext.Default.ProductInput
                );
                if (product is null)
                    return;

                var brand = product.Brand ?? "Unknown";
                brandCounts[brand] = brandCounts.GetValueOrDefault(brand) + 1;
                totalValue += product.Price;
            },
            ct
        );

        // Write the aggregation result (small, no streaming needed)
        ctx.Response.ContentType = "application/json";
        var output = ctx.Response.BodyWriter;
        await using var writer = new Utf8JsonWriter(output);

        writer.WriteStartObject();
        writer.WriteNumber("totalProducts"u8, brandCounts.Values.Sum());
        writer.WriteNumber("totalValue"u8, Math.Round(totalValue, 2));
        writer.WriteStartObject("brandCounts"u8);
        foreach (var (brand, count) in brandCounts.OrderByDescending(kv => kv.Value))
            writer.WriteNumber(brand, count);
        writer.WriteEndObject();
        writer.WriteEndObject();
        writer.Flush();

        await output.FlushAsync(ct);
    }
);

// ════════════════════════════════════════════════════════════════════════
// NDJSON — Newline-delimited JSON streaming.
//
// Each item is a complete JSON object on its own line. No wrapping array.
// Content-Type: application/x-ndjson. Clients read line-by-line as data arrives.
// Works with: fetch + ReadableStream, curl --no-buffer, EventSource polyfills.
// ════════════════════════════════════════════════════════════════════════

// NDJSON transform — one product per line, transformed shape.
app.MapGet(
    "/ndjson/products",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct, int limit = 100) =>
    {
        ctx.Response.ContentType = "application/x-ndjson";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        await using var upstream = await ctx.StreamFrom(
            httpFactory,
            $"https://dummyjson.com/products?limit={limit}",
            ct
        );

        var output = ctx.Response.BodyWriter;
        byte[] newline = [(byte)'\n'];

        await JsonStreamReader.ProcessArrayAsync(
            upstream.Pipe,
            "products",
            itemBytes =>
            {
                var reader = new Utf8JsonReader(itemBytes);
                var product = JsonSerializer.Deserialize(
                    ref reader,
                    SampleJsonContext.Default.ProductInput
                );
                if (product is null)
                    return;

                var line = new ProductOutput
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
                };

                // Write one JSON object per line — no array wrapper
                var buf = new ArrayBufferWriter<byte>();
                using (var lw = new Utf8JsonWriter(buf))
                    JsonSerializer.Serialize(lw, line, SampleJsonContext.Default.ProductOutput);
                output.Write(buf.WrittenSpan);
                output.Write(newline);
            },
            ct
        );

        await output.FlushAsync(ct);
    }
);

// NDJSON passthrough — verbatim items, one per line. Simplest possible.
app.MapGet(
    "/ndjson/comments",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct) =>
    {
        ctx.Response.ContentType = "application/x-ndjson";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        await using var upstream = await ctx.StreamFrom(
            httpFactory,
            "https://jsonplaceholder.typicode.com/comments",
            ct
        );

        var output = ctx.Response.BodyWriter;
        byte[] newline = [(byte)'\n'];

        await JsonStreamReader.ProcessArrayAsync(
            upstream.Pipe,
            JsonPath.Root,
            itemBytes =>
            {
                // Re-serialize compact — upstream may be pretty-printed
                using var doc = JsonDocument.Parse(itemBytes);
                var buf = new ArrayBufferWriter<byte>();
                using (var lw = new Utf8JsonWriter(buf))
                    doc.RootElement.WriteTo(lw);
                output.Write(buf.WrittenSpan);
                output.Write(newline);
            },
            ct
        );

        await output.FlushAsync(ct);
    }
);

// ════════════════════════════════════════════════════════════════════════
// DEEP MATCHING — JsonPath with Each() for select-many across nested arrays.
//
// Upstream shape: {"pages": [{"todos": [...]}, {"todos": [...]}, ...]}
// Each() flattens: iterate each page, yield all todos across all pages.
// Also works with nested paths: $.data.response[*].results.items
// ════════════════════════════════════════════════════════════════════════

// Select-many: flatten nested arrays from a single upstream response.
// DummyJSON doesn't return this shape natively, so we build it from 3 pages
// then stream through a single PipeReader with Each().
app.MapGet(
    "/deep/select-many",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct) =>
    {

        ctx.Response.ContentType = "application/json";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        // Build a nested structure: {"data":{"pages":[{page1},{page2},{page3}]}}
        using var http = httpFactory.CreateClient();
        var pages = new List<string>();
        for (int skip = 0; skip < 90; skip += 30)
        {
            pages.Add(
                await http.GetStringAsync(
                    $"https://dummyjson.com/todos?limit=30&skip={skip}",
                    ct
                )
            );
        }

        var nested = $$$"""{"data":{"pages":[{{{string.Join(",", pages)}}}]}}""";
        var pipe = PipeReader.Create(
            new MemoryStream(System.Text.Encoding.UTF8.GetBytes(nested)),
            new StreamPipeReaderOptions(bufferSize: 8192)
        );

        // Deep path: navigate data → pages → [*] → todos
        var path = JsonPath.Root
            .Property("data"u8)
            .Property("pages"u8)
            .Each()
            .Property("todos"u8);

        await JsonStreamPipeline.PassthroughArrayAsync(
            pipe,
            path,
            ctx.Response.BodyWriter,
            "todos",
            ct
        );

        await pipe.CompleteAsync();
    }
);

// Nested path without Each() — navigate deep into a single response.
// Demonstrates $.response.data.items style deep navigation.
app.MapGet(
    "/deep/nested",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct) =>
    {
        await using var upstream = await ctx.StreamFrom(
            httpFactory,
            "https://dummyjson.com/products?limit=5"
        );

        // DummyJSON wraps products in {"products": [...], "total": ...}
        // Navigate with JsonPath: $.products
        // For deeper nesting, use: JsonPath.Root.Property("a"u8).Property("b"u8).Property("c"u8)
        var path = JsonPath.Root.Property("products"u8);

        await JsonStreamPipeline.TransformArrayAsync(
            upstream.Pipe,
            path,
            ctx.Response.BodyWriter,
            "items",
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
            ct
        );
    }
);

// JSONPath.Parse — same deep navigation from a string.
// Use when: the path comes from configuration or user input.
app.MapGet(
    "/deep/jsonpath",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct) =>
    {
        await using var upstream = await ctx.StreamFrom(
            httpFactory,
            "https://dummyjson.com/products?limit=5"
        );

        // Parse from JSONPath string — equivalent to JsonPath.Root.Property("products"u8)
        var path = JsonPath.Parse("$.products");

        await JsonStreamPipeline.PassthroughArrayAsync(
            upstream.Pipe,
            path,
            ctx.Response.BodyWriter,
            "products",
            ct
        );
    }
);

// ════════════════════════════════════════════════════════════════════════
// LEVEL 3+: Multi-source — sequential pages into one output array.
//
// Not possible with Pipeline (single input). Use WriteArrayAsync directly
// with a shared writer across multiple PipeReaders.
// Trade-off: most boilerplate, but handles patterns Pipeline can't.
// ════════════════════════════════════════════════════════════════════════

app.MapGet(
    "/multi-source",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct) =>
    {

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

        using var http = httpFactory.CreateClient();

        writer.WriteStartObject();
        writer.WriteStartArray("todos"u8);

        int totalCount = 0;
        for (int skip = 0; skip < 90; skip += 30)
        {
            using var resp = await http.SendAsync(
                new HttpRequestMessage(
                    HttpMethod.Get,
                    $"https://dummyjson.com/todos?limit=30&skip={skip}"
                ),
                HttpCompletionOption.ResponseHeadersRead,
                ct
            );
            await using var stream = await resp.Content.ReadAsStreamAsync(ct);
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

// ── ASP.NET helper ─────────────────────────────────────────────────────

static class HttpContextExtensions
{
    /// <summary>
    /// Fetch a URL with streaming and return a PipeReader handle.
    /// </summary>
    public static Task<UpstreamPipe> StreamFrom(
        this HttpContext ctx,
        IHttpClientFactory httpFactory,
        string url,
        CancellationToken ct = default
    ) => ctx.StreamFrom(httpFactory, new HttpRequestMessage(HttpMethod.Get, url), ct);

    /// <summary>
    /// Send any HttpRequestMessage with streaming and return a PipeReader handle.
    /// Use for POST, custom headers, auth tokens, etc.
    /// </summary>
    public static async Task<UpstreamPipe> StreamFrom(
        this HttpContext ctx,
        IHttpClientFactory httpFactory,
        HttpRequestMessage request,
        CancellationToken ct = default
    )
    {
        ctx.Response.ContentType = "application/json";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        var http = httpFactory.CreateClient();
        var upstream = await http.SendAsync(
            request,
            HttpCompletionOption.ResponseHeadersRead,
            ct
        );
        var stream = await upstream.Content.ReadAsStreamAsync(ct);
        var pipe = PipeReader.Create(stream, new StreamPipeReaderOptions(bufferSize: 8192));

        return new UpstreamPipe(pipe, http, upstream);
    }
}

sealed class UpstreamPipe(PipeReader pipe, HttpClient http, HttpResponseMessage response)
    : IAsyncDisposable
{
    public PipeReader Pipe => pipe;

    public async ValueTask DisposeAsync()
    {
        await pipe.CompleteAsync();
        response.Dispose();
        http.Dispose();
    }
}

// ── Source-generated JSON types (AOT-compatible, zero reflection) ───────

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

public sealed record Photo
{
    public int AlbumId { get; init; }
    public int Id { get; init; }
    public string Title { get; init; } = "";
    public string Url { get; init; } = "";
    public string ThumbnailUrl { get; init; } = "";
}

[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
)]
[JsonSerializable(typeof(ProductInput))]
[JsonSerializable(typeof(ProductOutput))]
[JsonSerializable(typeof(Photo))]
public partial class SampleJsonContext : JsonSerializerContext;
