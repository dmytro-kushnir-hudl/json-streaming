using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using JsonStreaming;
using Microsoft.AspNetCore.Http.Features;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHttpClient();
var app = builder.Build();

// ════════════════════════════════════════════════════════════════════════
// LEVEL 1 — Envelope helpers using ProjectItemsAsync
//
// Build the JSON envelope manually, then use ProjectItemsAsync to write
// each matched item as a raw value into the array.
// ════════════════════════════════════════════════════════════════════════

// Passthrough — items flow verbatim from upstream to client.
// CancellationToken is injected by ASP.NET minimal API from ctx.RequestAborted.
// It flows through: HttpClient.SendAsync → PipeReader.ReadAsync → ProjectItemsAsync.
// If the client disconnects, the entire pipeline cancels.
app.MapGet(
    "/level1/passthrough",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct) =>
    {
        await using var upstream = await ctx.StreamFrom(httpFactory, "https://jsonplaceholder.typicode.com/comments", ct);

        var output = ctx.Response.BodyWriter;
        await using var writer = new Utf8JsonWriter(output);

        writer.WriteStartObject();
        writer.WriteStartArray("comments"u8);

        int count = 0;
        await upstream.Pipe.TransformItemsAsync(
            output,
            JsonPath.Root,
            (itemBytes, pw) =>
            {
                writer.WriteRawValue(itemBytes, skipInputValidation: true);
                count++;

            },
            ct: ct
        );

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, count);
        writer.WriteEndObject();
        await writer.FlushAsync(ct);
        await output.FlushAsync(ct);
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

        var output = ctx.Response.BodyWriter;
        await using var writer = new Utf8JsonWriter(output);

        writer.WriteStartObject();
        writer.WriteStartArray("products"u8);

        var count = await upstream.Pipe.ProjectTypedAsync(
            JsonPath.At("products"),
            writer,
            SampleJsonContext.Default.ProductInput,
            SampleJsonContext.Default.ProductOutput,
            product => [new ProductOutput
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
            }],
            ct
        );

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, count);
        writer.WriteEndObject();
        await writer.FlushAsync(ct);
        await output.FlushAsync(ct);
    }
);

// Filter — return empty to skip items.
app.MapGet(
    "/level1/filter",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct, int albumId = 1) =>
    {
        await using var upstream = await ctx.StreamFrom(
            httpFactory,
            "https://jsonplaceholder.typicode.com/photos",
            ct
        );

        var output = ctx.Response.BodyWriter;
        await using var writer = new Utf8JsonWriter(output);

        writer.WriteStartObject();
        writer.WriteStartArray("photos"u8);

        var count = await upstream.Pipe.ProjectTypedAsync(
            JsonPath.Root,
            writer,
            SampleJsonContext.Default.Photo,
            SampleJsonContext.Default.Photo,
            photo => photo.AlbumId == albumId ? [photo] : [],
            ct
        );

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, count);
        writer.WriteEndObject();
        await writer.FlushAsync(ct);
        await output.FlushAsync(ct);
    }
);

// ════════════════════════════════════════════════════════════════════════
// LEVEL 2 — Mid-level: ProjectTypedAsync with custom envelope
//
// You own the Utf8JsonWriter and the JSON envelope. The extension method
// handles deserialization/serialization via source-gen JsonTypeInfo<T>.
// Trade-off: more control over output structure, but you manage
// writer lifecycle, envelope, and flush yourself.
// ════════════════════════════════════════════════════════════════════════

// Typed transform with custom envelope — add metadata fields, nest differently.
// Use when: you need a custom output shape.
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

        // Custom envelope: add request metadata alongside results
        writer.WriteStartObject();
        writer.WriteString("source"u8, "dummyjson.com");
        writer.WriteNumber("limit"u8, limit);
        writer.WriteStartArray("products"u8);

        var count = await upstream.Pipe.ProjectTypedAsync(
            JsonPath.At("products"),
            writer,
            SampleJsonContext.Default.ProductInput,
            SampleJsonContext.Default.ProductOutput,
            product => [new ProductOutput
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
            }],
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
// LEVEL 3 — Low-level: ProjectItemsAsync with raw byte callback
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

        writer.WriteStartObject();
        writer.WriteStartArray("comments"u8);

        int count = 0;
        await upstream.Pipe.TransformItemsAsync(
            PipeWriter.Create(Stream.Null),
            JsonPath.Root,
            (itemBytes, _) =>
            {
                // Parse only what we need — skip 3 of 5 fields
                using var doc = JsonDocument.Parse(itemBytes);
                var root = doc.RootElement;

                writer.WriteStartObject();
                writer.WriteString("email"u8, root.GetProperty("email").GetString());
                writer.WriteString("body"u8, root.GetProperty("body").GetString());
                writer.WriteEndObject();
                count++;

            },
            ct: ct
        );

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, count);
        writer.WriteEndObject();
        writer.Flush();

        await pipeWriter.FlushAsync(ct);
    }
);

// ════════════════════════════════════════════════════════════════════════
// LEVEL 4 — Lowest level: ForEachItemAsync + raw callback
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

        await upstream.Pipe.ForEachItemAsync(
            JsonPath.At("products"),
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

// NDJSON transform — one product per line with trailer for error/completion signal.
// The trailer is the last line: {"__status":"complete","count":N} or {"__status":"error",...}
// Clients: read lines, check __status field to detect end-of-stream and errors.
// Pass ?failAt=N to simulate a mid-stream error at item N (demonstrates error trailer).
app.MapGet(
    "/ndjson/products",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct, int limit = 100, int? failAt = null) =>
    {
        ctx.Response.ContentType = "application/x-ndjson";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        await using var upstream = await ctx.StreamFrom(
            httpFactory,
            $"https://dummyjson.com/products?limit={limit}",
            ct
        );

        var output = ctx.Response.BodyWriter;
        var (header, streamId) = NdjsonEnvelope.CreateHeader();
        output.WriteNdjsonLine(header, SampleJsonContext.Default.NdjsonEnvelope);
        int count = 0;

        try
        {
            await upstream.Pipe.ForEachItemAsync(
                JsonPath.At("products"),
                itemBytes =>
                {
                    var reader = new Utf8JsonReader(itemBytes);
                    var product = JsonSerializer.Deserialize(
                        ref reader,
                        SampleJsonContext.Default.ProductInput
                    );
                    if (product is null)
                        return;

                    // Simulate mid-stream failure
                    if (failAt.HasValue && product.Id == failAt.Value)
                        throw new InvalidOperationException(
                            $"Simulated upstream failure at product {product.Id}"
                        );

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
                        InStock = product.Id % 2 == 0,
                    };

                    output.WriteNdjsonLine(line, SampleJsonContext.Default.ProductOutput);
                    count++;
                },
                ct
            );

            output.WriteNdjsonLine(
                NdjsonEnvelope.CreateFooter(streamId, count),
                SampleJsonContext.Default.NdjsonEnvelope
            );
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Client disconnected — no footer possible
        }
        catch (Exception ex)
        {
            output.WriteNdjsonLine(
                NdjsonEnvelope.CreateErrorFooter(streamId, count, ex.Message),
                SampleJsonContext.Default.NdjsonEnvelope
            );
        }

        await output.FlushAsync(ct);
    }
);

// NDJSON passthrough with trailer — verbatim items, one per line.
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
        var (header, streamId) = NdjsonEnvelope.CreateHeader();
        output.WriteNdjsonLine(header, SampleJsonContext.Default.NdjsonEnvelope);
        int count = 0;

        try
        {
            await upstream.Pipe.ForEachItemAsync(
                JsonPath.Root,
                itemBytes =>
                {
                    // Re-serialize compact — upstream may be pretty-printed
                    using var doc = JsonDocument.Parse(itemBytes);
                    var buf = new ArrayBufferWriter<byte>();
                    using (var lw = new Utf8JsonWriter(buf))
                        doc.RootElement.WriteTo(lw);
                    output.Write(buf.WrittenSpan);
                    output.Write([(byte)'\n']);
                    count++;
                },
                ct
            );

            output.WriteNdjsonLine(
                NdjsonEnvelope.CreateFooter(streamId, count),
                SampleJsonContext.Default.NdjsonEnvelope
            );
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Client disconnected — no footer possible
        }
        catch (Exception ex)
        {
            output.WriteNdjsonLine(
                NdjsonEnvelope.CreateErrorFooter(streamId, count, ex.Message),
                SampleJsonContext.Default.NdjsonEnvelope
            );
        }

        await output.FlushAsync(ct);
    }
);

// NDJSON projection — extract one nested value per line with NdJsonPath.
// No header/footer envelope here: this is the raw projection primitive.
app.MapGet(
    "/ndjson/product-titles",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct, int limit = 100) =>
    {
        ctx.Response.ContentType = "application/x-ndjson";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        await using var upstream = await ctx.StreamFrom(
            httpFactory,
            $"https://dummyjson.com/products?limit={limit}",
            ct
        );

        await upstream.Pipe.ProjectNdJsonVerbatimAsync(
            JsonPath.At("products").Each().Key("title"),
            ctx.Response.BodyWriter,
            ct: ct
        );

        await ctx.Response.BodyWriter.FlushAsync(ct);
    }
);

// ════════════════════════════════════════════════════════════════════════
// DEEP MATCHING — NdJsonPath with Each() for select-many across nested arrays.
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
        var path = JsonPath.At("data").Key("pages").Each().Key("todos");

        var output = ctx.Response.BodyWriter;
        await using var writer = new Utf8JsonWriter(output);

        writer.WriteStartObject();
        writer.WriteStartArray("todos"u8);

        int count = 0;
        await pipe.TransformItemsAsync(
            output,
            path,
            (itemBytes, pw) =>
            {
                writer.WriteRawValue(itemBytes, skipInputValidation: true);
                count++;

            },
            ct: ct
        );

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, count);
        writer.WriteEndObject();
        await writer.FlushAsync(ct);
        await output.FlushAsync(ct);

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
        // Navigate with NdJsonPath: $.products
        var path = JsonPath.At("products");

        var output = ctx.Response.BodyWriter;
        await using var writer = new Utf8JsonWriter(output);

        writer.WriteStartObject();
        writer.WriteStartArray("items"u8);

        var count = await upstream.Pipe.ProjectTypedAsync(
            path,
            writer,
            SampleJsonContext.Default.ProductInput,
            SampleJsonContext.Default.ProductOutput,
            product => [new ProductOutput
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
            }],
            ct
        );

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, count);
        writer.WriteEndObject();
        await writer.FlushAsync(ct);
        await output.FlushAsync(ct);
    }
);

// NdJsonPath.Parse — same deep navigation from a string.
// Use when: the path comes from configuration or user input.
app.MapGet(
    "/deep/jsonpath",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct) =>
    {
        await using var upstream = await ctx.StreamFrom(
            httpFactory,
            "https://dummyjson.com/products?limit=5"
        );

        // Parse from NdJsonPath string — equivalent to NdJsonPath.At("products")
        var path = JsonPath.Parse("$.products");

        var output = ctx.Response.BodyWriter;
        await using var writer = new Utf8JsonWriter(output);

        writer.WriteStartObject();
        writer.WriteStartArray("products"u8);

        int count = 0;
        await upstream.Pipe.TransformItemsAsync(
            output,
            path,
            (itemBytes, pw) =>
            {
                writer.WriteRawValue(itemBytes, skipInputValidation: true);
                count++;

            },
            ct: ct
        );

        writer.WriteEndArray();
        writer.WriteNumber("count"u8, count);
        writer.WriteEndObject();
        await writer.FlushAsync(ct);
        await output.FlushAsync(ct);
    }
);

// ════════════════════════════════════════════════════════════════════════
// LEVEL 3+: Multi-source — sequential pages into one output array.
//
// Not possible with a single ProjectItemsAsync call. Use ProjectItemsAsync
// directly with a shared writer across multiple PipeReaders.
// Trade-off: most boilerplate, but handles patterns a single call can't.
// ════════════════════════════════════════════════════════════════════════

app.MapGet(
    "/multi-source",
    async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct) =>
    {

        ctx.Response.ContentType = "application/json";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        var pipeWriter = ctx.Response.BodyWriter;
        await using var writer = new Utf8JsonWriter(pipeWriter);

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

            int pageCount = 0;
            await pipe.TransformItemsAsync(
                pipeWriter,
                JsonPath.At("todos"),
                (itemBytes, pw) =>
                {
                    writer.WriteRawValue(itemBytes, skipInputValidation: true);
                    pageCount++;
    
                },
                ct: ct
            );
            totalCount += pageCount;

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

static class PipeWriterNdjsonExtensions
{
    private static readonly byte[] Newline = [(byte)'\n'];

    /// <summary>
    /// Serialize a value as a single NDJSON line (compact JSON + newline).
    /// </summary>
    public static void WriteNdjsonLine<T>(
        this PipeWriter output,
        T value,
        JsonTypeInfo<T> typeInfo
    )
    {
        var buf = new ArrayBufferWriter<byte>();
        using (var w = new Utf8JsonWriter(buf))
            JsonSerializer.Serialize(w, value, typeInfo);
        output.Write(buf.WrittenSpan);
        output.Write(Newline);
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

/// <summary>
/// NDJSON envelope — first and last line of the stream.
/// Header: {"__stream":"begin","streamId":"...","version":1}
/// Footer: {"__stream":"end","streamId":"...","count":N}
/// Error:  {"__stream":"end","streamId":"...","count":N,"error":"..."}
///
/// The streamId is a random GUID that must match between header and footer.
/// Clients verify the footer's streamId matches the header to detect corruption.
/// </summary>
public sealed record NdjsonEnvelope
{
    [JsonPropertyName("__stream")]
    public string Stream { get; init; } = "";

    public string StreamId { get; init; } = "";
    public int? Version { get; init; }
    public int? Count { get; init; }
    public string? Error { get; init; }

    public static (NdjsonEnvelope Header, string Id) CreateHeader()
    {
        var id = Guid.NewGuid().ToString("N");
        return (new NdjsonEnvelope { Stream = "begin", StreamId = id, Version = 1 }, id);
    }

    public static NdjsonEnvelope CreateFooter(string streamId, int count) =>
        new() { Stream = "end", StreamId = streamId, Count = count };

    public static NdjsonEnvelope CreateErrorFooter(string streamId, int count, string error) =>
        new() { Stream = "end", StreamId = streamId, Count = count, Error = error };
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
[JsonSerializable(typeof(NdjsonEnvelope))]
public partial class SampleJsonContext : JsonSerializerContext;
