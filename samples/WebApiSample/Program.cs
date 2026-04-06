using System.IO.Pipelines;
using System.Text.Json;
using System.Text.Json.Serialization;
using JsonStreaming;
using Microsoft.AspNetCore.Http.Features;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHttpClient();
var app = builder.Build();

// ── GET /stream/comments — verbatim passthrough ────────────────────────
// 500 comments from JSONPlaceholder, root array, no transformation.
app.MapGet(
    "/stream/comments",
    async (HttpContext ctx, IHttpClientFactory httpFactory) =>
    {
        await using var upstream = await ctx.StreamFrom(httpFactory, "https://jsonplaceholder.typicode.com/comments");

        await JsonStreamPipeline.PassthroughArrayAsync(
            upstream.Pipe,
            JsonPath.Root,
            ctx.Response.BodyWriter,
            "results",
            upstream.Ct
        );
    }
);

// ── GET /stream/products — typed transform ─────────────────────────────
// Deserialize ProductInput → compute sale price → serialize ProductOutput.
app.MapGet(
    "/stream/products",
    async (HttpContext ctx, IHttpClientFactory httpFactory, int limit = 100) =>
    {
        await using var upstream = await ctx.StreamFrom(
            httpFactory,
            $"https://dummyjson.com/products?limit={limit}"
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
            upstream.Ct
        );
    }
);

// ── GET /stream/photos — filter by albumId ─────────────────────────────
// 5000 photos, return null from transform to skip non-matching items.
app.MapGet(
    "/stream/photos",
    async (HttpContext ctx, IHttpClientFactory httpFactory, int albumId = 1) =>
    {
        await using var upstream = await ctx.StreamFrom(
            httpFactory,
            "https://jsonplaceholder.typicode.com/photos"
        );

        await JsonStreamPipeline.TransformArrayAsync(
            upstream.Pipe,
            JsonPath.Root,
            ctx.Response.BodyWriter,
            "photos",
            SampleJsonContext.Default.Photo,
            SampleJsonContext.Default.Photo,
            photo => photo.AlbumId == albumId ? photo : null,
            upstream.Ct
        );
    }
);

// ── GET /stream/todos — sequential pages, same output array ────────────
// Fetches 3 pages, streams each page's $.todos into one output array.
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

        using var http = httpFactory.CreateClient();

        writer.WriteStartObject();
        writer.WriteStartArray("todos"u8);

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

// ── ASP.NET helper ─────────────────────────────────────────────────────

static class HttpContextExtensions
{
    /// <summary>
    /// Fetches a URL with streaming (ResponseHeadersRead), sets response headers,
    /// and returns a handle that owns the HttpClient + response lifecycle.
    /// Dispose the handle after streaming is complete.
    /// </summary>
    public static async Task<UpstreamPipe> StreamFrom(
        this HttpContext ctx,
        IHttpClientFactory httpFactory,
        string url
    )
    {
        ctx.Response.ContentType = "application/json";
        ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        var ct = ctx.RequestAborted;
        var http = httpFactory.CreateClient();
        var upstream = await http.SendAsync(
            new HttpRequestMessage(HttpMethod.Get, url),
            HttpCompletionOption.ResponseHeadersRead,
            ct
        );
        var stream = await upstream.Content.ReadAsStreamAsync(ct);
        var pipe = PipeReader.Create(stream, new StreamPipeReaderOptions(bufferSize: 8192));

        return new UpstreamPipe(pipe, ct, http, upstream);
    }
}

/// <summary>
/// Owns the PipeReader + upstream HTTP resources. Dispose after streaming.
/// </summary>
sealed class UpstreamPipe(
    PipeReader pipe,
    CancellationToken ct,
    HttpClient http,
    HttpResponseMessage response
) : IAsyncDisposable
{
    public PipeReader Pipe => pipe;
    public CancellationToken Ct => ct;

    public async ValueTask DisposeAsync()
    {
        await pipe.CompleteAsync();
        response.Dispose();
        http.Dispose();
    }
}

// ── Source-generated JSON types ────────────────────────────────────────

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
