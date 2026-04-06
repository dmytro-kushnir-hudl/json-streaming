# JsonStreaming

Bounded-memory streaming JSON array processor for .NET. Reads from `PipeReader`, writes to `Utf8JsonWriter`, with ~8KB working set regardless of input size.

## Why

`System.Text.Json` has `Utf8JsonReader` (low-level, no async, no `PipeReader`) and `JsonDocument`/`JsonSerializer` (full-buffer). Nothing in between for streaming array enumeration with bounded memory and HTTP backpressure.

This library fills that gap.

## Install

```bash
dotnet add package JsonStreaming
```

Targets `net8.0`, `net9.0`, `net10.0`.

## Quick Start

### Highest level — one call, everything handled

```csharp
await using var upstream = await ctx.StreamFrom(httpFactory, url, ct);

await JsonStreamPipeline.TransformArrayAsync(
    upstream.Pipe,
    "products",                              // source path in upstream JSON
    ctx.Response.BodyWriter,                 // output PipeWriter
    "products",                              // output array name
    Ctx.Default.ProductInput,                // source-gen deserializer
    Ctx.Default.ProductOutput,               // source-gen serializer
    product => new ProductOutput             // transform (return null to filter)
    {
        Id = product.Id,
        Title = product.Title,
        SalePrice = product.Price * 0.9,
    },
    ct
);
// Output: {"products":[...], "count": N}
```

### Lowest level — zero-copy callback

```csharp
await JsonStreamReader.ProcessArrayAsync(pipe, "items", itemBytes =>
{
    // itemBytes is ReadOnlySequence<byte> — valid only during this call
    var reader = new Utf8JsonReader(itemBytes);
    // parse, aggregate, side-effect — your choice
}, ct);
```

## API Layers

| Level | Class | You control | Trade-off |
|-------|-------|-------------|-----------|
| **1** | `JsonStreamPipeline` | Transform lambda | Least code, fixed envelope |
| **2** | `JsonStreamReaderTyped` | Envelope + types | Custom output structure |
| **3** | `JsonStreamReader.WriteArrayAsync` | Raw `WriteItemDelegate` | Manual field extraction |
| **4** | `JsonStreamReader.ProcessArrayAsync` | `ReadOnlySequence<byte>` | Aggregation, non-JSON output |

## Features

### JsonPath Navigation

```csharp
// Fluent builder (compile-time safe)
var path = JsonPath.Root.Property("response"u8).Property("data"u8).Property("items"u8);

// Parse from string (config, user input)
var path = JsonPath.Parse("$.response.data.items");
```

### Select-Many with `Each()`

Flatten nested arrays across sibling objects:

```csharp
// $.pages[*].results → iterate each page, yield all results
var path = JsonPath.Root.Property("pages"u8).Each().Property("results"u8);

await JsonStreamPipeline.PassthroughArrayAsync(pipe, path, output, "results", ct);
```

### HTTP Backpressure

Write-through methods flush automatically when buffered bytes exceed a threshold. The async flush callback enables true HTTP backpressure via `PipeWriter.FlushAsync`:

```csharp
var options = new WriteOptions
{
    FlushThreshold = 16_384,  // flush at ~14.7KB (90% of 16KB)
    AsyncFlush = async ct => { await pipeWriter.FlushAsync(ct); },
};
```

The iteration loop uses a checkpoint-and-resume pattern: save `JsonReaderState`, break the sync inner loop (destroying the ref struct `Utf8JsonReader`), await the flush, reconstruct the reader.

### Filtering

Return `null` from a transform to skip items:

```csharp
await JsonStreamPipeline.TransformArrayAsync(
    pipe, path, output, "photos",
    Ctx.Default.Photo, Ctx.Default.Photo,
    photo => photo.AlbumId == 1 ? photo : null,  // null = skip
    ct
);
```

### Source Generator Support

All typed APIs accept `JsonTypeInfo<T>` for AOT-compatible, zero-reflection operation:

```csharp
[JsonSerializable(typeof(ProductInput))]
[JsonSerializable(typeof(ProductOutput))]
public partial class Ctx : JsonSerializerContext;

// Used as: Ctx.Default.ProductInput, Ctx.Default.ProductOutput
```

## ASP.NET Core Integration

```csharp
app.MapGet("/api/products", async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct) =>
{
    await using var upstream = await ctx.StreamFrom(httpFactory, "https://api.example.com/products", ct);

    await JsonStreamPipeline.TransformArrayAsync(
        upstream.Pipe, "products", ctx.Response.BodyWriter, "products",
        Ctx.Default.ProductInput, Ctx.Default.ProductOutput,
        product => new ProductOutput { Id = product.Id, Title = product.Title },
        ct
    );
});
```

Response streams as `transfer-encoding: chunked` with backpressure. Client disconnect cancels the entire pipeline via `CancellationToken`.

## Performance

At 100K items (~15MB JSON):

| Method | Time | Allocated |
|--------|------|-----------|
| `ProcessArrayAsync` (callback) | 16ms | 9KB |
| `JsonDocument.Parse` (baseline) | 15ms | 48MB |
| `JsonSerializer.Deserialize` (baseline) | 14ms | 154MB |

The library adds ~9KB constant overhead regardless of input size. Streaming means the first byte reaches the client before the last byte is read from upstream.

## Sample App

See [`samples/WebApiSample`](samples/WebApiSample/) for 9 endpoints demonstrating every abstraction level, from one-liner passthrough to multi-source page aggregation.

```bash
cd samples/WebApiSample
dotnet run
# http://localhost:5000/level1/passthrough
# http://localhost:5000/level1/transform?limit=10
# http://localhost:5000/level1/filter?albumId=2
# http://localhost:5000/level4/aggregate
# http://localhost:5000/deep/select-many
```

## License

MIT
