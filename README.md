# JsonStreaming

Bounded-memory streaming JSON array processor for .NET. Reads from `PipeReader`, writes to `Utf8JsonWriter`, with ~3KB managed allocations at 200K items vs 69MB for the standard Deserialize/LINQ/Serialize pattern.

## Why

`System.Text.Json` has `Utf8JsonReader` (low-level, no async, no `PipeReader`) and `JsonDocument`/`JsonSerializer` (full-buffer). Nothing in between for streaming array enumeration with bounded memory and HTTP backpressure.

This library fills that gap: **2.0x faster, ~3KB vs 69MB** than the standard .NET pattern for JSON array relay/transform.

## Install

```bash
dotnet add package JsonStreaming
```

Targets `net8.0`, `net9.0`, `net10.0`.

## Quick Start

### Highest level — one call, everything handled

```csharp
app.MapGet("/api/products", async (HttpContext ctx, IHttpClientFactory httpFactory, CancellationToken ct) =>
{
    await using var upstream = await ctx.StreamFrom(httpFactory, url, ct);

    await JsonStreamPipeline.TransformArrayAsync(
        upstream.Pipe,
        "products",                          // source path in upstream JSON
        ctx.Response.BodyWriter,             // output PipeWriter
        "products",                          // output array name
        Ctx.Default.ProductInput,            // source-gen deserializer
        Ctx.Default.ProductOutput,           // source-gen serializer
        product => new ProductOutput         // transform (return null to filter)
        {
            Id = product.Id,
            Title = product.Title,
            SalePrice = product.Price * 0.9,
        },
        ct
    );
});
// Output: {"products":[...], "count": N}
// Response: transfer-encoding: chunked, with backpressure
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
| **1** | `JsonStreamPipeline` | Transform lambda | Least code, fixed envelope, error recovery |
| **2** | `JsonStreamReaderTyped` | Envelope + types | Custom output structure, source-gen types |
| **3** | `JsonStreamReader.WriteArrayAsync` | Raw `WriteItemDelegate` | Manual field extraction, max throughput |
| **4** | `JsonStreamReader.ProcessArrayAsync` | `ReadOnlySequence<byte>` | Aggregation, non-JSON output, NDJSON |

## Performance

Benchmarked at 200K items (~18MB JSON), Apple M4 Pro, .NET 10:

| Method | Time | Allocated | vs Baseline |
|--------|------|-----------|-------------|
| **Verbatim passthrough** (direct bytes) | **53ms** | **~3KB** | **2.0x faster, ~3KB vs 69MB** |
| Utf8JsonReader transform (select 2 fields) | 92ms | ~7KB | 1.2x faster, ~7KB vs 69MB |
| JsonDocument transform | 119ms | ~24MB | 1.1x slower, 65% less alloc |
| **Baseline** (Deserialize → LINQ → Serialize) | **109ms** | **~69MB** | **1.0x** |
| Typed source-gen transform (TIn → TOut) | 165ms | ~89MB | 1.5x slower, +30% alloc |

**Verbatim passthrough** copies raw item bytes directly to the output pipe — zero parsing overhead, single memcpy per item. Items are already validated by `Utf8JsonReader`; no re-parse needed.

**Flush is faster than no flush**: periodic `writer.Flush()` resets the internal buffer, preventing unbounded growth. With flush disabled at 200K items: ~69ms / ~50MB allocated. With 16KB flush: ~53ms / ~3KB. The GC pressure reduction from smaller buffers more than pays for the flush overhead.

**GC dump analysis** (14 threads, 243M items processed): zero library objects on the heap. Only ArrayPool bucket cache from returned `Utf8JsonWriter` buffer rentals. All `PipeReader` `BufferSegment` rentals correctly returned via `CompleteAsync` → `Reset` → `ArrayPool.Return`.

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

Write-through methods flush automatically when buffered bytes exceed a threshold (90% of 16KB by default, matching `System.Text.Json`). The async flush callback enables true HTTP backpressure:

```csharp
var options = new WriteOptions
{
    FlushThreshold = 16_384,
    AsyncFlush = async ct => { await pipeWriter.FlushAsync(ct); },
};
```

The iteration loop uses a checkpoint-and-resume pattern: save `JsonReaderState`, break the sync inner loop (destroying the ref struct `Utf8JsonReader`), await the flush, reconstruct the reader. Same pattern as `System.Text.Json`'s `IAsyncEnumerable` serializer.

### Filtering

Return `null` from a transform to skip items:

```csharp
await JsonStreamPipeline.TransformArrayAsync(
    pipe, path, output, "photos",
    Ctx.Default.Photo, Ctx.Default.Photo,
    photo => photo.AlbumId == 1 ? photo : null,
    ct
);
```

### NDJSON Streaming

```csharp
ctx.Response.ContentType = "application/x-ndjson";
await JsonStreamReader.ProcessArrayAsync(pipe, "items", itemBytes =>
{
    // one JSON object per line
    output.WriteNdjsonLine(transform(itemBytes), Ctx.Default.OutputType);
}, ct);
```

Or project matched values directly with `NdJsonPath`:

```csharp
await pipe.ProjectNdJsonDirectAsync(
    NdJsonPath.At("products").Each().Key("title"),
    output,
    ct: ct
);
```

With header/footer envelope for error signaling:
```
{"__stream":"begin","streamId":"a1b2c3d4...","version":1}
{"id":1,"title":"..."}
{"id":2,"title":"..."}
{"__stream":"end","streamId":"a1b2c3d4...","count":2}
```

### Source Generator Support

All typed APIs accept `JsonTypeInfo<T>` for AOT-compatible, zero-reflection operation:

```csharp
[JsonSerializable(typeof(ProductInput))]
[JsonSerializable(typeof(ProductOutput))]
public partial class Ctx : JsonSerializerContext;
```

## Sample App

See [`samples/WebApiSample`](samples/WebApiSample/) — 12 endpoints from highest to lowest abstraction:

| Endpoint | Pattern |
|----------|---------|
| `/level1/passthrough` | Pipeline verbatim relay |
| `/level1/transform` | Pipeline typed transform |
| `/level1/filter` | Pipeline filter (null = skip) |
| `/level2/typed` | Custom envelope + metadata |
| `/level3/manual` | WriteItemDelegate, manual fields |
| `/level4/aggregate` | Zero-copy callback, aggregation |
| `/ndjson/products` | NDJSON with typed transform |
| `/ndjson/comments` | NDJSON passthrough |
| `/ndjson/product-titles` | NDJSON via `NdJsonPath` projection |
| `/deep/select-many` | `Each()` across nested pages |
| `/deep/nested` | Deep JsonPath navigation |
| `/multi-source` | Sequential pages, shared writer |

```bash
cd samples/WebApiSample
dotnet run
```

## Architecture

```
PipeReader (input)
    → JsonPathNavigator (navigate to target array)
    → Utf8JsonReader (parse items, ref struct, sync)
    → callback / WriteRawValue / WriteItemDelegate
    → Utf8JsonWriter (output, backed by PipeWriter)
    → checkpoint & flush (async, backpressure)
    → PipeWriter.FlushAsync (HTTP chunked transfer)
```

Key constraint: `Utf8JsonReader` is a ref struct — can't cross `await` or `yield`. The library solves this with a checkpoint pattern: save `JsonReaderState` (plain struct), destroy the reader, await, reconstruct. Same approach as `System.Text.Json`'s internal `ContinueDeserialize` method.

## License

MIT
