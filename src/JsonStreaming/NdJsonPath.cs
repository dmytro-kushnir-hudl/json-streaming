using System.Text;

namespace JsonStreaming;

/// <summary>
/// Root-anchored, compile-time encoded JSON path pattern for NDJSON projection.
///
/// Segments: non-empty byte array = UTF-8 property name to match,
/// empty byte array (<see cref="Wildcard"/>) = array wildcard (any index).
///
/// Max meaningful depth is 64, matching <see cref="System.Text.Json.Utf8JsonReader"/>'s default limit.
/// </summary>
/// <example>
/// <code>
/// // All elements of the root array:
/// NdJsonPath.Each()
///
/// // Property on each array element:
/// NdJsonPath.Each().Key("name")
///
/// // Deeply nested:
/// NdJsonPath.At("users").Each().Key("address").Key("city")
/// </code>
/// </example>
public sealed class NdJsonPath
{
    /// <summary>Sentinel value for an array wildcard segment (<see cref="Builder.Each"/>).</summary>
    public static readonly byte[] Wildcard = [];

    /// <summary>Pre-encoded UTF-8 path segments. Empty array = wildcard.</summary>
    public readonly byte[][] Segments;

    private NdJsonPath(byte[][] segments) => Segments = segments;

    /// <summary>Start a path from the root by entering a named object property.</summary>
    public static Builder At(string key) => new Builder().Key(key);

    /// <summary>Start a path from the root by matching every element of a root-level array.</summary>
    public static Builder Each() => new Builder().Each();

    // ── Builder ───────────────────────────────────────────────────────────────

    /// <summary>Fluent builder for <see cref="NdJsonPath"/>.</summary>
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

        /// <summary>Build the immutable path.</summary>
        public NdJsonPath Build() => new([.. _segments]);

        /// <summary>Implicit conversion — allows passing a builder directly where an <see cref="NdJsonPath"/> is expected.</summary>
        public static implicit operator NdJsonPath(Builder b) => b.Build();
    }
}
