using System.Text;

namespace JsonStreaming;

/// <summary>
///     Root-anchored, compile-time encoded JSON path pattern.
///     Segments: non-empty byte array = UTF-8 property name to match,
///     empty byte array (<see cref="Wildcard" />) = array wildcard (any index).
///     Max meaningful depth is 64, matching <see cref="System.Text.Json.Utf8JsonReader" />'s default limit.
/// </summary>
/// <example>
///     <code>
/// // All elements of the root array:
/// NdJsonPath.Each()
/// 
/// // Property on each array element:
/// NdJsonPath.Each().Key("name")
/// 
/// // Deeply nested:
/// NdJsonPath.At("users").Each().Key("address").Key("city")
/// 
/// // From JSONPath string:
/// NdJsonPath.Parse("$.users[*].address.city")
/// </code>
/// </example>
public sealed class JsonPath
{
    /// <summary>Sentinel value for an array wildcard segment (<see cref="Builder.Each" />).</summary>
    public static readonly byte[] Wildcard = [];

    /// <summary>Pre-encoded UTF-8 path segments. Empty array = wildcard.</summary>
    public readonly byte[][] Segments;

    private JsonPath(byte[][] segments)
    {
        Segments = segments;
    }

    /// <summary>Empty path — targets the root.</summary>
    public static JsonPath Root { get; } = new([]);

    /// <summary>Start a path from the root by entering a named object property.</summary>
    public static Builder At(string key)
    {
        return new Builder().Key(key);
    }

    /// <summary>Start a path from the root by matching every element of a root-level array.</summary>
    public static Builder Each()
    {
        return new Builder().Each();
    }

    /// <summary>
    ///     Parse a JSONPath string into an <see cref="JsonPath" />.
    ///     Supported subset: <c>$</c>, <c>.property</c>, <c>[*]</c>.
    /// </summary>
    public static JsonPath Parse(ReadOnlySpan<char> jsonPath)
    {
        if (jsonPath.IsEmpty)
            return Root;

        var segments = new List<byte[]>();
        var i = 0;

        if (i < jsonPath.Length && jsonPath[i] == '$')
            i++;

        while (i < jsonPath.Length)
            if (jsonPath[i] == '.')
            {
                i++;
                var start = i;
                while (i < jsonPath.Length && jsonPath[i] != '.' && jsonPath[i] != '[')
                    i++;
                if (i > start)
                    segments.Add(Encoding.UTF8.GetBytes(jsonPath[start..i].ToString()));
            }
            else if (i + 2 < jsonPath.Length && jsonPath[i] == '[' && jsonPath[i + 1] == '*' && jsonPath[i + 2] == ']')
            {
                segments.Add(Wildcard);
                i += 3;
            }
            else
            {
                i++;
            }

        return new JsonPath([.. segments]);
    }

    /// <summary>
    ///     Converts this path to a JSONPath string (e.g. <c>$.response[*].messages</c>).
    /// </summary>
    public string ToJsonPath()
    {
        if (Segments.Length == 0)
            return "$";

        var sb = new StringBuilder("$");
        foreach (var seg in Segments)
            if (seg.Length == 0)
                sb.Append("[*]");
            else
                sb.Append('.').Append(Encoding.UTF8.GetString(seg));
        return sb.ToString();
    }

    /// <inheritdoc />
    public override string ToString()
    {
        return ToJsonPath();
    }

    // ── Builder ───────────────────────────────────────────────────────────────

    /// <summary>Fluent builder for <see cref="JsonPath" />.</summary>
    public sealed class Builder
    {
        private readonly List<byte[]> _segments = [];

        /// <summary>Descend into a named object property.</summary>
        public Builder Key(string name)
        {
            _segments.Add(Encoding.UTF8.GetBytes(name));
            return this;
        }

        /// <summary>Descend into a named object property from UTF-8 bytes.</summary>
        public Builder Property(ReadOnlySpan<byte> utf8Name)
        {
            _segments.Add(utf8Name.ToArray());
            return this;
        }

        /// <summary>Descend into a named object property (alias for Key).</summary>
        public Builder Property(string name)
        {
            return Key(name);
        }

        /// <summary>Descend into every element of an array (wildcard index).</summary>
        public Builder Each()
        {
            _segments.Add(Wildcard);
            return this;
        }

        /// <summary>Build the immutable path.</summary>
        public JsonPath Build()
        {
            return new JsonPath([.. _segments]);
        }

        /// <summary>Implicit conversion — allows passing a builder directly where an <see cref="JsonPath" /> is expected.</summary>
        public static implicit operator JsonPath(Builder b)
        {
            return b.Build();
        }
    }
}