using System.Text;

namespace JsonStreaming;

/// <summary>
/// Describes how a segment navigates the JSON tree.
/// </summary>
public enum SegmentKind : byte
{
    /// <summary>Match a property name and descend into its value.</summary>
    Property,

    /// <summary>Match a property name and enter its value as an array.</summary>
    EnterArray,

    /// <summary>Iterate each element of the current array, re-entering for each.</summary>
    Each,
}

/// <summary>
/// A single navigation step in a <see cref="JsonPath"/>.
/// </summary>
public readonly struct Segment
{
    public SegmentKind Kind { get; }

    /// <summary>
    /// UTF-8 property name to match. Empty for <see cref="SegmentKind.Each"/>.
    /// </summary>
    public ReadOnlyMemory<byte> Name { get; }

    internal Segment(SegmentKind kind, ReadOnlyMemory<byte> name = default)
    {
        Kind = kind;
        Name = name;
    }
}

/// <summary>
/// Zero-allocation path descriptor for navigating into nested JSON structures.
/// Built via a fluent API; translatable to/from JSONPath strings.
///
/// <code>
/// var path = JsonPath.Root.Property("response"u8).Each().Property("messages"u8);
/// // Equivalent JSONPath: $.response[*].messages
/// </code>
/// </summary>
public readonly struct JsonPath
{
    private readonly Segment[]? _segments;

    private JsonPath(Segment[]? segments) => _segments = segments;

    /// <summary>Empty path — targets the root array.</summary>
    public static JsonPath Root => default;

    /// <summary>The segments that define this path.</summary>
    public ReadOnlySpan<Segment> Segments => _segments.AsSpan();

    /// <summary>
    /// Navigate into a property. The state machine will match this property name
    /// and descend into its value (expected to be an object or array).
    /// </summary>
    public JsonPath Property(ReadOnlySpan<byte> utf8Name)
    {
        return Append(new Segment(SegmentKind.Property, utf8Name.ToArray()));
    }

    /// <summary>
    /// Navigate into a property and enter its value as the target array.
    /// Shorthand for the terminal segment — the array whose items are yielded.
    /// </summary>
    public JsonPath Array(ReadOnlySpan<byte> utf8Name)
    {
        return Append(new Segment(SegmentKind.EnterArray, utf8Name.ToArray()));
    }

    /// <summary>
    /// Iterate each element of the current array. For each element, the remaining
    /// path segments are applied. This enables select-many semantics.
    /// </summary>
    public JsonPath Each()
    {
        return Append(new Segment(SegmentKind.Each));
    }

    /// <summary>
    /// Parse a JSONPath string into a <see cref="JsonPath"/>.
    /// Supported subset: <c>$</c>, <c>.property</c>, <c>[*]</c>.
    /// </summary>
    public static JsonPath Parse(ReadOnlySpan<char> jsonPath)
    {
        if (jsonPath.IsEmpty)
            return Root;

        var segments = new List<Segment>();
        int i = 0;

        // Skip leading $
        if (i < jsonPath.Length && jsonPath[i] == '$')
            i++;

        while (i < jsonPath.Length)
        {
            if (jsonPath[i] == '.')
            {
                i++;
                // Read property name until next . or [ or end
                int start = i;
                while (i < jsonPath.Length && jsonPath[i] != '.' && jsonPath[i] != '[')
                    i++;
                if (i > start)
                {
                    var name = Encoding.UTF8.GetBytes(jsonPath[start..i].ToString());
                    segments.Add(new Segment(SegmentKind.Property, name));
                }
            }
            else if (i + 2 < jsonPath.Length && jsonPath[i] == '[' && jsonPath[i + 1] == '*' && jsonPath[i + 2] == ']')
            {
                segments.Add(new Segment(SegmentKind.Each));
                i += 3;
            }
            else
            {
                i++; // skip unknown
            }
        }

        return new JsonPath(segments.ToArray());
    }

    /// <summary>
    /// Converts this path to a JSONPath string (e.g. <c>$.response[*].messages</c>).
    /// </summary>
    public string ToJsonPath()
    {
        if (_segments is null or { Length: 0 })
            return "$";

        var sb = new StringBuilder("$");
        foreach (var seg in _segments)
        {
            switch (seg.Kind)
            {
                case SegmentKind.Property:
                case SegmentKind.EnterArray:
                    sb.Append('.').Append(Encoding.UTF8.GetString(seg.Name.Span));
                    break;
                case SegmentKind.Each:
                    sb.Append("[*]");
                    break;
            }
        }
        return sb.ToString();
    }

    public override string ToString() => ToJsonPath();

    private JsonPath Append(Segment segment)
    {
        var existing = _segments ?? [];
        var newArr = new Segment[existing.Length + 1];
        existing.CopyTo(newArr, 0);
        newArr[^1] = segment;
        return new JsonPath(newArr);
    }
}
