using FluentAssertions;
using JsonStreaming;

namespace JsonStreaming.Tests;

public class JsonPathTests
{
    [Fact]
    public void Root_HasNoSegments()
    {
        JsonPath.Root.Segments.Length.Should().Be(0);
    }

    [Fact]
    public void Property_AddsSegment()
    {
        var path = JsonPath.Root.Property("messages"u8);

        path.Segments.Length.Should().Be(1);
        path.Segments[0].Kind.Should().Be(SegmentKind.Property);
    }

    [Fact]
    public void Array_AddsEnterArraySegment()
    {
        var path = JsonPath.Root.Array("items"u8);

        path.Segments.Length.Should().Be(1);
        path.Segments[0].Kind.Should().Be(SegmentKind.EnterArray);
    }

    [Fact]
    public void Each_AddsEachSegment()
    {
        var path = JsonPath.Root.Each();

        path.Segments.Length.Should().Be(1);
        path.Segments[0].Kind.Should().Be(SegmentKind.Each);
    }

    [Fact]
    public void Chained_BuildsCorrectPath()
    {
        var path = JsonPath.Root.Property("response"u8).Each().Property("messages"u8);

        path.Segments.Length.Should().Be(3);
        path.Segments[0].Kind.Should().Be(SegmentKind.Property);
        path.Segments[1].Kind.Should().Be(SegmentKind.Each);
        path.Segments[2].Kind.Should().Be(SegmentKind.Property);
    }

    [Fact]
    public void ToJsonPath_Root_ReturnsDollar()
    {
        JsonPath.Root.ToJsonPath().Should().Be("$");
    }

    [Fact]
    public void ToJsonPath_SingleProperty()
    {
        var path = JsonPath.Root.Property("messages"u8);
        path.ToJsonPath().Should().Be("$.messages");
    }

    [Fact]
    public void ToJsonPath_NestedWithEach()
    {
        var path = JsonPath.Root.Property("response"u8).Each().Property("messages"u8);
        path.ToJsonPath().Should().Be("$.response[*].messages");
    }

    [Fact]
    public void Parse_Empty_ReturnsRoot()
    {
        var path = JsonPath.Parse("");
        path.Segments.Length.Should().Be(0);
    }

    [Fact]
    public void Parse_DollarOnly_ReturnsRoot()
    {
        var path = JsonPath.Parse("$");
        path.Segments.Length.Should().Be(0);
    }

    [Fact]
    public void Parse_SimpleProperty()
    {
        var path = JsonPath.Parse("$.messages");

        path.Segments.Length.Should().Be(1);
        path.Segments[0].Kind.Should().Be(SegmentKind.Property);
    }

    [Fact]
    public void Parse_NestedPath()
    {
        var path = JsonPath.Parse("$.response.data.items");

        path.Segments.Length.Should().Be(3);
    }

    [Fact]
    public void Parse_WithWildcard()
    {
        var path = JsonPath.Parse("$.response[*].messages");

        path.Segments.Length.Should().Be(3);
        path.Segments[0].Kind.Should().Be(SegmentKind.Property);
        path.Segments[1].Kind.Should().Be(SegmentKind.Each);
        path.Segments[2].Kind.Should().Be(SegmentKind.Property);
    }

    [Fact]
    public void Roundtrip_BuilderToJsonPathAndBack()
    {
        var original = JsonPath.Root.Property("response"u8).Each().Property("items"u8);
        var jsonPathStr = original.ToJsonPath();
        var parsed = JsonPath.Parse(jsonPathStr);

        parsed.ToJsonPath().Should().Be(jsonPathStr);
    }

    [Fact]
    public void ToString_MatchesToJsonPath()
    {
        var path = JsonPath.Root.Property("data"u8);
        path.ToString().Should().Be(path.ToJsonPath());
    }

    [Fact]
    public void Immutable_OriginalUnchanged()
    {
        var root = JsonPath.Root;
        var withProp = root.Property("x"u8);

        root.Segments.Length.Should().Be(0);
        withProp.Segments.Length.Should().Be(1);
    }
}
