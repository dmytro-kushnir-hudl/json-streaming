using System.Text;
using FluentAssertions;
using JsonStreaming;

namespace JsonStreaming.Tests;

public class JsonPathTests
{
    [Fact]
    public void Root_HasNoSegments()
    {
        NdJsonPath.Root.Segments.Length.Should().Be(0);
    }

    [Fact]
    public void At_CreatesPropertySegment()
    {
        NdJsonPath path = NdJsonPath.At("messages");
        path.Segments.Length.Should().Be(1);
        path.Segments[0].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("messages"));
    }

    [Fact]
    public void Each_CreatesWildcardSegment()
    {
        NdJsonPath path = NdJsonPath.Each();
        path.Segments.Length.Should().Be(1);
        path.Segments[0].Length.Should().Be(0);
    }

    [Fact]
    public void Chained_BuildsCorrectPath()
    {
        NdJsonPath path = NdJsonPath.At("response").Each().Key("messages");
        path.Segments.Length.Should().Be(3);
        path.Segments[0].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("response"));
        path.Segments[1].Length.Should().Be(0);
        path.Segments[2].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("messages"));
    }

    [Fact]
    public void Property_Utf8_Works()
    {
        NdJsonPath path = NdJsonPath.Each().Property("name"u8);
        path.Segments.Length.Should().Be(2);
        path.Segments[1].Should().BeEquivalentTo("name"u8.ToArray());
    }

    [Fact]
    public void ToJsonPath_Root_ReturnsDollar()
    {
        NdJsonPath.Root.ToJsonPath().Should().Be("$");
    }

    [Fact]
    public void ToJsonPath_SingleProperty()
    {
        NdJsonPath path = NdJsonPath.At("messages");
        path.ToJsonPath().Should().Be("$.messages");
    }

    [Fact]
    public void ToJsonPath_NestedWithEach()
    {
        NdJsonPath path = NdJsonPath.At("response").Each().Key("messages");
        path.ToJsonPath().Should().Be("$.response[*].messages");
    }

    [Fact]
    public void Parse_Empty_ReturnsRoot()
    {
        NdJsonPath.Parse("").Segments.Length.Should().Be(0);
    }

    [Fact]
    public void Parse_DollarOnly_ReturnsRoot()
    {
        NdJsonPath.Parse("$").Segments.Length.Should().Be(0);
    }

    [Fact]
    public void Parse_SimpleProperty()
    {
        var path = NdJsonPath.Parse("$.messages");
        path.Segments.Length.Should().Be(1);
        path.Segments[0].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("messages"));
    }

    [Fact]
    public void Parse_NestedPath()
    {
        NdJsonPath.Parse("$.response.data.items").Segments.Length.Should().Be(3);
    }

    [Fact]
    public void Parse_WithWildcard()
    {
        var path = NdJsonPath.Parse("$.response[*].messages");
        path.Segments.Length.Should().Be(3);
        path.Segments[0].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("response"));
        path.Segments[1].Length.Should().Be(0);
        path.Segments[2].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("messages"));
    }

    [Fact]
    public void Roundtrip_BuilderToJsonPathAndBack()
    {
        NdJsonPath original = NdJsonPath.At("response").Each().Key("items");
        var jsonPathStr = original.ToJsonPath();
        var parsed = NdJsonPath.Parse(jsonPathStr);
        parsed.ToJsonPath().Should().Be(jsonPathStr);
    }

    [Fact]
    public void ToString_MatchesToJsonPath()
    {
        NdJsonPath path = NdJsonPath.At("data");
        path.ToString().Should().Be(path.ToJsonPath());
    }

    [Fact]
    public void Separate_Builders_AreIndependent()
    {
        NdJsonPath pathA = NdJsonPath.At("x");
        NdJsonPath pathB = NdJsonPath.At("x").Key("y");

        pathA.Segments.Length.Should().Be(1);
        pathB.Segments.Length.Should().Be(2);
    }
}
