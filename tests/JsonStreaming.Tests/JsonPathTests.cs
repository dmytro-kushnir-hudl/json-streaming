using System.Text;
using FluentAssertions;

namespace JsonStreaming.Tests;

public class JsonPathTests
{
    [Fact]
    public void Root_HasNoSegments()
    {
        JsonPath.Root.Segments.Length.Should().Be(0);
    }

    [Fact]
    public void At_CreatesPropertySegment()
    {
        JsonPath path = JsonPath.At("messages");
        path.Segments.Length.Should().Be(1);
        path.Segments[0].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("messages"));
    }

    [Fact]
    public void Each_CreatesWildcardSegment()
    {
        JsonPath path = JsonPath.Each();
        path.Segments.Length.Should().Be(1);
        path.Segments[0].Length.Should().Be(0);
    }

    [Fact]
    public void Chained_BuildsCorrectPath()
    {
        JsonPath path = JsonPath.At("response").Each().Key("messages");
        path.Segments.Length.Should().Be(3);
        path.Segments[0].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("response"));
        path.Segments[1].Length.Should().Be(0);
        path.Segments[2].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("messages"));
    }

    [Fact]
    public void Property_Utf8_Works()
    {
        JsonPath path = JsonPath.Each().Property("name"u8);
        path.Segments.Length.Should().Be(2);
        path.Segments[1].Should().BeEquivalentTo("name"u8.ToArray());
    }

    [Fact]
    public void ToJsonPath_Root_ReturnsDollar()
    {
        JsonPath.Root.ToJsonPath().Should().Be("$");
    }

    [Fact]
    public void ToJsonPath_SingleProperty()
    {
        JsonPath path = JsonPath.At("messages");
        path.ToJsonPath().Should().Be("$.messages");
    }

    [Fact]
    public void ToJsonPath_NestedWithEach()
    {
        JsonPath path = JsonPath.At("response").Each().Key("messages");
        path.ToJsonPath().Should().Be("$.response[*].messages");
    }

    [Fact]
    public void Parse_Empty_ReturnsRoot()
    {
        JsonPath.Parse("").Segments.Length.Should().Be(0);
    }

    [Fact]
    public void Parse_DollarOnly_ReturnsRoot()
    {
        JsonPath.Parse("$").Segments.Length.Should().Be(0);
    }

    [Fact]
    public void Parse_SimpleProperty()
    {
        var path = JsonPath.Parse("$.messages");
        path.Segments.Length.Should().Be(1);
        path.Segments[0].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("messages"));
    }

    [Fact]
    public void Parse_NestedPath()
    {
        JsonPath.Parse("$.response.data.items").Segments.Length.Should().Be(3);
    }

    [Fact]
    public void Parse_WithWildcard()
    {
        var path = JsonPath.Parse("$.response[*].messages");
        path.Segments.Length.Should().Be(3);
        path.Segments[0].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("response"));
        path.Segments[1].Length.Should().Be(0);
        path.Segments[2].Should().BeEquivalentTo(Encoding.UTF8.GetBytes("messages"));
    }

    [Fact]
    public void Roundtrip_BuilderToJsonPathAndBack()
    {
        JsonPath original = JsonPath.At("response").Each().Key("items");
        var jsonPathStr = original.ToJsonPath();
        var parsed = JsonPath.Parse(jsonPathStr);
        parsed.ToJsonPath().Should().Be(jsonPathStr);
    }

    [Fact]
    public void ToString_MatchesToJsonPath()
    {
        JsonPath path = JsonPath.At("data");
        path.ToString().Should().Be(path.ToJsonPath());
    }

    [Fact]
    public void Separate_Builders_AreIndependent()
    {
        JsonPath pathA = JsonPath.At("x");
        JsonPath pathB = JsonPath.At("x").Key("y");

        pathA.Segments.Length.Should().Be(1);
        pathB.Segments.Length.Should().Be(2);
    }
}
