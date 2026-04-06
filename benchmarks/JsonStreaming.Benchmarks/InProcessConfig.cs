using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace JsonStreaming.Benchmarks;

/// <summary>
/// Fast in-process benchmarks — no child process spawn, ~5s per benchmark.
/// </summary>
public class InProcessConfig : ManualConfig
{
    public InProcessConfig()
    {
        AddJob(
            Job.ShortRun
                .WithToolchain(InProcessEmitToolchain.Instance)
                .WithWarmupCount(3)
                .WithIterationCount(5)
        );
        AddColumn(StatisticColumn.OperationsPerSecond);
    }
}
