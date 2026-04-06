using BenchmarkDotNet.Running;
using JsonStreaming.Benchmarks;

BenchmarkSwitcher.FromAssembly(typeof(InProcessConfig).Assembly).Run(args);
