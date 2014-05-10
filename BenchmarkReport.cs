using System;

namespace SqlBenchmark
{
	public class BenchmarkReport
	{
		public int UserCount { get; set; }
 		public int QueriesPerUser { get; set; }
		public int TotalQueriesExecuted { get; set; }

		public double AverageQueriesPerSecond { get; set; }
		public TimeSpan TotalQueryRuntime { get; set; }
		public TimeSpan OverallTimeTaken { get; set; }
	}
}