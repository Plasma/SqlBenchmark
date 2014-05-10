using System.IO;
using System.Threading.Tasks;
using log4net;
using log4net.Core;
using log4net.Filter;
using log4net.Layout;

namespace SqlBenchmark
{
	class Program
	{
		static readonly ILog _logger = LogManager.GetLogger(typeof(Program));

		static async Task Run(Options options)
		{
			_logger.Info("Starting Up");

			// Create Benchmark
			var benchmarkService = new BenchmarkService();

			// Load Query
			var queryOrFile = options.Query;
			if (File.Exists(queryOrFile)) {
				_logger.Info(string.Format("Loaded query from file: {0}", queryOrFile));
				queryOrFile = File.ReadAllText(queryOrFile);
			}

			// Execute Report
			for (var i = 0; i < options.Iterations; i++) {
				if (i == 0)
					_logger.Info("Starting Benchmark");

				// Run Benchmark
				var report = await benchmarkService.BenchmarkAsync(options.ConnectionString, options.Users, options.Queries, queryOrFile, options.SkipTestTable);

				// Display Report
				_logger.Info(string.Format("[{3}/{4}] Benchmark Completed [Runtime: {0}ms | Average queries/sec: {1} | Users: {2} | Per-user Queries: {5}]", report.TotalQueryRuntime.TotalMilliseconds.ToString("N0"), report.AverageQueriesPerSecond.ToString("N0"), report.UserCount.ToString("N0"), i + 1, options.Iterations, options.Queries.ToString("N0")));
			}

			_logger.Info("Finished Benchmarking");
		}

		static int Main(string[] args)
		{
			// Parse Options
			var options = new Options();
			if (!CommandLine.Parser.Default.ParseArguments(args, options))
				return 1;

			// Configure Logging
			ConfigureLogging(options.Verbose);

			// Execute
			var program = new Program();
			var task = Run(options);

			// Wait
			task.Wait();
			return 0;
		}

		static void ConfigureLogging(bool verbose)
		{
			var consoleAppender = new log4net.Appender.ConsoleAppender();
			consoleAppender.Layout = new PatternLayout("[%date{yyyy-MM-dd HH:mm:ss}] %-5p %c{1} - %m%n");
			if (!verbose)
				consoleAppender.AddFilter(new LevelMatchFilter{AcceptOnMatch = false, LevelToMatch = Level.Debug});
			log4net.Config.BasicConfigurator.Configure(consoleAppender);
			
		}
	}
}
