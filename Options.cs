﻿using CommandLine;
using CommandLine.Text;

namespace SqlBenchmark
{
	public class Options
	{
		[Option('c', HelpText = "SQL Server connection string", Required = true)]
		public string ConnectionString { get; set; }

		[Option('q', HelpText = "Query to execute for each user. Optionally specify a filename to read a query from.", DefaultValue = "SELECT * FROM BenchmarkTable Where Id = '%guid%'")]
		public string Query { get; set; }

		[Option('u', HelpText = "How many users to simulate", DefaultValue = 10)]
		public int Users { get; set; }

		[Option('n', HelpText = "How many queries to execute per user", DefaultValue = 100000)]
		public int Queries { get; set; }

		[Option('i', HelpText = "Number of benchmark iterations to run ", DefaultValue = 1)]
		public int Iterations { get; set; }

		[Option('v', HelpText = "Use verbose logging")]
		public bool Verbose { get; set; }

		[Option('s', HelpText = "Skip creating the test database table")]
		public bool SkipTestTable { get; set; }

		[Option("no-sample", HelpText = "Suppress printing continuous queries per second sample report")]
		public bool NoSampling { get; set; }

		[Option("no-report", HelpText = "Suppress writing a report file containing the performance numbers")]
		public bool NoReport { get; set; }

		[HelpOption]
		public string GetUsage()
		{
			return HelpText.AutoBuild(this, current => HelpText.DefaultParsingErrorsHandler(this, current));
		}
	}
}