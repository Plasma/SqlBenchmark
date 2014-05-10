using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using log4net;

namespace SqlBenchmark
{
	public class BenchmarkService
	{
		const string TestTableName = "BenchmarkTable";
		const int TestTableRowCount = 1000000;
		readonly ILog _log = LogManager.GetLogger(typeof(BenchmarkService));
		readonly object _completedQueryCounterLock = new object();
		volatile bool _running;
		int _completedQueryCounter;
		int _timedOutQueryCounter;
		Thread _sampleThread;
		bool _timeSampling;
		string _reportFilename;

		public async Task<BenchmarkReport> BenchmarkAsync(string connectionString, int userCount, int queriesPerUser, string query, bool skipTestTable, bool timeSampling, bool writeReport)
		{
			// Options
			_timeSampling = timeSampling;
			if (writeReport) {
				_reportFilename = string.Format("BenchmarkReport-{0}.csv", DateTime.UtcNow.ToString("yyyy-MM-dd_hh_mm_ss"));
				_log.Info(string.Format("Writing progress report to: {0}", _reportFilename));
			} else {
				_reportFilename = null;
			}
 
			// Verify Schema
			if (!skipTestTable)
				await CreateSchemaAsync(connectionString);

			// Start Watchdog Thread
			_running = true;
			_sampleThread = new Thread(WatchdogThreadStart);
			_sampleThread.IsBackground = true;
			_sampleThread.Start();

			// Create User Tasks
			_completedQueryCounter = 0;
			var sw = Stopwatch.StartNew();
			var tasks = new List<Task<Stopwatch>>();
			for(var i = 0; i < userCount; i++) {
				var task = SimulateUserAsync(i + 1, connectionString, queriesPerUser, query);
				tasks.Add(task);
			}

			// Await
			await Task.WhenAll(tasks);
			_running = false;
			sw.Stop();

			// Stop Thread
			_sampleThread.Join();
			_sampleThread = null;

			// Calculate Stats
			var totalQueriesExecuted = userCount*queriesPerUser;
			var totalElapsedQueryRuntimeMilliseconds = tasks.Sum(x => x.Result.Elapsed.TotalMilliseconds);
			
			// Need to sum up the queries per second value for each user as they happened concurrently
			var averageQueriesPerSecond = 0d;
			foreach (var task in tasks) {
				var queriesPerSecondForTask = queriesPerUser / task.Result.Elapsed.TotalSeconds;
				averageQueriesPerSecond += queriesPerSecondForTask;
			}
			
			// Create Report
			var report = new BenchmarkReport();
			report.UserCount = userCount;
			report.QueriesPerUser = queriesPerUser;
			report.TotalQueriesExecuted = totalQueriesExecuted;
			report.OverallTimeTaken = sw.Elapsed;
			report.TotalQueryRuntime = TimeSpan.FromMilliseconds(totalElapsedQueryRuntimeMilliseconds);
			report.AverageQueriesPerSecond = averageQueriesPerSecond;

			return report;
		}

		void WatchdogThreadStart()
		{
			// Wrie Log Header
			if (_reportFilename != null)
				File.WriteAllText(_reportFilename, "UtcDateTime,Sample,QueriesPerSecond,TimeoutsPerSecond\n");

			var sampleNumber = 0;
			while (_running) {
				// Wait
				Thread.Sleep(1000);

				// Capture Stats
				int queryCount;
				int timeoutCount;
				lock (_completedQueryCounterLock) {
					queryCount = _completedQueryCounter;
					timeoutCount = _timedOutQueryCounter;
					_completedQueryCounter = 0;
					_timedOutQueryCounter = 0;
				}
				
				// Print
				if (_timeSampling) {
					if (timeoutCount > 0)
						Console.ForegroundColor = ConsoleColor.Yellow;

					_log.Info(string.Format("Queries Per Second: {0} | Timeouts Per Second: {1}", queryCount.ToString("N0"), timeoutCount.ToString("N0")));
					Console.ResetColor();
				}

				// Log
				if (_reportFilename != null)
					File.AppendAllText(_reportFilename, string.Format("{0},{1},{2},{3}\n", DateTime.UtcNow.ToString("yyyy-MM-dd hh:mm:ss"), ++sampleNumber, queryCount, timeoutCount));
			}
		}

		async Task CreateSchemaAsync(string connectionString)
		{
			const int ValueBatchSize = 100;
			const int StatementBatchSize = 5;

			// Connect to host
			_log.Debug("Verifying schema is in place for test");
			using (var conn = new SqlConnection(connectionString)) {
				// Open Connection
				await conn.OpenAsync();

				// Verify table exists
				var command = new SqlCommand(string.Format("SELECT Count(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = '{0}'", TestTableName), conn);
				var exists = Convert.ToInt32(await command.ExecuteScalarAsync());
				if (exists == 1) {
					// Verify row count
					command = new SqlCommand(string.Format("SELECT Count(*) FROM [{0}]", TestTableName), conn);
					var count = Convert.ToInt32(await command.ExecuteScalarAsync());
					if (count == TestTableRowCount)
						return;

					// Table exists; but row data mismatch - re-create table
					_log.Info("Test table existed but had a mismatch of data; dropping to re-create it");
					command = new SqlCommand(string.Format("DROP TABLE [{0}]", TestTableName), conn);
					await command.ExecuteNonQueryAsync();
				}

				// Create table
				_log.Info(string.Format("Creating database table '{0}' and filling with test data of {1} rows", TestTableName, TestTableRowCount.ToString("N0")));
				command = new SqlCommand(string.Format(@"CREATE TABLE [dbo].[{0}](
	[Id] [uniqueidentifier] NOT NULL,
	[Name] [nvarchar](255) NOT NULL,
 CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
))", TestTableName), conn);

				// Execute
				await command.ExecuteNonQueryAsync();

				// Fill with test data
				var values = new List<string>();
				var statements = new List<string>();
				for (var i = 0; i < TestTableRowCount; i++) {
					// Add to Values
					var value = string.Format("('{0}', 'Test User #{1}')", Guid.NewGuid(), i + 1);
					values.Add(value);

					// Build Query
					if (values.Count > 0 && (values.Count % ValueBatchSize == 0 || i == TestTableRowCount - 1)) {
						var statement = string.Format("INSERT INTO [{0}] (Id, Name) VALUES {1}", TestTableName, string.Join(", ", values));
						statements.Add(statement);
						values.Clear();
					}
				}

				// Execute Statements
				_log.Info("Committing test data to database...");
				var statementsToProcess = new List<string>();
				var processed = 0;
				var lastProgress = 0d;
				for (var i = 0; i < statements.Count; i++) {
					statementsToProcess.Add(statements[i]);
					processed++;

					if (statementsToProcess.Count %StatementBatchSize == 0 || i == statements.Count - 1) {
						var progress = (processed/(double)statements.Count) * 100;
						if (progress - lastProgress > 5) {
							_log.Info(string.Format("Executing INSERT batch ({0:N2}%)", progress));
							lastProgress = progress;
						}

						command = new SqlCommand(string.Join(";\n", statementsToProcess), conn);

						await command.ExecuteScalarAsync();

						statementsToProcess.Clear();
					}
				}
				_log.Info("Committed test data to database OK");
			}
		}

		async Task<Stopwatch> SimulateUserAsync(int userId, string connectionString, int queryCount, string queryTemplate)
		{
			var stopwatch = new Stopwatch();

			// Connect to host
			_log.Debug(string.Format("User {0} connecting to database server", userId));
			using (var conn = new SqlConnection(connectionString)) {
				// Open Connection
				await conn.OpenAsync();

				// Execute Queries
				for (var i = 0; i < queryCount; i++) {
					// Replace Variables
					var query = queryTemplate.Replace("%guid%", Guid.NewGuid().ToString());
					var command = new SqlCommand(query, conn);

					// Execute Tran under Stopwatch
					stopwatch.Start();
					try {
						await command.ExecuteNonQueryAsync();
					} catch (SqlException ex) {
						// Only Handle Timeout Exceptions
						if (!ex.Message.StartsWith("Timeout expired"))
							throw;

						// Do not record this as a valid transaction
						lock (_completedQueryCounterLock)
							_timedOutQueryCounter++;

						continue;
					}
					stopwatch.Stop();

					// Increment query counter
					lock (_completedQueryCounterLock)
						_completedQueryCounter++;
				}
			}

			_log.Debug(string.Format("User {0} has finished simulation", userId));
			return stopwatch;
		}
	}
}