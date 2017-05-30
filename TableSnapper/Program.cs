using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx;

namespace TableSnapper
{
    internal class Program
    {
        public static ILoggerFactory LoggerFactory { get; } = new LoggerFactory();
        public static ILogger CreateLogger<T>() => LoggerFactory.CreateLogger<T>();

        private static readonly ILogger _logger = CreateLogger<Program>();

        private static void Main(string[] args)
        {
            LoggerFactory.AddConsole(LogLevel.Debug, true);
            
            AsyncContext.Run(MainImpl);
        }

        private static async Task MainImpl()
        {
            _logger.LogInformation("Started");

            var repoA = await Repository.OpenDatabaseAsync("localhost", "TestA");
            var repoB = await Repository.OpenDatabaseAsync("localhost", "TestB");

            var tablesA = (await repoA.ListTablesAsync()).ToArray();
            _logger.LogTrace($"{tablesA.Length} tables");

            var directory = Path.Combine(Environment.GetEnvironmentVariable("LocalAppData"), "TableSnapper");
            Directory.CreateDirectory(directory);
            _logger.LogTrace($"output path '{directory}'");

            foreach (var file in new DirectoryInfo(directory).GetFiles())
                file.Delete();

            for (var i = 0; i < tablesA.Length; i++)
            {
                var table = tablesA[i];
                var content = await repoA.CloneTableSqlAsync(table);

                var path = Path.Combine(directory, $"{i}_{table.Name}.sql");
                File.WriteAllText(path, content);
            }

            _logger.LogInformation("Completed");
        }
    }
}
