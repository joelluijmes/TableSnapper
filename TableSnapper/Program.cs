using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx;
using TableSnapper.Models;

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

            var connectionA = await DatabaseConnection.CreateConnectionAsync("localhost", "TestA");
            var databaseA = new DatabaseManager(connectionA);

            var connectionB = await DatabaseConnection.CreateConnectionAsync("localhost", "TestB");
            var databaseB = new DatabaseManager(connectionB);

            await databaseB.CloneFromAsync(databaseA);

            //var directory = Path.Combine(Environment.GetEnvironmentVariable("LocalAppData"), "TableSnapper");
            //Directory.CreateDirectory(directory);
            //_logger.LogTrace($"output path '{directory}'");

            //foreach (var file in new DirectoryInfo(directory).GetFiles())
            //    file.Delete();

            //var dict = new Dictionary<Table, string>();
            //for (var i = 0; i < tablesA.Length; i++)
            //{
            //    var table = tablesA[i];
            //    var content = await databaseA.CloneTableSqlAsync(table);
            //    dict[table] = content;

            //    var path = Path.Combine(directory, $"{i}_{table.Name}.sql");
            //    File.WriteAllText(path, content);
            //}

            //for (var i = tablesA.Length - 1; i >= 0; --i)
            //    await databaseB.DropTableAsync(tablesA[i].Name);

            //foreach (var table in tablesA)
            //    await connectionB.ExecuteNonQueryAsync(dict[table]);

            _logger.LogInformation("Completed");
        }
    }
}
