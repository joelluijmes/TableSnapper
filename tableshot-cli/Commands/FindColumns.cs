using System;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using tableshot.Models;

namespace tableshot.Commands
{
    internal sealed class FindColumns : ICommand
    {
        private static readonly ILogger _logger = ApplicationLogging.CreateLogger<ResolveCommand>();
        
        public string Name => "find";
        public string Description => "find tables where column names starts with";

        public void Configure(CommandLineApplication application)
        {
        }

        public async Task Execute()
        { 
            if (string.IsNullOrEmpty(Program.Configuration.Schema))
                throw new InvalidOperationException();

            // read the config
            var source = Program.Configuration.SourceCredentials.ToConnectionStringBuilder();
            var columnNames = Program.Configuration.Columns;

            // do the cloning
            using (var sourceConnection = await DatabaseConnection.CreateConnectionAsync(source))
            {
                var manager = new DatabaseManager(sourceConnection, Program.Configuration.Schema);

                var tables = await manager.QueryShallowTablesAsync(Program.Configuration.Schema);
                var tableColumns = await Task.WhenAll(
                    tables.Select(async table => new
                    {
                        table = table,
                        columns = await manager.QueryColumnsAsync(table)
                    }));

                foreach (var column in columnNames)
                {
                    var matches = tableColumns
                        .Select(tc => new
                        {
                            table = tc.table,
                            column = tc.columns.Where(c => CultureInfo.CurrentCulture.CompareInfo.IndexOf(c.Name, column, CompareOptions.IgnoreCase) >= 0).ToArray()
                        })
                        .Where(tc => tc.column.Any())
                        .ToList();

                    Console.WriteLine($" tables and their columns like '{column}': ");
                    Console.WriteLine(matches.SelectMany(tc => tc.column).Aggregate("", (a, b) => $"{a}\r\n  {b}"));
                }
            }
        }
    }
}
