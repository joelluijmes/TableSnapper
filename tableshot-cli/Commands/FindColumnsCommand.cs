using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;

namespace tableshot.Commands
{
    internal sealed class FindColumnsCommand : DatabaseCommand
    {
        private static readonly ILogger _logger = ApplicationLogging.CreateLogger<ResolveCommand>();

        public override string Name => "find";
        public override string Description => "find tables where column names starts with";

        public override void Configure(CommandLineApplication application)
        {
        
        }

        protected override async Task Execute(DatabaseManager databaseManager)
        {
            var columnNames = Program.Configuration.Columns;

            var tables = await databaseManager.ListShallowTablesAsync(null, Scope);
            var tableColumns = await Task.WhenAll(
                tables.Select(async table => new
                {
                    table,
                    columns = await databaseManager.ListTableColumnsAsync(table)
                }));

            foreach (var column in columnNames)
            {
                var matches = tableColumns
                    .Select(tc => new
                    {
                        tc.table,
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
