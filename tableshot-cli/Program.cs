using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx;
using tableshot.Models;

namespace tableshot
{
    internal class Program
    {
        private static IConfigurationRoot _configurationRoot;
        private static ILogger _logger;
        
        private static void Main(string[] args)
        {
            // Get the configuration
            var configuration = new ConfigurationBuilder();
            configuration.SetBasePath(Directory.GetCurrentDirectory());
            configuration.AddJsonFile("settings.json");
            _configurationRoot = configuration.Build();
            
            // Add logging
            ApplicationLogging.LoggerFactory.AddConsole(LogLevel.Debug);
            ApplicationLogging.LoggerFactory.AddDebug(LogLevel.Trace);
            _logger = ApplicationLogging.CreateLogger<Program>();

            //AsyncContext.Run(() => MainImpl(args));
            MainImpl(args);
        }

        private static Task MainImpl(string[] args)
        {
            _logger.LogDebug("application started");

            var commandApplication = new CommandLineApplication()
            {
                Name = "tableshot"
            };

            commandApplication.HelpOption("-h|--help");

            // commands
            commandApplication.Command("resolve", command =>
            {
                command.Description = "Resolve all dependent tables";
                command.HelpOption("-h|--help");
                var tableArgument = command.Argument("table", "table to resolve");

                command.OnExecute(() => ResolveTable(tableArgument.Value));
            });

            commandApplication.Command("export", command =>
            {
                command.Description = "Export specific table to file";
                command.HelpOption("-h|--help");
                var tableArgument = command.Argument("table", "table to export");
                var referenceOption = command.Option("-r|--referenced", "export dependent tables", CommandOptionType.NoValue);
                var outputOption = command.Option("-o|--output", "single output file of export", CommandOptionType.SingleValue);
                var directoryOption = command.Option("-d|--directory", "directory for splitted output", CommandOptionType.SingleValue);
                var skipDataOption = command.Option("-s|--structure", "skip data", CommandOptionType.NoValue);

                command.OnExecute(() => ExportTable(tableArgument.Value, referenceOption.HasValue(), outputOption.Value(), directoryOption.Value(), skipDataOption.HasValue()));
            });
            
            commandApplication.Execute(args);

            _logger.LogDebug("application completed");

            return Task.CompletedTask;
        }

        private static async Task<int> ExportTable(string table, bool referenced, string outputFile, string outputDirectory, bool skipData)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            var connection = await DatabaseConnection.CreateConnectionAsync(_configurationRoot["server"], _configurationRoot["database"]);

            ShallowTable shallowTable;
            var splitted = table.Split('.');
            switch (splitted.Length)
            {
            case 0:
                var schema = _configurationRoot["schema"] ?? await DatabaseManager.GetDefaultSchema(connection);
                shallowTable = new ShallowTable(schema, table);
                break;
            case 2:
                shallowTable = new ShallowTable(splitted[0], splitted[1]);
                break;
            default:
                throw new InvalidOperationException();
            }

            var manager = new DatabaseManager(connection);
            var shallowTables = referenced
                ? await manager.QueryTablesReferencedByAsync(shallowTable)
                : new[] {shallowTable} as IList<ShallowTable>;

            var tables = await Task.WhenAll(shallowTables.Select(manager.QueryTableAsync));
            if (!string.IsNullOrEmpty(outputFile))
                await manager.BackupToFileAsync(outputFile, tables, skipData);
            if (!string.IsNullOrEmpty(outputDirectory))
                await manager.BackupToDirectoryAsync(outputDirectory, tables, true, skipData);

            return 0;
        }
        
        private static async Task<int> ResolveTable(string table)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            var connection = await DatabaseConnection.CreateConnectionAsync(_configurationRoot["server"], _configurationRoot["database"]);

            ShallowTable shallowTable;
            var splitted = table.Split('.');
            switch (splitted.Length) {
            case 0:
                var schema = _configurationRoot["schema"] ?? await DatabaseManager.GetDefaultSchema(connection);
                shallowTable = new ShallowTable(schema, table);
                break;
            case 2:
                shallowTable = new ShallowTable(splitted[0], splitted[1]);
                break;
            default:
                throw new InvalidOperationException();
            }
            
            var manager = new DatabaseManager(connection);
            var referencedTables = await manager.QueryTablesReferencedByAsync(shallowTable);

            _logger.LogInformation("All tables depending on (in order):");
            _logger.LogInformation(referencedTables.Aggregate($"{shallowTable}", (a,b) => $"{a}\r\n {b}"));

            return 0;
        }
    }
}
