using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;
using tableshot.Models;

namespace tableshot.Commands
{
    internal sealed class ExportCommand : TableCommand
    {
        private CommandOption _directoryOption;
        private CommandOption _outputOption;
        private CommandOption _referenceOption;
        private CommandOption _skipDataOption;
        private CommandArgument _tableArgument;

        public override string Name => "export";
        public override string Description => "Export table to file";

        public override void Configure(CommandLineApplication application)
        {
            _tableArgument = application.Argument("table", "table to export ([Schema].Table)");
            _referenceOption = application.Option("-r|--referenced", "export referenced tables", CommandOptionType.NoValue);
            _outputOption = application.Option("-o|--output", "single output file of export", CommandOptionType.SingleValue);
            _directoryOption = application.Option("-d|--directory", "directory for splitted output", CommandOptionType.SingleValue);
            _skipDataOption = application.Option("-s|--structure", "skip data", CommandOptionType.NoValue);
        }

        protected override async Task Execute(DatabaseManager databaseManager)
        {
            var table = await ParseTable(_tableArgument.Value);
            var shallowTables = _referenceOption.HasValue()
                ? await databaseManager.QueryTablesReferencedByAsync(table)
                : new[] {table} as IList<ShallowTable>;

            var tables = await Task.WhenAll(shallowTables.Select(databaseManager.QueryTableAsync));

            var outputFile = _outputOption.Value();
            var outputDirectory = _directoryOption.Value();
            var skipData = _skipDataOption.HasValue();

            if (!string.IsNullOrEmpty(outputFile))
                await databaseManager.BackupToFileAsync(outputFile, tables, skipData);
            if (!string.IsNullOrEmpty(outputDirectory))
                await databaseManager.BackupToDirectoryAsync(outputDirectory, tables, true, skipData);
        }
    }
}
