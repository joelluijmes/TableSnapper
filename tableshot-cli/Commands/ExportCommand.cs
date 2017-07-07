using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;
using tableshot.Models;

namespace tableshot.Commands
{
    internal sealed class ExportCommand : DatabaseCommand
    {
        private CommandOption _directoryOption;
        private CommandOption _outputOption;
        private CommandOption _skipDataOption;

        public override string Name => "export";
        public override string Description => "Export table to file";

        public override void Configure(CommandLineApplication application)
        {
            _outputOption = application.Option("-o|--output", "single output file of export", CommandOptionType.SingleValue);
            _directoryOption = application.Option("-d|--directory", "directory for splitted output", CommandOptionType.SingleValue);
            _skipDataOption = application.Option("-s|--structure", "skip data", CommandOptionType.NoValue);
        }

        protected override async Task Execute(DatabaseManager databaseManager)
        {
            foreach (var table in Program.Configuration.Tables)
            {
                var shallowTables = table.ReferencedBy != ReferencedByOptions.Disabled
                    ? await databaseManager.ListTablesReferencedByAsync(table.Table, table.ReferencedBy)
                    : new[] { table.Table } as IList<ShallowTable>;

                var tables = await Task.WhenAll(shallowTables.Select(databaseManager.QueryTableAsync));

                var outputFile = _outputOption.Value();
                var outputDirectory = _directoryOption.Value();
                var skipData = _skipDataOption.HasValue();

                var cloneManager = new DatabaseCloneManager(databaseManager);
                if (!string.IsNullOrEmpty(outputFile))
                    await cloneManager.BackupToFileAsync(outputFile, tables, skipData);
                if (!string.IsNullOrEmpty(outputDirectory))
                    await cloneManager.BackupToDirectoryAsync(outputDirectory, tables, true, skipData);
            }
        }
    }
}
