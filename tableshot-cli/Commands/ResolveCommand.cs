using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;
using tableshot.Models;

namespace tableshot.Commands
{
    internal sealed class ResolveCommand : DatabaseCommand
    {
        private static readonly ILogger _logger = ApplicationLogging.CreateLogger<ResolveCommand>();
        private CommandArgument _tableArgument;
        private CommandOption _schemaOnlyReferencedOption;

        public override string Name => "resolve";
        public override string Description => "Resolves (all) referenced tables";

        public override void Configure(CommandLineApplication application)
        {
            _tableArgument = application.Argument("table", "table to find (all) referenced tables ([Schema].Table)");
            _schemaOnlyReferencedOption = application.Option("--schema-only", "limit referenced tables by same schema only", CommandOptionType.NoValue);
        }

        protected override async Task Execute(DatabaseManager databaseManager)
        {
            var shallowTable = Util.ParseTableName(_tableArgument.Value);
            var referencedBy = _schemaOnlyReferencedOption.HasValue()
                ? ReferencedByOptions.SchemaOnly
                : ReferencedByOptions.FullDescend;

            var referencedTables = await databaseManager.QueryTablesReferencedByAsync(shallowTable, referencedBy);

            _logger.LogInformation("All referenced tables on (in order of dependency):");
            _logger.LogInformation(referencedTables.Aggregate($"{shallowTable}", (a, b) => $"{a}\r\n {b}"));
        }
    }
}
