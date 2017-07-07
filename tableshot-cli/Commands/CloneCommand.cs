using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using tableshot.Models;

namespace tableshot.Commands
{
    internal sealed class CloneCommand : ICommand
    {
        private CommandOption _createMissing;
        private CommandOption _checkReferenced;

        public string Name => "clone";
        public string Description => "clones tables and data between schemas and databases";

        public void Configure(CommandLineApplication application)
        {
            _createMissing = application.Option("--create-missing", "creates missing schemas (default false)", CommandOptionType.NoValue);
            _checkReferenced = application.Option("--check-referenced", "check referenced tables before droping them (default false)", CommandOptionType.NoValue);
        }

        public async Task Execute()
        { 
            // read the config
            var source = Program.Configuration.SourceCredentials.ToConnectionStringBuilder();
            var target = Program.Configuration.TargetCredentials.ToConnectionStringBuilder();
            var tables = Program.Configuration.TableConfigurations;

            // do the cloning
            using (var sourceConnection = await DatabaseConnection.CreateConnectionAsync(source))
            using (var targetConnection = await DatabaseConnection.CreateConnectionAsync(target))
            {
                var cloner = new DatabaseCloner(sourceConnection, targetConnection);
                var options = new DatabaseCloner.DatabaseCloneOptions(tables)
                {
                    CreateMissingSchemas = _createMissing.HasValue(),
                    CheckReferencedTables = _checkReferenced.HasValue()
                };

                await cloner.CloneAsync(options);
            }
        }
    }
}
