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
    internal sealed class CloneCommand : DatabaseCommand
    {
        private CommandOption _createMissing;
        private CommandOption _checkReferenced;

        public override string Name => "clone";
        public override string Description => "clones tables and data between schemas and databases";

        public override void Configure(CommandLineApplication application)
        {
            _createMissing = application.Option("--create-missing", "creates missing schemas (default false)", CommandOptionType.NoValue);
            _checkReferenced = application.Option("--check-referenced", "check referenced tables before droping them (default false)", CommandOptionType.NoValue);
        }

        protected override async Task Execute(DatabaseManager databaseManager)
        { 
            // read the config
            var target = Program.Configuration.TargetCredentials.ToConnectionStringBuilder();
            var tables = Program.Configuration.TableConfigurations;

            // do the cloning
            using (var targetConnection = await DatabaseConnection.CreateConnectionAsync(target))
            {
                var cloner = new DatabaseCloner(databaseManager.Connection, targetConnection);
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
