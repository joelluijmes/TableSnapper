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
        public string Name => "clone";
        public string Description => "clones tables and data between schemas and databases";

        public void Configure(CommandLineApplication application)
        {
        }

        public async Task Execute()
        { 
            // read the config
            var source = Program.Configuration.SourceCredentials.ToConnectionStringBuilder();
            var target = Program.Configuration.TargetCredentials.ToConnectionStringBuilder();
            var tables = Program.Configuration.Tables;

            // do the cloning
            using (var sourceConnection = await DatabaseConnection.CreateConnectionAsync(source))
            using (var targetConnection = await DatabaseConnection.CreateConnectionAsync(target))
            {
                var cloner = new DatabaseCloner(sourceConnection, targetConnection);
                var options = new DatabaseCloner.DatabaseCloneOptions(tables);

                await cloner.CloneAsync(options);
            }
        }
    }
}
