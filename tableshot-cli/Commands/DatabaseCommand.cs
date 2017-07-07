using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;
using tableshot.Models;

namespace tableshot.Commands
{
    internal abstract class DatabaseCommand : ICommand
    {
        protected DatabaseConnection Connection;
        
        protected SchemaScope Scope { get; private set; }
        public abstract string Name { get; }
        public abstract string Description { get; }
        public abstract void Configure(CommandLineApplication application);

        public async Task Execute()
        {
            Scope = new SchemaScope(Program.Configuration.Schemas);
            var source = Program.Configuration.SourceCredentials.ToConnectionStringBuilder();

            using (Connection = await DatabaseConnection.CreateConnectionAsync(source))
            {
                var manager = new DatabaseManager(Connection);

                var tableConfigurations = new List<TableConfiguration>();
                foreach (var tableConfiguration in Program.Configuration.TableConfigurations)
                {
                    if (tableConfiguration.Table.SchemaName != "*")
                    {
                        tableConfigurations.Add(tableConfiguration);
                        continue;
                    }

                    var tables = await manager.ListShallowTablesAsync(tableConfiguration.Table.Name, Scope);
                    tableConfigurations.AddRange(tables.Select(table => new TableConfiguration
                    {
                        Table = table,
                        ReferencedBy = tableConfiguration.ReferencedBy
                    }));
                }

                Program.Configuration.TableConfigurations = tableConfigurations
                    .OrderBy(t => t.Table.SchemaName)
                    .ThenBy(t => t.Table.Name)
                    .ToList();

                await Execute(manager);
            }
        }

        protected abstract Task Execute(DatabaseManager databaseManager);
    }
}
