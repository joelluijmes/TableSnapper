using System;
using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;
using tableshot.Models;

namespace tableshot.Commands
{
    internal abstract class TableCommand : ICommand
    {
        private DatabaseConnection _connection;

        public abstract string Name { get; }
        public abstract string Description { get; }
        public abstract void Configure(CommandLineApplication application);

        public async Task Execute()
        {
            using (_connection = await DatabaseConnection.CreateConnectionAsync(Program.Configuration["server"], Program.Configuration["database"]))
            {
                var schema = Program.Configuration["schema"] ?? await DatabaseManager.GetDefaultSchema(_connection);

                var manager = new DatabaseManager(_connection, schema);
                await Execute(manager);
            }
        }

        protected abstract Task Execute(DatabaseManager databaseManager);

        protected async Task<ShallowTable> ParseTable(string tableName)
        {
            ShallowTable shallowTable;
            var splitted = tableName.Split('.');
            switch (splitted.Length)
            {
            case 0:
                var schema = Program.Configuration["schema"] ?? await DatabaseManager.GetDefaultSchema(_connection);
                shallowTable = new ShallowTable(schema, tableName);
                break;
            case 2:
                shallowTable = new ShallowTable(splitted[0], splitted[1]);
                break;
            default:
                throw new InvalidOperationException();
            }

            return shallowTable;
        }
    }
}
