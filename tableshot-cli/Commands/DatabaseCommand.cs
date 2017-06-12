using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;

namespace tableshot.Commands
{
    internal abstract class DatabaseCommand : ICommand
    {
        protected DatabaseConnection Connection;

        public abstract string Name { get; }
        public abstract string Description { get; }
        public abstract void Configure(CommandLineApplication application);

        public async Task Execute()
        {
            using (Connection = await DatabaseConnection.CreateConnectionAsync(Program.Configuration["server"], Program.Configuration["database"]))
            {
                var schema = Program.Configuration["schema"] ?? await DatabaseManager.GetDefaultSchema(Connection);

                var manager = new DatabaseManager(Connection, schema);
                await Execute(manager);
            }
        }

        protected abstract Task Execute(DatabaseManager databaseManager);
    }
}
