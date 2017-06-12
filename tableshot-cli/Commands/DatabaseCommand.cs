using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;
using tableshot.Models;

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
            var connectionBuilder = Program.ConfigurationJson["source"].ToObject<ServerCredentials>().ToConnectionStringBuilder();
            using (Connection = await DatabaseConnection.CreateConnectionAsync(connectionBuilder))
            {
                var schema = Program.ConfigurationJson.Value<string>("schema") ?? await DatabaseManager.GetDefaultSchema(Connection);

                var manager = new DatabaseManager(Connection, schema);
                await Execute(manager);
            }
        }

        protected abstract Task Execute(DatabaseManager databaseManager);
    }
}
