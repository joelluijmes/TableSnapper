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
            var source = Program.Configuration.SourceCredentials.ToConnectionStringBuilder();
            using (Connection = await DatabaseConnection.CreateConnectionAsync(source))
            {
                var manager = new DatabaseManager(Connection);
                await Execute(manager);
            }
        }

        protected abstract Task Execute(DatabaseManager databaseManager);
    }
}
