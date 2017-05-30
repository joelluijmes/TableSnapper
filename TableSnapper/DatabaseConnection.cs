using System;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace TableSnapper
{
    internal sealed class DatabaseConnection : IDisposable
    {
        private static readonly ILogger _logger = Program.CreateLogger<DatabaseConnection>();

        private readonly string _database;
        private readonly string _server;
        private readonly SqlConnection _sqlConnection;

        private bool _disposed;

        public DatabaseConnection(DbConnectionStringBuilder sqlBuilder)
        {
            _server = sqlBuilder["Server"]?.ToString();
            _database = sqlBuilder["Database"]?.ToString();

            sqlBuilder["MultipleActiveResultSets"] = true;

            var connectionString = sqlBuilder.ConnectionString;
            _sqlConnection = new SqlConnection(connectionString);
            _logger.LogInformation($"connectionstring: {connectionString}");
        }

        public DatabaseConnection(string server, string database)
        {
            _server = server;
            _database = database;
            if (server == null)
                throw new ArgumentNullException(nameof(server));

            var connectionString = database == null
                ? $"Server={server};Trusted_Connection=True;MultipleActiveResultSets=True;"
                : $"Server={server};Database={database};Trusted_Connection=True;MultipleActiveResultSets=True;";

            _sqlConnection = new SqlConnection(connectionString);
            _logger.LogInformation($"connectionstring: {connectionString}");
        }

        public void Dispose()
        {
            if (_disposed)
                throw new ObjectDisposedException("Repository is already disposed");

            _sqlConnection.Dispose();
            _disposed = true;
        }

        public static async Task<DatabaseConnection> CreateConnectionAsync(SqlConnectionStringBuilder connectionBuilder)
        {
            var repo = new DatabaseConnection(connectionBuilder);
            await repo.OpenAsync();

            return repo;
        }

        public static Task<DatabaseConnection> CreateConnectionAsync(string server) => CreateConnectionAsync(server, null);

        public static async Task<DatabaseConnection> CreateConnectionAsync(string server, string database)
        {
            var repo = new DatabaseConnection(server, database);
            await repo.OpenAsync();

            return repo;
        }

        public async Task<int> ExecuteNonQueryAsync(string command)
        {
            using (var sqlCommand = new SqlCommand(command, _sqlConnection))
                return await sqlCommand.ExecuteNonQueryAsync();
        }

        public Task ExecuteQueryReaderAsync(string command, Action<SqlDataReader> callback)
        {
            return ExecuteQueryReaderAsync(command, row =>
            {
                callback(row);
                return Task.FromResult(true);
            });
        }

        public Task ExecuteQueryReaderAsync(string command, Func<SqlDataReader, Task> callback)
        {
            return ExecuteQueryReaderAsync(command, async row =>
            {
                await callback(row);
                return true;
            });
        }

        public Task ExecuteQueryReaderAsync(string command, Func<SqlDataReader, bool> callback)
        {
            return ExecuteQueryReaderAsync(command, row =>
            {
                var shouldContinue = callback(row);
                return Task.FromResult(shouldContinue);
            });
        }

        public async Task ExecuteQueryReaderAsync(string command, Func<SqlDataReader, Task<bool>> callback)
        {
            using (var sqlCommand = new SqlCommand(command, _sqlConnection))
            using (var reader = await sqlCommand.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    // stop if callback returned false
                    if (!await callback(reader))
                        break;
                }
            }
        }

        public Task OpenAsync() => _sqlConnection.OpenAsync();
    }
}
