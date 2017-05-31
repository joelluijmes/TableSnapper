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

        public DatabaseConnection(SqlConnectionStringBuilder sqlBuilder)
        {
            _server = sqlBuilder.DataSource;
            _database = sqlBuilder.InitialCatalog;

            sqlBuilder.MultipleActiveResultSets = true;

            var connectionString = sqlBuilder.ConnectionString;
            _logger.LogTrace($"connectionstring: {connectionString}");

            _sqlConnection = new SqlConnection(connectionString);
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
            var connectionBuilder = new SqlConnectionStringBuilder
            {
                DataSource = server,
                InitialCatalog = database,
                IntegratedSecurity = true
            };

            var repo = new DatabaseConnection(connectionBuilder);
            await repo.OpenAsync();

            return repo;
        }

        public static async Task<DatabaseConnection> CreateConnectionAsync(string server, string database, string username, string password)
        {
            var connectionBuilder = new SqlConnectionStringBuilder
            {
                DataSource = server,
                UserID = username,
                Password = password,
                InitialCatalog = database,
                IntegratedSecurity = false
            };

            var repo = new DatabaseConnection(connectionBuilder);
            await repo.OpenAsync();

            return repo;
        }

        public async Task<int> ExecuteNonQueryAsync(string command)
        {
            _logger.LogTrace(command);

            using (var sqlCommand = new SqlCommand(command, _sqlConnection))
            {
                var count = await sqlCommand.ExecuteNonQueryAsync();
                _logger.LogTrace($"{count} updated");

                return count;
            }
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
            _logger.LogTrace(command);

            var count = 0;
            using (var sqlCommand = new SqlCommand(command, _sqlConnection))
            using (var reader = await sqlCommand.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    // stop if callback returned false
                    if (await callback(reader))
                        continue;

                    _logger.LogTrace("callback returned false -> early exit");
                    break;
                }

                ++count;
            }

            _logger.LogTrace($"{count} rows read");
        }

        public Task OpenAsync() => _sqlConnection.OpenAsync();
    }
}
