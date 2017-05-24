using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using TableSnapper.Models;

namespace TableSnapper
{
    internal class Repository : IDisposable
    {
        private readonly string _database;
        private readonly string _server;
        private readonly SqlConnection _sqlConnection;

        private bool _disposed;

        public Repository(string server, string database)
        {
            _server = server;
            _database = database;
            if (server == null)
                throw new ArgumentNullException(nameof(server));

            _sqlConnection = new SqlConnection(database == null
                ? $"Server={server};Trusted_Connection=True;MultipleActiveResultSets=True;"
                : $"Server={server};Database={database};Trusted_Connection=True;MultipleActiveResultSets=True;");
        }

        public void Dispose()
        {
            if (_disposed)
                throw new ObjectDisposedException("Repository is already disposed");

            _sqlConnection.Dispose();
            _disposed = true;
        }

        public static Task<Repository> OpenServerAsync(string server)
        {
            return OpenDatabaseAsync(server, null);
        }

        public static async Task<Repository> OpenDatabaseAsync(string server, string database)
        {
            var repo = new Repository(server, database);
            await repo.OpenAsync();

            return repo;
        }

        public Task OpenAsync()
        {
            return _sqlConnection.OpenAsync();
        }

        public async Task<List<string>> ListDatabasesAsync()
        {
            var databases = new List<string>();

            using (var sqlCommand = new SqlCommand("SELECT name FROM master.dbo.sysdatabases", _sqlConnection))
            using (var reader = await sqlCommand.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                    databases.Add(reader["name"].ToString());
            }

            return databases;
        }

        private Task ProcessQueryAsync(string command, Action<SqlDataReader> callback)
        {
            return ProcessQueryAsync(command, row =>
            {
                callback(row);
                return Task.FromResult(true);
            });
        }

        private Task ProcessQueryAsync(string command, Func<SqlDataReader, Task> callback)
        {
            return ProcessQueryAsync(command, async row =>
            {
                await callback(row);
                return true;
            });
        }

        private Task ProcessQueryAsync(string command, Func<SqlDataReader, bool> callback)
        {
            return ProcessQueryAsync(command, row =>
            {
                var shouldContinue = callback(row);
                return Task.FromResult(shouldContinue);
            });
        }

        private async Task ProcessQueryAsync(string command, Func<SqlDataReader, Task<bool>> callback)
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

        public async Task<List<Table>> ListTablesAsync()
        {
            var tables = new List<Table>();

            await ProcessQueryAsync("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES", async tableRow =>
            {
                var tableName = tableRow["TABLE_NAME"].ToString();

                var columns = await ListColumnsAsync(tableName);
                var keys = await ListKeysAsync(tableName);

                var table = new Table(tableName, columns, keys, null);
                tables.Add(table);
            });

            return tables;
        }

        private async Task<Key> ListPrimaryKeyAsync(string tableName)
        {
            var query = "SELECT		tab.TABLE_NAME as tableName, COLUMN_NAME as columnName, col.CONSTRAINT_NAME as keyName\r\n" +
                        "FROM		INFORMATION_SCHEMA.TABLE_CONSTRAINTS tab\r\n" +
                        "INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE col\r\n" +
                        "ON			tab.CONSTRAINT_NAME = col.CONSTRAINT_NAME AND\r\n" +
                        "			tab.TABLE_NAME = col.TABLE_NAME\r\n" +
                        "WHERE		CONSTRAINT_TYPE = 'PRIMARY KEY' AND\r\n" +
                        (tableName != null ? $"tab.TABLE_NAME = '{tableName}'" : "");

            Key primaryKey = null;
            await ProcessQueryAsync(query, reader =>
            {
                primaryKey = new Key(
                    reader["tableName"].ToString(),
                    reader["columnName"].ToString(),
                    reader["keyName"].ToString()
                );
                
                // stop iterating : there wouldn't be more than one primary key
                return false;
            });

            return primaryKey;
        }

        private async Task<List<Key>> ListForeignKeysAsync(string tableName)
        {
            var query = "SELECT		COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,\r\n" +
                        "           f.name AS KeyName,\r\n" +
                        "			OBJECT_NAME (f.referenced_object_id) AS ReferenceTableName,\r\n" +
                        "			COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferenceColumnName,\r\n" +
                        "			OBJECT_NAME(f.parent_object_id) AS TableName\r\n" +
                        "FROM		sys.foreign_keys AS f\r\n" +
                        "INNER JOIN	sys.foreign_key_columns AS fc\r\n" +
                        "ON			f.OBJECT_ID = fc.constraint_object_id\r\n" +
                        (tableName != null ? $"WHERE OBJECT_NAME(f.parent_object_id) = '{tableName}'" : "");

            var keys = new List<Key>();
            await ProcessQueryAsync(query, reader =>
            {
                var key = new Key(
                    reader["TableName"].ToString(),
                    reader["ColumnName"].ToString(),
                    reader["KeyName"].ToString(),
                    reader["ReferenceTableName"].ToString(),
                    reader["ReferenceColumnName"].ToString()
                );

                keys.Add(key);
            });

            return keys;
        }

        private async Task<List<Key>> ListKeysAsync(string tableName)
        {
            var keys = new List<Key>();

            var primaryKey = await ListPrimaryKeyAsync(tableName);
            if (primaryKey != null)
                keys.Add(primaryKey);

            var foreignKeys = await ListForeignKeysAsync(tableName);
            if (foreignKeys != null)
                keys.AddRange(foreignKeys);

            return keys;
        }

        private async Task<List<Column>> ListColumnsAsync(string tableName)
        {
            var columns = new List<Column>();

            var query = "SELECT	COLUMN_NAME as name,\r\n" +
                        "		TABLE_NAME as tableName,\r\n" +
                        "		ORDINAL_POSITION as position,\r\n" +
                        "		COLUMN_DEFAULT as defaultValue,\r\n" +
                        "		IS_NULLABLE as isNullable,\r\n" +
                        "		DATA_TYPE as dataType,\r\n" +
                        "		CHARACTER_MAXIMUM_LENGTH as characterMaximumLength,\r\n" +
                        "		NUMERIC_PRECISION as numericPrecision,\r\n" +
                        "		NUMERIC_SCALE as numericScale,\r\n" +
                        "		(SELECT COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'isIdentity')) as isIdentity\r\n" +
                        "FROM	INFORMATION_SCHEMA.COLUMNS\r\n" +
                        (tableName != null ? $"WHERE TABLE_NAME='{tableName}'" : "");

            await ProcessQueryAsync(query, columnRow =>
            {
                var column = new Column(
                    columnRow["tableName"].ToString(),
                    columnRow.GetString(columnRow.GetOrdinal("name")),
                    columnRow.GetInt32(columnRow.GetOrdinal("position")),
                    columnRow.IsDBNull(columnRow.GetOrdinal("defaultValue")) ? null : columnRow["defaultValue"],
                    columnRow.GetString(columnRow.GetOrdinal("isNullable")) == "YES",
                    columnRow.GetString(columnRow.GetOrdinal("dataType")),
                    columnRow.GetNullableInt("characterMaximumLength"),
                    columnRow.GetNullableInt("numericPrecision"),
                    columnRow.GetNullableInt("numericScale"),
                    columnRow.GetInt32(columnRow.GetOrdinal("isIdentity")) == 1
                );

                columns.Add(column);
            });
            return columns;
        }

        public async Task CopyTableToAsync(string table, Repository targetRepository)
        {
            using (var sqlCommand = new SqlCommand($"SELECT * FROM {table}", _sqlConnection))
            using (var reader = await sqlCommand.ExecuteReaderAsync())
            using (var bulkCopy = new SqlBulkCopy(targetRepository._sqlConnection))
            {
                bulkCopy.DestinationTableName = table;
                await bulkCopy.WriteToServerAsync(reader);
            }
        }

        public async Task DropTable(string tableName, bool dropReferencingTables = true)
        {
            var referencingKeys = 
                (await ListForeignKeysAsync(null))
                .Where(key => key.IsForeignKey && key.ForeignTable == tableName)
                .ToList();

            if (referencingKeys.Any())
            {
                if (!dropReferencingTables)
                    throw new InvalidOperationException($"This table is referenced by one or more foreign keys.");

                foreach (var key in referencingKeys)
                    await DropTable(key.TableName);
            }

            await ProcessQueryAsync($"DROP TABLE IF EXISTS {tableName}", x => {});
        }
        
        public async Task SynchronizeWithAsync(Repository inputRepository)
        {
            var tables = await inputRepository.ListTablesAsync();
            
            foreach (var table in tables)
                await DropTable(table.Name);
        }
    }
}
