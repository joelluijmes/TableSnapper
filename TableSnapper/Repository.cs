using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
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

        public static Task<Repository> OpenServerAsync(string server) => OpenDatabaseAsync(server, null);

        public static async Task<Repository> OpenDatabaseAsync(string server, string database)
        {
            var repo = new Repository(server, database);
            await repo.OpenAsync();

            return repo;
        }

        public Task OpenAsync() => _sqlConnection.OpenAsync();

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

        private async Task<int> ExecuteNonQueryAsync(string command)
        {
            using (var sqlCommand = new SqlCommand(command, _sqlConnection))
                return await sqlCommand.ExecuteNonQueryAsync();
        }
        
        private Task ExecuteQueryReaderAsync(string command, Action<SqlDataReader> callback)
        {
            return ExecuteQueryReaderAsync(command, row =>
            {
                callback(row);
                return Task.FromResult(true);
            });
        }

        private Task ExecuteQueryReaderAsync(string command, Func<SqlDataReader, Task> callback)
        {
            return ExecuteQueryReaderAsync(command, async row =>
            {
                await callback(row);
                return true;
            });
        }

        private Task ExecuteQueryReaderAsync(string command, Func<SqlDataReader, bool> callback)
        {
            return ExecuteQueryReaderAsync(command, row =>
            {
                var shouldContinue = callback(row);
                return Task.FromResult(shouldContinue);
            });
        }

        private async Task ExecuteQueryReaderAsync(string command, Func<SqlDataReader, Task<bool>> callback)
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

            await ExecuteQueryReaderAsync("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES", async tableRow =>
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
            await ExecuteQueryReaderAsync(query, reader =>
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
            await ExecuteQueryReaderAsync(query, reader =>
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

            await ExecuteQueryReaderAsync(query, columnRow =>
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

            await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }

        public async Task SynchronizeWithAsync(Repository inputRepository)
        {
            var tables = await inputRepository.ListTablesAsync();

            foreach (var table in tables)
            {
                await DropTable(table.Name);
                await ExecuteNonQueryAsync(CloneTableStructureSql(table));

                using (var command = new SqlCommand($"SELECT * FROM {table.Name}", inputRepository._sqlConnection))
                using (var reader = await command.ExecuteReaderAsync())
                using (var bulkCopy = new SqlBulkCopy(_sqlConnection))
                {
                    bulkCopy.BatchSize = 500;
                    bulkCopy.DestinationTableName = table.Name;

                    await bulkCopy.WriteToServerAsync(reader);
                }
            }
        }

        public async Task<string> CloneTableDataSqlAsync(Table table)
        {
            var builder = new StringBuilder();
            var tableName = table.Name;

            // we need to explicity set 'IDENTITY_INSERT' on before we can insert values in a table
            // with a identity column
            var shouldDisableIdentityInsert = table.Columns.Any(c => c.IsIdentity);
            
            if (shouldDisableIdentityInsert)
                builder.AppendLine($"SET IDENTITY_INSERT {tableName} ON");

            await ExecuteQueryReaderAsync($"SELECT * FROM {tableName}", reader =>
            {
                builder.Append($"INSERT {tableName} (");
                builder.Append(table.Columns.Select(c => c.Name).Aggregate((a, b) => $"{a}, {b}"));
                builder.Append(") VALUES (");
                builder.Append(Enumerable
                    .Range(0, reader.FieldCount)
                    .Select(i => $"'{reader[i]}'")
                    .Aggregate((a, b) => $"{a}, {b}")
                );

                builder.AppendLine(")");
            });

            if (shouldDisableIdentityInsert)
                builder.AppendLine($"SET IDENTITY_INSERT {tableName} OFF");

            return builder.ToString();
        }

        public static string CloneTableStructureSql(Table table)
        {
            var builder = new StringBuilder();
            builder.AppendLine($"CREATE TABLE {table.Name}(");

            var primaryKey = table.Keys.SingleOrDefault(key => key.IsPrimaryKey);
            var foreignKeys = table.Keys.Where(key => key.IsForeignKey).ToList();

            for (var i = 0; i < table.Columns.Count; i++)
            {
                var column = table.Columns[i];
                // COLUMN
                builder.Append($"  {column.Name} {column.DataTypeName} ");
                if (column.CharacterMaximumLength.HasValue)
                    builder.Append($"({column.CharacterMaximumLength}) ");

                if (column.DataTypeName == "decimal")
                {
                    if (column.NumericPrecision.HasValue && !column.NumericScale.HasValue)
                        builder.Append($"({column.NumericPrecision}) ");
                    else if (column.NumericPrecision.HasValue && column.NumericScale.HasValue)
                        builder.Append($"({column.NumericPrecision}, {column.NumericScale}) ");
                    else
                        throw new InvalidOperationException("Unable to parse decimal");
                }

                if (column.IsIdentity)
                    builder.Append("IDENTITY ");

                if (!column.IsNullable)
                    builder.Append("NOT NULL ");

                // DEFAULT VALUE
                if (column.DefaultValue != null)
                    builder.Append($"DEFAULT({column.DefaultValue})");

                // KEY
                if (primaryKey != null && primaryKey.Column == column.Name)
                    builder.Append(" PRIMARY KEY");

                var foreignKey = foreignKeys.SingleOrDefault(key => key.Column == column.Name);
                if (foreignKey != null)
                    builder.Append($" REFERENCES {foreignKey.ForeignTable}({foreignKey.ForeignColumn})");

                // add the , if not last column
                builder.AppendLine(i < table.Columns.Count - 1 ? "," : "");
            }

            builder.AppendLine(");");
            return builder.ToString();
        }

        public async Task<string> CloneTableSqlAsync(Table table)
        {
            var builder = new StringBuilder();

            var structureSql = CloneTableStructureSql(table);
            var dataSql = await CloneTableDataSqlAsync(table);

            builder.AppendLine(structureSql);
            builder.AppendLine();
            builder.AppendLine(dataSql);

            return builder.ToString();
        }
    }
}
