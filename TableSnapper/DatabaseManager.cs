using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using TableSnapper.Models;

namespace TableSnapper
{
    internal sealed class DatabaseManager : IDisposable
    {
        private static readonly ILogger _logger = Program.CreateLogger<DatabaseManager>();

        private readonly DatabaseConnection _connection;

        private bool _disposed;

        public DatabaseManager(DatabaseConnection connection)
        {
            _connection = connection;
        }

        public void Dispose()
        {
            if (_disposed)
                throw new ObjectDisposedException("Object already disposed");

            _connection?.Dispose();
            _disposed = true;
        }

        public async Task<string> BackupToDirectoryAsync(string baseDirectory, bool splitPerTable = true, bool skipData = false)
        {
            var tables = await ListTablesAsync();
            return await BackupToDirectoryAsync(baseDirectory, tables, splitPerTable, skipData);
        }

        public async Task<string> BackupToDirectoryAsync(string baseDirectory, IList<Table> tables, bool splitPerTable = true, bool skipData = false)
        {
            var directory = Path.Combine(baseDirectory, $"{DateTime.Now:ddMMyy-HHmmss}");
            Directory.CreateDirectory(directory);

            for (var i = 0; i < tables.Count; i++)
            {
                var table = tables[i];

                var clone = skipData
                    ? CloneTableStructureSql(table)
                    : await CloneTableSqlAsync(table);

                if (splitPerTable)
                {
                    var path = Path.Combine(directory, $"{i + 1}_{table.Name}.sql");
                    File.WriteAllText(path, clone);
                }
                else
                {
                    var path = Path.Combine(directory, "0_backup.sql");
                    File.AppendAllText(path, clone);
                }
            }

            return directory;
        }

        public async Task CloneFromAsync(DatabaseManager otherDatabase, bool skipData = false)
        {
            var tables = await otherDatabase.ListTablesAsync();

            // tables is sorted on dependency, so we delete the tables in reverse
            for (var i = tables.Count - 1; i >= 0; --i)
                await DropTableAsync(tables[i].Name);

            foreach (var table in tables)
            {
                var clone = skipData
                    ? otherDatabase.CloneTableStructureSql(table)
                    : await otherDatabase.CloneTableSqlAsync(table);

                await _connection.ExecuteNonQueryAsync(clone);
            }
        }

        public async Task CloneFromDirectoryAsync(string directory)
        {
            var files = Directory.GetFiles(directory).OrderBy(f => f).ToArray();
            if (files.Any(f => !Regex.IsMatch(f, "\\d+_.*\\.sql")))
                throw new InvalidOperationException("Directory contains one or more invalid files to import");

            foreach (var file in files)
            {
                var content = File.ReadAllText(file);
                await _connection.ExecuteNonQueryAsync(content);
            }
        }

        public async Task<string> CloneTableDataSqlAsync(Table table)
        {
            _logger.LogDebug($"cloning table data of {table}..");

            var builder = new StringBuilder();
            var tableName = table.Name;

            // we need to explicity set 'IDENTITY_INSERT' on before we can insert values in a table
            // with a identity column
            var shouldDisableIdentityInsert = table.Columns.Any(c => c.IsIdentity);

            if (shouldDisableIdentityInsert)
                builder.AppendLine($"SET IDENTITY_INSERT {tableName} ON");

            await _connection.ExecuteQueryReaderAsync($"SELECT * FROM {tableName}", reader =>
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

            _logger.LogDebug($"cloned table data of {table}!");
            return builder.ToString();
        }

        public async Task<string> CloneTableSqlAsync(Table table)
        {
            _logger.LogDebug($"cloning full table {table}..");
            var builder = new StringBuilder();

            var structureSql = CloneTableStructureSql(table);
            var dataSql = await CloneTableDataSqlAsync(table);

            builder.AppendLine(structureSql);
            builder.AppendLine();
            builder.AppendLine(dataSql);

            _logger.LogDebug($"cloned full table {table}!");
            return builder.ToString();
        }

        public string CloneTableStructureSql(Table table)
        {
            _logger.LogDebug($"cloning table structure of {table}..");

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

            _logger.LogDebug($"cloned table structure of {table}!");
            return builder.ToString();
        }

        public async Task DropTableAsync(string tableName, bool checkIfTableIsReferenced = false)
        {
            if (checkIfTableIsReferenced)
            {
                var referencingKeys = await ListTableForeignKeysAsync(null, tableName);

                if (referencingKeys.Any())
                    throw new InvalidOperationException($"This table is referenced by one or more foreign keys.");
            }

            await _connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }

        public async Task<Table> GetTableAsync(string tableName)
        {
            var columns = await ListColumnsAsync(tableName);
            var keys = await ListKeysAsync(tableName);

            var table = new Table(tableName, columns, keys, null);
            return table;
        }

        public async Task<List<string>> ListDatabasesAsync()
        {
            _logger.LogDebug("listing databases..");
            var databases = new List<string>();

            await _connection.ExecuteQueryReaderAsync("SELECT name FROM master.dbo.sysdatabases", reader => { databases.Add(reader["name"].ToString()); });

            _logger.LogDebug($"found {databases.Count} databases");
            return databases;
        }

        public async Task<List<Table>> ListTablesAsync(bool sortOnDependency = true)
        {
            _logger.LogDebug("listing tables..");
            var tables = new List<Table>();

            await _connection.ExecuteQueryReaderAsync("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES", async tableRow =>
            {
                var tableName = tableRow["TABLE_NAME"].ToString();

                var table = await GetTableAsync(tableName);
                tables.Add(table);
            });

            _logger.LogDebug($"found {tables.Count} tables");

            if (!sortOnDependency)
                return tables;

            var copyTables = tables.ToArray();
            tables = tables.TopologicalSort(left => copyTables.Where(right => left != right && right.Keys.Any(key => key.ForeignTable == left.Name))).ToList();

            return tables;
        }

        public async Task<List<Table>> ListTablesDependentOnAsync(string tableName)
        {
            _logger.LogDebug($"listing dependent tables of {tableName}..");
            var tables = new List<Table>();

            var foreignKeys = await ListTableForeignKeysAsync(tableName);
            foreach (var table in foreignKeys.Select(f => f.ForeignTable))
            {
                tables.Add(await GetTableAsync(table));
                tables.AddRange(await ListTablesDependentOnAsync(table));
            }

            _logger.LogDebug($"found {tables.Count} dependent tables");
            return tables;
        }

        public async Task<List<Table>> ListTablesReferencedByAsync(string tableName)
        {
            _logger.LogDebug($"listing referenced tables of {tableName}..");
            var tables = new List<Table>();

            var foreignKeys = await ListTableForeignKeysAsync(null, tableName);
            foreach (var table in foreignKeys.Select(f => f.TableName))
            {
                tables.Add(await GetTableAsync(table));
                tables.AddRange(await ListTablesReferencedByAsync(table));
            }

            _logger.LogDebug($"found {tables.Count} referenced tables");
            return tables;
        }

        private async Task<List<Column>> ListColumnsAsync(string tableName)
        {
            _logger.LogDebug($"listing columns of {tableName}..");
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

            await _connection.ExecuteQueryReaderAsync(query, columnRow =>
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
                    columnRow.GetNullableInt("isIdentity") == 1
                );

                columns.Add(column);
            });

            _logger.LogDebug($"found {columns.Count} columns in {tableName}");
            return columns;
        }

        private async Task<List<Key>> ListKeysAsync(string tableName)
        {
            _logger.LogDebug("listing keys..");

            var keys = new List<Key>();

            var primaryKey = await ListPrimaryKeyAsync(tableName);
            if (primaryKey != null)
                keys.Add(primaryKey);

            var foreignKeys = await ListTableForeignKeysAsync(tableName);
            if (foreignKeys != null)
                keys.AddRange(foreignKeys);

            _logger.LogDebug($"found {keys.Count} keys");

            return keys;
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
            await _connection.ExecuteQueryReaderAsync(query, reader =>
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

        private async Task<List<Key>> ListTableForeignKeysAsync(string tableName, string referencedTableName = null)
        {
            var query = "SELECT		COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,\r\n" +
                        "           f.name AS KeyName,\r\n" +
                        "			OBJECT_NAME (f.referenced_object_id) AS ReferenceTableName,\r\n" +
                        "			COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferenceColumnName,\r\n" +
                        "			OBJECT_NAME(f.parent_object_id) AS TableName\r\n" +
                        "FROM		sys.foreign_keys AS f\r\n" +
                        "INNER JOIN	sys.foreign_key_columns AS fc\r\n" +
                        "ON			f.OBJECT_ID = fc.constraint_object_id" +
                        (tableName != null ? $"\r\nWHERE OBJECT_NAME(f.parent_object_id) = '{tableName}'" : "") +
                        (referencedTableName != null ? $"\r\nWHERE OBJECT_NAME(f.referenced_object_id) = '{referencedTableName}'" : "");

            var keys = new List<Key>();
            await _connection.ExecuteQueryReaderAsync(query, reader =>
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
    }
}
