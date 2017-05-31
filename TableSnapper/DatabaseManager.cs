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
        private readonly bool _disposeConnection;
        private readonly string _schemaName;

        private bool _disposed;

        public DatabaseManager(DatabaseConnection connection, string schemaName = null, bool disposeConnection = true)
        {
            _connection = connection;
            _schemaName = schemaName;
            _disposeConnection = disposeConnection;
        }

        public void Dispose()
        {
            if (_disposed)
                throw new ObjectDisposedException("Object already disposed");

            if (_disposeConnection)
                _connection?.Dispose();

            _disposed = true;
        }

        public async Task<string> BackupToDirectoryAsync(string baseDirectory, string schemaName = null, bool splitPerTable = true, bool skipData = false)
        {
            var tables = await QueryTablesAsync(schemaName);
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
                    var path = Path.Combine(directory, $"{i + 1}_{table.SchemaName}_{table.Name}.sql");
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

        public async Task CloneFromAsync(DatabaseManager otherDatabase, string schemaName = null, bool skipData = false)
        {
            var tables = await otherDatabase.QueryTablesAsync(schemaName);

            // tables is sorted on dependency, so we delete the tables in reverse
            for (var i = tables.Count - 1; i >= 0; --i)
                await DropTableAsync(tables[i].Name, schemaName);

            foreach (var table in tables)
            {
                var clone = skipData
                    ? otherDatabase.CloneTableStructureSql(table)
                    : await otherDatabase.CloneTableSqlAsync(table);

                // clone = clone.Replace("[SCHEMA_NAME]", $"{schemaName ?? _schemaName}");
                await _connection.ExecuteNonQueryAsync(clone);
            }
        }

        public async Task CloneFromDirectoryAsync(string directory, string schemaName = null)
        {
            var files = Directory.GetFiles(directory).OrderBy(f => f).ToArray();
            if (files.Any(f => !Regex.IsMatch(f, "\\d+_.*\\.sql")))
                throw new InvalidOperationException("Directory contains one or more invalid files to import");

            foreach (var file in files)
            {
                var content = File.ReadAllText(file);

                // content = content.Replace("[SCHEMA_NAME]", $"{schemaName ?? _schemaName}");
                await _connection.ExecuteNonQueryAsync(content);
            }
        }

        public async Task<string> CloneTableDataSqlAsync(Table table)
        {
            _logger.LogDebug($"cloning table data of {table}..");

            var builder = new StringBuilder();

            // we need to explicity set 'IDENTITY_INSERT' on before we can insert values in a table
            // with a identity column
            var shouldDisableIdentityInsert = table.Columns.Any(c => c.IsIdentity);

            if (shouldDisableIdentityInsert)
                builder.AppendLine($"SET IDENTITY_INSERT {table.SchemaName}.{table.Name} ON");

            await _connection.ExecuteQueryReaderAsync($"SELECT * FROM {table.SchemaName}.{table.Name}", reader =>
            {
                builder.Append($"INSERT {table.SchemaName}.{table.Name} (");
                builder.Append(table.Columns.Select(c => c.Name).Aggregate((a, b) => $"{a}, {b}"));
                builder.Append(") VALUES (");

                for (var i = 0; i < reader.FieldCount; ++i)
                {
                    if (reader.IsDBNull(i))
                    {
                        builder.Append("NULL");
                    }
                    else
                    {
                        switch (reader[i])
                        {
                        case byte[] bytes:
                            var hexString = bytes.Select(x => x.ToString("X2")).Aggregate("0x", (a, b) => $"{a}{b}");
                            var length = table.Columns[i].CharacterMaximumLength;

                            builder.Append($"CONVERT(varbinary({(length == -1 ? "MAX" : length.ToString())}), '{hexString}')");
                            break;

                        case Guid guid:
                                builder.Append($"CONVERT(uniqueidentifier, '{guid}')");
                                break;
                        default:
                            builder.Append($"'{reader[i]}'");
                            break;
                        }
                    }

                    if (i < reader.FieldCount - 1)
                        builder.Append(", ");
                }
                        
                builder.AppendLine(")");
            });

            if (shouldDisableIdentityInsert)
                builder.AppendLine($"SET IDENTITY_INSERT {table.SchemaName}.{table.Name} OFF");

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
            builder.AppendLine($"CREATE TABLE {table.SchemaName}.{table.Name}(");

            var primaryKey = table.Keys.SingleOrDefault(key => key.IsPrimaryKey);
            var foreignKeys = table.Keys.Where(key => key.IsForeignKey).ToList();

            for (var i = 0; i < table.Columns.Count; i++)
            {
                var column = table.Columns[i];
                // COLUMN
                builder.Append($"  {column.Name} {column.DataTypeName} ");
                if (column.CharacterMaximumLength.HasValue)
                {
                    builder.Append(column.CharacterMaximumLength == -1 
                        ? "(max)" 
                        : $"({column.CharacterMaximumLength}) ");
                }

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
                    builder.Append($" REFERENCES {foreignKey.ForeignSchemaName}.{foreignKey.ForeignTable}({foreignKey.ForeignColumn})");

                // add the , if not last column
                builder.AppendLine(i < table.Columns.Count - 1 ? "," : "");
            }

            builder.AppendLine(");");

            _logger.LogDebug($"cloned table structure of {table}!");
            return builder.ToString();
        }

        public Task DropTableAsync(Table table, bool checkIfTableIsReferenced = false) =>
            DropTableAsync(table.Name, table.SchemaName, checkIfTableIsReferenced);

        public async Task DropTableAsync(string tableName, string schemaName = null, bool checkIfTableIsReferenced = false)
        {
            if (checkIfTableIsReferenced)
            {
                var referencingKeys = await QueryTableForeignKeysAsync(null, tableName);

                if (referencingKeys.Any())
                    throw new InvalidOperationException($"This table is referenced by one or more foreign keys.");
            }

            await _connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {schemaName ?? _schemaName}.{tableName}");
        }

        public static async Task<List<string>> GetDatabasesAsync(DatabaseConnection connection)
        {
            _logger.LogDebug("listing databases..");
            var databases = new List<string>();

            await connection.ExecuteQueryReaderAsync("SELECT name FROM master.dbo.sysdatabases", reader => { databases.Add(reader["name"].ToString()); });

            _logger.LogDebug($"found {databases.Count} databases");
            return databases;
        }

        public static async Task<string> GetDefaultSchema(DatabaseConnection connection)
        {
            string schema = null;
            await connection.ExecuteQueryReaderAsync("SELECT SCHEMA_NAME()", reader => { schema = reader[0].ToString(); });

            return schema;
        }

        public static async Task<List<string>> GetSchemasAsync(DatabaseConnection connection)
        {
            _logger.LogDebug("listing schemas..");
            var databases = new List<string>();

            await connection.ExecuteQueryReaderAsync("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA", reader => { databases.Add(reader["SCHEMA_NAME"].ToString()); });

            _logger.LogDebug($"found {databases.Count} schemas");
            return databases;
        }

        public async Task<List<string>> GetTablesAsync(string schemaName = null)
        {
            var query = SqlQueryBuilder
                .FromString("SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES")
                .Where("TABLE_SCHEMA", schemaName ?? _schemaName)
                .ToString();

            var tables = new List<string>();
            await _connection.ExecuteQueryReaderAsync(query, tableRow =>
            {
                var tableName = tableRow["TABLE_NAME"].ToString();

                tables.Add(tableName);
            });

            return tables;
        }

        public async Task<Table> QueryTableAsync(string tableName, string schemaName = null)
        {
            schemaName = schemaName ?? _schemaName;

            var columns = await QueryColumnsAsync(tableName, schemaName);
            var keys = await QueryKeysAsync(tableName, schemaName);

            var table = new Table(schemaName, tableName, columns, keys, null);
            return table;
        }

        public async Task<List<Table>> QueryTablesAsync(IEnumerable<string> tableNames, string schemaName = null, bool sortOnDependency = true)
        {
            var tables = await Task.WhenAll(tableNames.Select(t => QueryTableAsync(t, schemaName)));
            return SortTables(sortOnDependency, tables.ToList());
        }

        public async Task<List<Table>> QueryTablesAsync(string schemaName = null, bool sortOnDependency = true)
        {
            _logger.LogDebug("listing tables..");
            var tables = new List<Table>();

            var query = SqlQueryBuilder
                .FromString("SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES")
                .Where("TABLE_SCHEMA", schemaName ?? _schemaName)
                .ToString();

            await _connection.ExecuteQueryReaderAsync(query, async tableRow =>
            {
                var tableName = tableRow["TABLE_NAME"].ToString();
                var schema = tableRow["TABLE_SCHEMA"].ToString();

                var table = await QueryTableAsync(tableName, schema);
                tables.Add(table);
            });

            _logger.LogDebug($"found {tables.Count} tables");

            return SortTables(sortOnDependency, tables);
        }

        public Task<List<Table>> QueryTablesReferencedByAsync(Table table, bool descendReferencedTables = true) =>
            QueryTablesReferencedByAsync(table.Name, table.SchemaName, descendReferencedTables);

        public async Task<List<Table>> QueryTablesReferencedByAsync(string tableName, string schemaName = null, bool descendReferencedTables = true)
        {
            _logger.LogDebug($"listing dependent tables of {tableName}..");
            var tables = new List<Table>();

            var foreignKeys = await QueryTableForeignKeysAsync(tableName, schemaName);
            foreach (var key in foreignKeys)
            {
                tables.Add(await QueryTableAsync(key.ForeignTable, key.ForeignSchemaName));

                if (descendReferencedTables)
                    tables.AddRange(await QueryTablesReferencedByAsync(key.ForeignTable, key.ForeignSchemaName));
            }

            _logger.LogDebug($"found {tables.Count} dependent tables");
            return SortTables(tables);
        }

        //public async Task<List<Table>> QueryTablesReferencedByAsync(string tableName, string schemaName = null)
        //{
        //    _logger.LogDebug($"listing referenced tables of {tableName}..");
        //    var tables = new List<Table>();

        //    var foreignKeys = await QueryTableForeignKeysAsync(null, tableName);
        //    foreach (var table in foreignKeys.Select(f => f.TableName))
        //    {
        //        tables.Add(await QueryTableAsync(table));
        //        tables.AddRange(await QueryTablesReferencedByAsync(table));
        //    }

        //    _logger.LogDebug($"found {tables.Count} referenced tables");
        //    return tables;
        //}

        private Task<List<Column>> QueryColumnsAsync(Table table) =>
            QueryColumnsAsync(table.Name, table.SchemaName);

        private async Task<List<Column>> QueryColumnsAsync(string tableName, string schemaName = null)
        {
            _logger.LogDebug($"listing columns of {tableName}..");
            var columns = new List<Column>();

            var query = SqlQueryBuilder.FromString(
                    "SELECT	COLUMN_NAME as name,\r\n" +
                    "		TABLE_NAME as tableName,\r\n" +
                    "		ORDINAL_POSITION as position,\r\n" +
                    "		COLUMN_DEFAULT as defaultValue,\r\n" +
                    "		IS_NULLABLE as isNullable,\r\n" +
                    "		DATA_TYPE as dataType,\r\n" +
                    "		CHARACTER_MAXIMUM_LENGTH as characterMaximumLength,\r\n" +
                    "		NUMERIC_PRECISION as numericPrecision,\r\n" +
                    "		NUMERIC_SCALE as numericScale,\r\n" +
                    "		TABLE_SCHEMA as tableSchema,\r\n" +
                    "		(SELECT COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'isIdentity')) as isIdentity\r\n" +
                    "FROM	INFORMATION_SCHEMA.COLUMNS")
                .Where("TABLE_NAME", tableName)
                .Where("TABLE_SCHEMA", schemaName ?? _schemaName)
                .ToString();

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

        private Task<List<Key>> QueryKeysAsync(Table table) =>
            QueryKeysAsync(table.Name, table.SchemaName);

        private async Task<List<Key>> QueryKeysAsync(string tableName, string schemaName = null)
        {
            _logger.LogDebug("listing keys..");

            var keys = new List<Key>();

            var primaryKey = await QueryPrimaryKeyAsync(tableName, schemaName);
            if (primaryKey != null)
                keys.Add(primaryKey);

            var foreignKeys = await QueryTableForeignKeysAsync(tableName, schemaName);
            if (foreignKeys != null)
                keys.AddRange(foreignKeys);

            _logger.LogDebug($"found {keys.Count} keys");

            return keys;
        }

        private Task<Key> QueryPrimaryKeyAsync(Table table) =>
            QueryPrimaryKeyAsync(table.Name, table.SchemaName);

        private async Task<Key> QueryPrimaryKeyAsync(string tableName, string schemaName = null)
        {
            var query = SqlQueryBuilder.FromString(
                    "SELECT		tab.TABLE_NAME as tableName, COLUMN_NAME as columnName, col.CONSTRAINT_NAME as keyName\r\n" +
                    "FROM		INFORMATION_SCHEMA.TABLE_CONSTRAINTS tab\r\n" +
                    "INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE col\r\n" +
                    "ON			tab.CONSTRAINT_NAME = col.CONSTRAINT_NAME AND\r\n" +
                    "			tab.TABLE_NAME = col.TABLE_NAME")
                .Where("CONSTRAINT_TYPE", "PRIMARY KEY")
                .Where("tab.TABLE_NAME", tableName)
                .Where("tab.TABLE_SCHEMA", schemaName ?? _schemaName)
                .Where("col.TABLE_SCHEMA", schemaName ?? _schemaName)
                .ToString();

            Key primaryKey = null;
            await _connection.ExecuteQueryReaderAsync(query, reader =>
            {
                primaryKey = new Key(
                    schemaName ?? _schemaName,
                    reader["tableName"].ToString(),
                    reader["columnName"].ToString(),
                    reader["keyName"].ToString()
                );

                // stop iterating : there wouldn't be more than one primary key
                return false;
            });

            return primaryKey;
        }

        private Task<List<Key>> QueryTableForeignKeysAsync(Table table, string referencedTableName = null) =>
            QueryTableForeignKeysAsync(table.Name, table.SchemaName, referencedTableName);

        private async Task<List<Key>> QueryTableForeignKeysAsync(string tableName, string schemaName = null, string referencedTableName = null)
        {
            var query = SqlQueryBuilder.FromString(
                    "SELECT		COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,\r\n" +
                    "           f.name AS KeyName,\r\n" +
                    "			OBJECT_NAME (f.referenced_object_id) AS ReferenceTableName,\r\n" +
                    "			COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferenceColumnName,\r\n" +
                    "			OBJECT_NAME(f.parent_object_id) AS TableName,\r\n" +
                    "           OBJECT_SCHEMA_NAME(f.referenced_object_id) AS ReferenceSchemaName,\r\n" +
                    "           OBJECT_SCHEMA_NAME(fc.parent_object_id) as SchemaName\r\n" +
                    "FROM		sys.foreign_keys AS f\r\n" +
                    "INNER JOIN	sys.foreign_key_columns AS fc\r\n" +
                    "ON			f.OBJECT_ID = fc.constraint_object_id")
                .Where("OBJECT_NAME(f.parent_object_id)", tableName)
                .Where("OBJECT_NAME(f.referenced_object_id)", referencedTableName)
                .Where("OBJECT_SCHEMA_NAME(fc.parent_object_id)", schemaName ?? _schemaName)
                .ToString();

            var keys = new List<Key>();
            await _connection.ExecuteQueryReaderAsync(query, reader =>
            {
                var key = new Key(
                    reader["SchemaName"].ToString(),
                    reader["TableName"].ToString(),
                    reader["ColumnName"].ToString(),
                    reader["KeyName"].ToString(),
                    reader["ReferenceSchemaName"].ToString(),
                    reader["ReferenceTableName"].ToString(),
                    reader["ReferenceColumnName"].ToString()
                );

                keys.Add(key);
            });

            return keys;
        }

        private static List<Table> SortTables(List<Table> tables) => SortTables(true, tables);

        private static List<Table> SortTables(bool sortOnDependency, List<Table> tables)
        {
            if (!sortOnDependency)
                return tables;

            var copyTables = tables.ToArray();
            tables = tables.TopologicalSort(left => copyTables.Where(right => left != right && right.Keys.Any(key => key.ForeignTable == left.Name))).ToList();

            return tables;
        }

        private sealed class SqlQueryBuilder
        {
            private readonly StringBuilder _stringBuilder;

            private bool _appendWhere;

            private SqlQueryBuilder(string query)
            {
                _stringBuilder = new StringBuilder(query);
            }

            public static SqlQueryBuilder FromString(string query) => new SqlQueryBuilder(query);

            public override string ToString() => _stringBuilder.ToString();

            public SqlQueryBuilder Where(string key, string value)
            {
                if (string.IsNullOrEmpty(value))
                    return this;

                _stringBuilder.Append(_appendWhere
                    ? $"\r\rAND {key}='{value}'"
                    : $"\r\nWHERE {key}='{value}'"
                );

                _appendWhere = true;
                return this;
            }
        }
    }
}
