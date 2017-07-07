using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using tableshot.Models;

namespace tableshot
{
    public sealed class DatabaseManager
    {
        private static readonly ILogger _logger = ApplicationLogging.CreateLogger<DatabaseManager>();

        public DatabaseManager(DatabaseConnection connection)
        {
            Connection = connection;
        }

        public DatabaseConnection Connection { get; }

        public Task CreateSchemaAsync(string schemaName)
        {
            return Connection.ExecuteNonQueryAsync($"CREATE SCHEMA {schemaName}");
        }

        public Task DropTableAsync(ShallowTable table, bool checkIfTableIsReferenced)
        {
            return DropTableAsync(table.Name, table.SchemaName, checkIfTableIsReferenced);
        }

        public async Task DropTableAsync(string tableName, string schemaName, bool checkIfTableIsReferenced)
        {
            if (checkIfTableIsReferenced)
            {
                var referencingKeys = await ListTableForeignKeysAsync(tableName, schemaName, ReferencedByOptions.Ascending);

                if (referencingKeys.Any())
                    throw new InvalidOperationException($"This table is referenced by one or more foreign keys.");
            }

            await Connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {schemaName}.{tableName}");
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;

            return obj is DatabaseManager && Equals((DatabaseManager) obj);
        }

        public static async Task<string> GetDefaultSchema(DatabaseConnection connection)
        {
            string schema = null;
            await connection.ExecuteQueryReaderAsync("SELECT SCHEMA_NAME()", reader => { schema = reader[0].ToString(); });

            return schema;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Connection != null ? Connection.GetHashCode() : 0) * 397;
            }
        }

        public static async Task<List<string>> ListDatabasesAsync(DatabaseConnection connection)
        {
            _logger.LogDebug("listing databases..");
            var databases = new List<string>();

            await connection.ExecuteQueryReaderAsync("SELECT name FROM master.dbo.sysdatabases", reader => { databases.Add(reader["name"].ToString()); });

            _logger.LogDebug($"found {databases.Count} databases");
            return databases;
        }

        public static async Task<List<string>> ListSchemasAsync(DatabaseConnection connection)
        {
            _logger.LogDebug("listing schemas..");
            var databases = new List<string>();

            await connection.ExecuteQueryReaderAsync("SELECT SCHEMA_NAME, SCHEMA_OWNER FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME != SCHEMA_OWNER",
                reader => { databases.Add(reader["SCHEMA_NAME"].ToString()); });

            _logger.LogDebug($"found {databases.Count} schemas");
            return databases;
        }

        public async Task<List<Schema>> ListSchemasAsync()
        {
            var schemaNames = await ListSchemasAsync(Connection);
            return (await Task.WhenAll(schemaNames.Select(async schemaName => new Schema(schemaName, await ListTableNamesAsync(null, schemaName))))).ToList();
        }

        public async Task<List<ShallowTable>> ListShallowTablesAsync(string tableName, string schemaName)
        {
            _logger.LogDebug("listing tables..");
            var tables = new List<ShallowTable>();

            var query = SqlQueryBuilder
                .FromString("SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES")
                .Where(schemaName, "TABLE_SCHEMA")
                .Where(tableName, "TABLE_NAME")
                .ToString();

            await Connection.ExecuteQueryReaderAsync(query, tableRow =>
            {
                var rowTable = tableRow["TABLE_NAME"].ToString();
                var rowSchema = tableRow["TABLE_SCHEMA"].ToString();

                tables.Add(new ShallowTable(rowSchema, rowTable));
            });

            _logger.LogDebug($"found {tables.Count} tables");

            return tables;
        }

        public async Task<List<ShallowTable>> ListShallowTablesAsync(string tableName, SchemaScope schemaScope)
        {
            _logger.LogDebug("listing tables..");
            var tables = new List<ShallowTable>();

            var query = SqlQueryBuilder
                .FromString("SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES")
                .WhereIn(schemaScope, "TABLE_SCHEMA")
                .Where(tableName, "TABLE_NAME")
                .ToString();

            await Connection.ExecuteQueryReaderAsync(query, tableRow =>
            {
                var rowTable = tableRow["TABLE_NAME"].ToString();
                var rowSchema = tableRow["TABLE_SCHEMA"].ToString();

                tables.Add(new ShallowTable(rowSchema, rowTable));
            });

            _logger.LogDebug($"found {tables.Count} tables");

            return tables;
        }

        public Task<List<Column>> ListTableColumnsAsync(ShallowTable table)
        {
            return ListTableColumnsAsync(table.Name, table.SchemaName);
        }

        public async Task<List<Column>> ListTableColumnsAsync(string tableName, string schemaName)
        {
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));
            if (schemaName == null)
                throw new ArgumentNullException(nameof(schemaName));

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
                .Where(tableName, "TABLE_NAME")
                .Where(schemaName, "TABLE_SCHEMA")
                .ToString();

            await Connection.ExecuteQueryReaderAsync(query, columnRow =>
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

        public async Task<List<string>> ListTableNamesAsync(string tableName, string schemaName)
        {
            _logger.LogDebug("listing tables..");

            var query = SqlQueryBuilder
                .FromString("SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES")
                .Where(schemaName, "TABLE_SCHEMA")
                .Where(tableName, "TABLE_NAME")
                .ToString();

            var tables = new List<string>();
            await Connection.ExecuteQueryReaderAsync(query, tableRow =>
            {
                var rowTable = tableRow["TABLE_NAME"].ToString();

                tables.Add(rowTable);
            });

            _logger.LogDebug($"found {tables.Count} tables");

            return tables;
        }

        public async Task<List<Table>> ListTablesAsync(string tableName, string schemaName, bool sortOnDependency = true)
        {
            var shallowTables = await ListShallowTablesAsync(tableName, schemaName);
            var tables = await Task.WhenAll(shallowTables.Select(async table => await QueryTableAsync(table)));

            return TopologicalSort(tables, sortOnDependency);
        }

        public Task<List<ShallowTable>> ListTablesReferencedByAsync(ShallowTable table, ReferencedByOptions options, SchemaScope schemaScope)
        {
            return ListTablesReferencedByAsync(table.Name, table.SchemaName, options, schemaScope);
        }

        public async Task<List<ShallowTable>> ListTablesReferencedByAsync(string tableName, string schemaName, ReferencedByOptions options, SchemaScope schemaScope)
        {
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));
            if (schemaName == null)
                throw new ArgumentNullException(nameof(schemaName));

            _logger.LogDebug($"listing dependent tables of {schemaName}.{tableName}..");
            var referencedTables = await ListTablesReferencedByAsyncImpl(tableName, schemaName, options, schemaScope);
            referencedTables[new ShallowTable(schemaName, tableName)] = referencedTables.Keys.ToList();

            var sorted = referencedTables.Keys.TopologicalSort(table => referencedTables[table], false);
            if (options.HasFlag(ReferencedByOptions.Ascending))
                sorted = sorted.Reverse();

            var tables = sorted.ToList();
            _logger.LogDebug($"found {tables.Count} dependent tables");
            return tables;
        }

        public static bool operator ==(DatabaseManager left, DatabaseManager right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(DatabaseManager left, DatabaseManager right)
        {
            return !Equals(left, right);
        }

        public Task<Table> QueryTableAsync(ShallowTable table)
        {
            return QueryTableAsync(table.Name, table.SchemaName);
        }

        public async Task<Table> QueryTableAsync(string tableName, string schemaName)
        {
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));
            if (schemaName == null)
                throw new ArgumentNullException(nameof(schemaName));

            if (!await TableExistsAsync(tableName, schemaName))
                return null;

            var columns = await ListTableColumnsAsync(tableName, schemaName);
            var keys = await ListTableKeysAsync(tableName, schemaName);

            var table = new Table(schemaName, tableName, columns, keys, null);
            return table;
        }

        public async Task<bool> SchemaExistsAsync(string schemaName)
        {
            var query = SqlQueryBuilder
                .FromString("SELECT SCHEMA_NAME, SCHEMA_OWNER FROM INFORMATION_SCHEMA.SCHEMATA")
                .Where(schemaName, "SCHEMA_NAME")
                .ToString();

            var anyRow = false;
            await Connection.ExecuteQueryReaderAsync(query, x => { anyRow = true; });

            return anyRow;
        }

        public static List<Table> TopologicalSort(IEnumerable<Table> tables)
        {
            return TopologicalSort(tables, true);
        }

        public Task<bool> TableExistsAsync(ShallowTable table)
        {
            return TableExistsAsync(table.Name, table.SchemaName);
        }

        public async Task<bool> TableExistsAsync(string tableName, string schemaName)
        {
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));
            if (schemaName == null)
                throw new ArgumentNullException(nameof(schemaName));

            var query = SqlQueryBuilder
                .FromString("SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES")
                .Where(schemaName, "TABLE_SCHEMA")
                .Where(tableName, "TABLE_NAME")
                .ToString();

            var anyRow = false;
            await Connection.ExecuteQueryReaderAsync(query, x => { anyRow = true; });

            return anyRow;
        }

        public Task TruncateTableAsync(ShallowTable table, bool truncateReferenced)
        {
            return TruncateTableAsync(table.Name, table.SchemaName, truncateReferenced);
        }

        public async Task TruncateTableAsync(string tableName, string schemaName, bool truncateReferenced)
        {
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));
            if (schemaName == null)
                throw new ArgumentNullException(nameof(schemaName));

            if (truncateReferenced)
            {
                var referenced = await ListTablesReferencedByAsync(tableName, schemaName, ReferencedByOptions.Descending, SchemaScope.All);

                foreach (var table in referenced)
                    await TruncateTableAsync(table.Name, table.SchemaName, true);
            }

            await Connection.ExecuteNonQueryAsync($"TRUNCATE TABLE {schemaName}.{tableName}");
        }

        private bool Equals(DatabaseManager other)
        {
            return Equals(Connection, other.Connection);
        }

        private Task<List<Key>> ListTableForeignKeysAsync(ShallowTable table, ReferencedByOptions options)
        {
            return ListTableForeignKeysAsync(table.Name, table.SchemaName, options);
        }

        private async Task<List<Key>> ListTableForeignKeysAsync(string tableName, string schemaName, ReferencedByOptions options)
        {
            if (options == ReferencedByOptions.Disabled)
                throw new ArgumentException("Disabled doesn't make sense in this context.", nameof(options));
            if (!options.HasFlag(ReferencedByOptions.Ascending) && !options.HasFlag(ReferencedByOptions.Descending))
                throw new ArgumentException("Can't resolve Foreign keys without selecting either Descending (referenced to) or Ascending (referenced by)", nameof(options));

            var queryBuilder = SqlQueryBuilder.FromString(
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
                .Where(options.HasFlag(ReferencedByOptions.Ascending) && !options.HasFlag(ReferencedByOptions.Descending), tableName, "OBJECT_NAME(f.referenced_object_id)")
                .Where(options.HasFlag(ReferencedByOptions.Descending) && !options.HasFlag(ReferencedByOptions.Ascending), tableName, "OBJECT_NAME(f.parent_object_id)")
                .Where(options.HasFlag(ReferencedByOptions.Schema) && options.HasFlag(ReferencedByOptions.Ascending), schemaName, "OBJECT_SCHEMA_NAME(fc.parent_object_id)")
                .Where(options.HasFlag(ReferencedByOptions.Schema) && options.HasFlag(ReferencedByOptions.Descending), schemaName, "OBJECT_SCHEMA_NAME(f.referenced_object_id)")
                .WhereEither(schemaName, "OBJECT_SCHEMA_NAME(f.referenced_object_id)", "OBJECT_SCHEMA_NAME(fc.parent_object_id)")
                .WhereEither(options.HasFlag(ReferencedByOptions.Ascending) && options.HasFlag(ReferencedByOptions.Descending), tableName, "OBJECT_NAME(f.referenced_object_id)", "OBJECT_NAME(fc.parent_object_id)");

            var query = queryBuilder.ToString();

            var keys = new List<Key>();
            await Connection.ExecuteQueryReaderAsync(query, reader =>
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

        private Task<List<Key>> ListTableKeysAsync(ShallowTable table)
        {
            return ListTableKeysAsync(table.Name, table.SchemaName);
        }

        private async Task<List<Key>> ListTableKeysAsync(string tableName, string schemaName)
        {
            _logger.LogDebug("listing keys..");

            var keys = new List<Key>();

            var primaryKey = await QueryPrimaryKeyAsync(tableName, schemaName);
            if (primaryKey != null)
                keys.Add(primaryKey);

            var foreignKeys = await ListTableForeignKeysAsync(tableName, schemaName, ReferencedByOptions.Descending);
            if (foreignKeys != null)
                keys.AddRange(foreignKeys);

            _logger.LogDebug($"found {keys.Count} keys");

            return keys;
        }

        private Task<Dictionary<ShallowTable, List<ShallowTable>>> ListTablesReferencedByAsyncImpl(string tableName, string schemaName, ReferencedByOptions options, SchemaScope schemaScope)
        {
            return ListTablesReferencedByAsyncImpl(tableName, schemaName, options, schemaScope, new Dictionary<ShallowTable, List<ShallowTable>>());
        }
        
        private Task<Dictionary<ShallowTable, List<ShallowTable>>> ListTablesReferencedByAsyncImpl(ShallowTable table, ReferencedByOptions options, SchemaScope schemaScope, Dictionary<ShallowTable, List<ShallowTable>> tables)
        {
            return ListTablesReferencedByAsyncImpl(table.Name, table.SchemaName, options, schemaScope, tables);
        }

        private async Task<Dictionary<ShallowTable, List<ShallowTable>>> ListTablesReferencedByAsyncImpl(string tableName, string schemaName, ReferencedByOptions options, SchemaScope schemaScope, Dictionary<ShallowTable, List<ShallowTable>> tables)
        {
            if (options == ReferencedByOptions.Disabled)
                throw new ArgumentException("Referenced is disabled", nameof(options));
            if (!options.HasFlag(ReferencedByOptions.Ascending) && !options.HasFlag(ReferencedByOptions.Descending))
                throw new ArgumentException("Can't resolve Referenced Tables without selecting either Descending (referenced to) or Ascending (referenced by)", nameof(options));

            if (!schemaScope.Contains(schemaName))
                return tables;

            var currentTable = new ShallowTable(schemaName, tableName);
            tables[currentTable] = new List<ShallowTable>();

            var foreignKeys = await ListTableForeignKeysAsync(tableName, schemaName, options);
            foreach (var key in foreignKeys)
            {
                //var shallowTable = new ShallowTable(key.ForeignSchemaName, key.ForeignTable);

                //// check if we decend, if we do check if we limit to our own scheme only, and last skip already descended tables
                //if (options == ReferencedByOptions.Disabled ||
                //    options == ReferencedByOptions.Schema && !shallowTable.SchemaName.Equals(schemaName, StringComparison.CurrentCultureIgnoreCase) ||
                //    tables.ContainsKey(shallowTable))
                //    continue;
                
                async Task ProcessTable(ShallowTable table)
                {
                    // only descend/ascend in the specified scope
                    if (tables.ContainsKey(table) || !schemaScope.Contains(table.SchemaName))
                        return;

                    var referenced = await ListTablesReferencedByAsyncImpl(table, options, schemaScope, tables);
                    // throw new NotImplementedException($"Result Referenced Tables already contained definition for {foreignTable}, how to deal with this?");

                    tables[table] = referenced.Keys.ToList();
                    foreach (var pair in referenced)
                    {
                        if (tables.ContainsKey(pair.Key))
                            continue;
                        // throw new NotImplementedException($"Result Referenced Tables already contained definition for {pair.Key}, how to deal with this?");

                        tables[pair.Key] = pair.Value;
                    }
                }

                var ascendTable = new ShallowTable(key.SchemaName, key.TableName);
                var descendTable = new ShallowTable(key.ForeignSchemaName, key.ForeignTable);

                if (options.HasFlag(ReferencedByOptions.Ascending) && !currentTable.Equals(ascendTable))
                    await ProcessTable(ascendTable);
                if (options.HasFlag(ReferencedByOptions.Descending) && !currentTable.Equals(descendTable))
                    await ProcessTable(descendTable);
            }

            return tables;
        }

        private Task<Key> QueryPrimaryKeyAsync(ShallowTable table)
        {
            return QueryPrimaryKeyAsync(table.Name, table.SchemaName);
        }

        private async Task<Key> QueryPrimaryKeyAsync(string tableName, string schemaName)
        {
            var query = SqlQueryBuilder.FromString(
                    "SELECT		tab.TABLE_NAME as tableName, COLUMN_NAME as columnName, col.CONSTRAINT_NAME as keyName\r\n" +
                    "FROM		INFORMATION_SCHEMA.TABLE_CONSTRAINTS tab\r\n" +
                    "INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE col\r\n" +
                    "ON			tab.CONSTRAINT_NAME = col.CONSTRAINT_NAME AND\r\n" +
                    "			tab.TABLE_NAME = col.TABLE_NAME")
                .Where("PRIMARY KEY", "CONSTRAINT_TYPE")
                .Where(tableName, "tab.TABLE_NAME")
                .Where(schemaName, "tab.TABLE_SCHEMA")
                .Where(schemaName, "col.TABLE_SCHEMA")
                .ToString();

            Key primaryKey = null;
            await Connection.ExecuteQueryReaderAsync(query, reader =>
            {
                primaryKey = new Key(
                    schemaName,
                    reader["tableName"].ToString(),
                    reader["columnName"].ToString(),
                    reader["keyName"].ToString()
                );

                // stop iterating : there wouldn't be more than one primary key
                return false;
            });

            return primaryKey;
        }

        private static List<Table> TopologicalSort(IEnumerable<Table> tables, bool sortOnDependency)
        {
            var copyTables = tables.ToList();

            return sortOnDependency
                ? copyTables.TopologicalSort(left => copyTables.Where(right => left != right && right.Keys.Any(key => key.ForeignTable == left.Name)), false).ToList()
                : copyTables;
        }

        private sealed class SqlQueryBuilder
        {
            private readonly StringBuilder _stringBuilder;

            private bool _appendWhere;

            private SqlQueryBuilder(string query)
            {
                _stringBuilder = new StringBuilder(query);
            }

            public static SqlQueryBuilder FromString(string query)
            {
                return new SqlQueryBuilder(query);
            }

            public override string ToString()
            {
                return _stringBuilder.ToString();
            }

            public SqlQueryBuilder Where(bool predicate, string value, string key)
            {
                return predicate ? Where(value, key) : this;
            }

            public SqlQueryBuilder WhereEither(bool predicate, string value, params string[] keys)
            {
                return predicate ? WhereEither(value, keys) : this;
            }

            public SqlQueryBuilder Where(string value, string key)
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

            public SqlQueryBuilder WhereEither(string value, params string[] keys)
            {
                if (string.IsNullOrEmpty(value))
                    return this;

                var clause = "";
                for (var i = 0; i < keys.Length; ++i)
                {
                    clause += $"{keys[i]}='{value}'";

                    if (i < keys.Length - 1)
                        clause += " OR ";
                }

                _stringBuilder.Append(_appendWhere
                    ? $"\r\rAND ({clause})"
                    : $"\r\nWHERE ({clause})"
                );

                _appendWhere = true;
                return this;
            }

            public SqlQueryBuilder WhereIn(IEnumerable<object> values, string key)
            {
                if (values == null)
                    throw new ArgumentNullException(nameof(values));

                var cached = values.ToArray();
                if (!cached.Any())
                    return this;

                var clause = cached.Aggregate((acc, cur) => $"{acc}, {cur}");

                _stringBuilder.Append(_appendWhere
                    ? $"\r\rAND {key}='({clause})'"
                    : $"\r\nWHERE {key}='({clause})'"
                );

                _appendWhere = true;
                return this;
            }
        }
    }
}
