using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using tableshot.Models;

namespace tableshot
{
    public sealed class DatabaseCloneManager
    {
        private static readonly ILogger _logger = ApplicationLogging.CreateLogger<DatabaseCloneManager>();
        private readonly DatabaseManager _manager;

        public DatabaseCloneManager(DatabaseManager manager)
        {
            _manager = manager;
        }

        public async Task BackupToDirectoryAsync(string directory, string schemaName, bool splitPerTable = true, bool skipData = false)
        {
            var tables = await _manager.ListTablesAsync(null, schemaName);
            await BackupToDirectoryAsync(directory, tables, splitPerTable, skipData);
        }

        public async Task BackupToDirectoryAsync(string directory, IList<Table> tables, bool splitPerTable = true, bool skipData = false)
        {
            if (splitPerTable)
            {
                for (var i = 0; i < tables.Count; i++)
                {
                    var table = tables[i];

                    var path = Path.Combine(directory, $"{i + 1}_{table.SchemaName}_{table.Name}.sql");
                    await BackupToFileAsync(path, table, skipData);
                }
            }
            else
            {
                var path = Path.Combine(directory, "0_backup.sql");
                await BackupToFileAsync(path, tables, skipData);
            }
        }

        public async Task BackupToFileAsync(string path, Table table, bool skipData = false)
        {
            var clone = skipData
                ? CloneTableStructureSql(table)
                : await CloneTableSqlAsync(table);

            File.WriteAllText(path, clone);
        }

        public async Task BackupToFileAsync(string path, IList<Table> tables, bool skipData = false)
        {
            for (var i = 0; i < tables.Count; i++)
            {
                var table = tables[i];
                var clone = skipData
                    ? CloneTableStructureSql(table)
                    : await CloneTableSqlAsync(table);

                if (i == 0)
                    File.WriteAllText(path, clone);
                else
                    File.AppendAllText(path, clone);
            }
        }

        public async Task CloneFromDirectoryAsync(string directory, string schemaName)
        {
            var files = Directory.GetFiles(directory).OrderBy(f => f).ToArray();
            if (files.Any(f => !Regex.IsMatch(f, "\\d+_.*\\.sql")))
                throw new InvalidOperationException("Directory contains one or more invalid files to import");

            foreach (var file in files)
            {
                var content = File.ReadAllText(file);

                // content = content.Replace("[SCHEMA_NAME]", $"{schemaName ?? _schemaName}");
                await _manager.Connection.ExecuteNonQueryAsync(content);
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

            await _manager.Connection.ExecuteQueryReaderAsync($"SELECT * FROM {table.SchemaName}.{table.Name}", reader =>
            {
                builder.Append($"INSERT {table.SchemaName}.{table.Name} (");
                builder.Append(table.Columns.Select(c => c.Name).Aggregate((a, b) => $"{a}, {b}"));
                builder.Append(") VALUES (");

                for (var i = 0; i < reader.FieldCount; ++i)
                {
                    if (reader.IsDBNull(i))
                        builder.Append("NULL");
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
                            var value = reader[i].ToString().Replace("'", "''");
                            builder.Append($"'{value}'");
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
    }
}
