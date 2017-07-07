using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using tableshot.Models;

namespace tableshot
{
    public sealed partial class DatabaseCloner
    {
        private readonly DatabaseConnection _sourceConnection;
        private readonly DatabaseConnection _targetConnection;

        public DatabaseCloner(DatabaseConnection connection) : this(connection, connection)
        {
            
        }

        public DatabaseCloner(DatabaseConnection sourceConnection, DatabaseConnection targetConnection)
        {
            _sourceConnection = sourceConnection;
            _targetConnection = targetConnection;
        }

        public async Task CloneAsync(DatabaseCloneOptions options)
        {
            if (_sourceConnection == _targetConnection && options.SourceSchema == options.TargetSchema)
                throw new InvalidOperationException("Target and Source schema can't be the same for the same database");
            if ((!string.IsNullOrEmpty(options.SourceSchema) && string.IsNullOrEmpty(options.TargetSchema)) ||
                (string.IsNullOrEmpty(options.SourceSchema) && !string.IsNullOrEmpty(options.TargetSchema)))
                throw new InvalidOperationException("Both or none Schema names should be given but not one.");

            var sourceManager = new DatabaseManager(_sourceConnection, options.SourceSchema);
            var targetManager = new DatabaseManager(_targetConnection, options.TargetSchema);

            var tables = (await Task.WhenAll(options.Tables.Select(async table =>
                table.ReferencedBy == ReferencedByOptions.Disabled
                    ? new[] {table.Table} as IList<ShallowTable>
                    : await sourceManager.QueryTablesReferencedByAsync(table.Table, table.ReferencedBy)
            ))).SelectMany(s => s).ToArray();
            
            // cache the schemas 
            var targetSchemas = await DatabaseManager.GetSchemasAsync(_targetConnection);
            var schemas = tables.Select(s => s.SchemaName)
                .Distinct(StringComparer.CurrentCultureIgnoreCase)
                .Where(schema => schema != options.SourceSchema)
                .ToList();

            if (!string.IsNullOrEmpty(options.TargetSchema))
                 schemas.Add(options.TargetSchema);

            foreach (var schema in schemas)
            {
                if (targetSchemas.Contains(schema))
                    continue;

                if (options.CreateMissingSchemas)
                    await targetManager.CreateSchemaAsync(schema);
                else
                    throw new InvalidOperationException("Schema doesn't exist, enable 'CreatingMissingSchemas' to create schema");
            }

            bool SkipShared(ShallowTable table) => options.SkipSharedTables && _targetConnection == _sourceConnection && table.SchemaName != sourceManager.SchemaName;
            string TargetSchemaName(ShallowTable table) => sourceManager == targetManager ? table.SchemaName : (options.TargetSchema ?? table.SchemaName);
            
            // drop the target tables (if exists)
            foreach (var table in tables.Reverse())
            {
                // same database -> don't drop shared tables
                if (SkipShared(table))
                    continue;

                await targetManager.DropTableAsync(table.Name, TargetSchemaName(table), options.CheckReferencedTables);
            }

            // copy the data from source to target
            foreach (var table in tables)
            {
                // same database -> don't copy shared tables
                if (SkipShared(table) && await targetManager.QueryTableExistsAsync(table))
                    continue;

                var fullTable = await sourceManager.QueryTableAsync(table);
                var query = await sourceManager.CloneTableSqlAsync(fullTable);

                // replace source schema with this one
                if (table.SchemaName == options.SourceSchema && !string.IsNullOrEmpty(options.TargetSchema))
                    query = query.Replace(options.SourceSchema, options.TargetSchema);

                await targetManager.Connection.ExecuteNonQueryAsync(query);
            }
        }
    }
}
