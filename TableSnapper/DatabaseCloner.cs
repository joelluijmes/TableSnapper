using System;
using System.Linq;
using System.Threading.Tasks;

namespace TableSnapper
{
    internal sealed partial class DatabaseCloner
    {
        private readonly DatabaseConnection _sourceConnection;
        private readonly DatabaseConnection _targetConnection;

        public DatabaseCloner(DatabaseConnection sourceConnection, DatabaseConnection targetConnection)
        {
            _sourceConnection = sourceConnection;
            _targetConnection = targetConnection;
        }

        public async Task CloneDatabaseAsync(CloneOptions options)
        {
            var sourceManager = new DatabaseManager(_sourceConnection);
            var targetManager = new DatabaseManager(_targetConnection);

            // cache the schemas 
            var targetSchemas = await DatabaseManager.GetSchemasAsync(_targetConnection);

            var tables = options.CheckReferencedTables
                ? await sourceManager.QueryTablesReferencedByAsync(options.Tables)
                : options.Tables;

            var schemas = tables.Select(s => s.SchemaName).Distinct();
            foreach (var schema in schemas)
            {
                // check if schemas exist, create if target doesn't have it
                if (targetSchemas.Contains(schema))
                    continue;

                if (options.CreateMissingSchemas)
                    await targetManager.CreateSchemaAsync(schema);
                else
                    throw new InvalidOperationException("Schema doesn't exist, enable 'CreatingMissingSchemas' to create schema");
            }

            // drop the target tables (if exists)
            foreach (var table in tables.Reverse())
                await targetManager.DropTableAsync(table);

            // copy the data from source to target
            foreach (var table in tables)
            {
                var fullTable = await sourceManager.QueryTableAsync(table);
                var query = await sourceManager.CloneTableSqlAsync(fullTable);

                // replace source schema with this one
                if (!string.IsNullOrEmpty(sourceManager.SchemaName) && !string.IsNullOrEmpty(targetManager.SchemaName))
                    query = query.Replace(sourceManager.SchemaName, targetManager.SchemaName);

                await targetManager.Connection.ExecuteNonQueryAsync(query);
            }
        }
    }
}
