using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TableSnapper.Models;

namespace TableSnapper
{
    internal sealed class DatabaseCloner
    {
        private readonly DatabaseConnection _sourceConnection;
        private readonly DatabaseConnection _targetConnection;

        public DatabaseCloner(DatabaseConnection sourceConnection, DatabaseConnection targetConnection)
        {
            if (sourceConnection == targetConnection)
                throw new InvalidOperationException("DatabaseCloner cannot operate on the same database, use SchemaCloner to clone schema's.");

            _sourceConnection = sourceConnection;
            _targetConnection = targetConnection;
        }

        public async Task CloneDatabaseAsync(CloneOptions options)
        {
            var sourceManager = new DatabaseManager(_sourceConnection);
            var targetManager = new DatabaseManager(_targetConnection);

            var sourceSchemas = await DatabaseManager.GetSchemasAsync(_sourceConnection);
            var targetSchemas = await DatabaseManager.GetSchemasAsync(_targetConnection);

            var schemas = options.Schemas ?? await sourceManager.QuerySchemasAsync();
            
            foreach (var schema in schemas)
            {
                // check if schemas exist, create if target doesn't have it
                if (!sourceSchemas.Contains(schema.Name))
                    throw new InvalidOperationException("Cannot clone non-existing schema");

                if (!targetSchemas.Contains(schema.Name))
                {
                    if (options.CreateMissingSchemas)
                        await targetManager.CreateSchemaAsync(schema.Name);
                    else
                        throw new InvalidOperationException("Schema doesn't exist, enable 'CreatingMissingSchemas' to create schema");
                }

                var schemaCloner = new SchemaCloner(_sourceConnection, _targetConnection);
                var schemaClonerOptions = new SchemaCloner.CloneOptions(schema.Name)
                {
                    Tables = schema.Tables ?? await sourceManager.GetTablesAsync()
                };

                await schemaCloner.CloneSchemaAsync(schemaClonerOptions);
            }
        }

        public sealed class CloneOptions
        {
            public IList<Schema> Schemas { get; set; }
            public bool CreateMissingSchemas { get; set; } = true;
        }
    }
}
