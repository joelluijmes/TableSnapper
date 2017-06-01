using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using TableSnapper.Models;

namespace TableSnapper
{
    internal sealed class SchemaCloner
    {
        private readonly DatabaseConnection _sourceConnection;
        private readonly DatabaseConnection _targetConnection;

        public SchemaCloner(DatabaseConnection databaseConnection) : this(databaseConnection, databaseConnection)
        { }

        public SchemaCloner(DatabaseConnection sourceConnection, DatabaseConnection targetConnection)
        {
            _sourceConnection = sourceConnection;
            _targetConnection = targetConnection;
        }

        public async Task CloneSchemaAsync(CloneOptions options)
        {
            var sourceManager = new DatabaseManager(_sourceConnection, options.SourceSchema);
            var targetManager = new DatabaseManager(_targetConnection, options.TargetSchema);

            // get tables
            var inputTables = options.Tables ?? await sourceManager.GetTablesAsync();
            IList<Table> sourceTables = await Task.WhenAll(inputTables.Select(async table => await sourceManager.QueryTableAsync(table)));

            if (options.ResolveReferencedTables)
            {
                var temp = new List<Table>();
                temp.AddRange(sourceTables);
                foreach (var table in sourceTables)
                    temp.AddRange(await sourceManager.QueryTablesReferencedByAsync(table));

                sourceTables = temp.Distinct().ToList();
            }

            // be sure to sort them by dependency
            // only use tables owned by the source schema (skip shared tables)
            sourceTables = DatabaseManager.SortTables(sourceTables.Where(table => table.SchemaName == sourceManager.SchemaName));

            // check if the tables exist in the target scheme
            //var targetTables = await Task.WhenAll(sourceTables.Select(async table => await targetManager.QueryTableAsync(table.Name)));
            //if (!sourceTables.SequenceEqual(targetTables, Table.TableStructureComparer))
            //{
            //    throw new InvalidOperationException("The tables in target scheme does not equal the source tables. Enable 'CloneStructure' to overwrite the target tables.");
            //}
            
            // drop the target tables (if exists)
            foreach (var table in sourceTables.Reverse())
                await targetManager.DropTableAsync(table.Name);

            // copy the data from source to target
            foreach (var table in sourceTables)
            {
                var query = await sourceManager.CloneTableSqlAsync(table);

                // replace source schema with this one
                query = query.Replace(sourceManager.SchemaName, targetManager.SchemaName);
                await targetManager.Connection.ExecuteNonQueryAsync(query);
            }
        }

        public class CloneOptions
        {
            public string SourceSchema { get; }
            public string TargetSchema { get; }
            public IList<string> Tables { get; set; }
            public bool ResolveReferencedTables { get; set; }
            public bool CloneStructure { get; set; }
            public bool CloneData { get; set; }

            public CloneOptions(string schema) : this (schema, schema) { }

            public CloneOptions(string sourceSchema, string targetSchema)
            {
                SourceSchema = sourceSchema;
                TargetSchema = targetSchema;
            }
        }
    }
}
