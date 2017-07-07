using System.Collections.Generic;
using System.Linq;
using tableshot.Models;

namespace tableshot
{
    public sealed partial class DatabaseCloner
    {
        public sealed class DatabaseCloneOptions
        {
            public DatabaseCloneOptions(IEnumerable<TableConfiguration> tables)
            {
                Tables = tables.ToList();
            }

            public DatabaseCloneOptions(IEnumerable<TableConfiguration> tables, string targetSchema, string sourceSchema)
            {
                Tables = tables.ToList();
                TargetSchema = targetSchema;
                SourceSchema = sourceSchema;
            }

            public IList<TableConfiguration> Tables { get; }
            public string TargetSchema { get; }
            public string SourceSchema { get; }
            public bool CreateMissingSchemas { get; set; }
            public bool CheckReferencedTables { get; set; }
            public bool SkipSharedTables { get; set; } = true;
        }
    }
}
