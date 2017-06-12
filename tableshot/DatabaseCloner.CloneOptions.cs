using System.Collections.Generic;
using System.Linq;
using tableshot.Models;

namespace tableshot
{
    public sealed partial class DatabaseCloner
    {
        public sealed class DatabaseCloneOptions
        {
            public DatabaseCloneOptions(IEnumerable<ShallowTable> tables)
            {
                Tables = tables.ToList();
            }

            public DatabaseCloneOptions(IEnumerable<ShallowTable> tables, string targetSchema, string sourceSchema)
            {
                Tables = tables.ToList();
                TargetSchema = targetSchema;
                SourceSchema = sourceSchema;
            }

            public IList<ShallowTable> Tables { get; }
            public string TargetSchema { get; }
            public string SourceSchema { get; }
            public bool CreateMissingSchemas { get; set; } = true;
            public ReferencedByOptions ReferencedBy { get; set; } = ReferencedByOptions.FullDescend;
            public bool SkipSharedTables { get; set; } = true;
        }
    }
}
