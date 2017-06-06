using System.Collections.Generic;
using TableSnapper.Models;

namespace TableSnapper
{
    internal sealed partial class DatabaseCloner
    {
        public sealed class DatabaseCloneOptions
        {
            public DatabaseCloneOptions(IList<ShallowTable> tables)
            {
                Tables = tables;
            }

            public DatabaseCloneOptions(IList<ShallowTable> tables, string targetSchema, string sourceSchema)
            {
                Tables = tables;
                TargetSchema = targetSchema;
                SourceSchema = sourceSchema;
            }

            public IList<ShallowTable> Tables { get; }
            public string TargetSchema { get; }
            public string SourceSchema { get; }
            public bool CreateMissingSchemas { get; set; } = true;
            public bool CheckReferencedTables { get; set; } = true;
            public bool SkipSharedTables { get; set; } = true;
        }
    }
}
