using System;
using System.Collections.Generic;
using TableSnapper.Models;

namespace TableSnapper
{
    internal sealed partial class DatabaseManager
    {
        public class CloneOptions
        {
            public DatabaseManager OtherDatabase { get; }

            public CloneOptions(DatabaseManager otherDatabase)
            {
                OtherDatabase = otherDatabase;
            }

            public IList<Table> Tables { get; set; }
            public bool ResolveReferencedTables { get; set; }
            public bool SkipData { get; set; }
            public bool OnlyOwnedTables { get; set; }
            public string Schema { get; set; }
        }
    }
}
