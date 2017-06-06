using System.Collections.Generic;
using TableSnapper.Models;

namespace TableSnapper
{
    internal sealed partial class DatabaseCloner
    {
        public sealed class CloneOptions
        {
            public CloneOptions(IList<ShallowTable> tables)
            {
                Tables = tables;
            }

            public IList<ShallowTable> Tables { get; }
            public bool CreateMissingSchemas { get; set; } = true;
            public bool CheckReferencedTables { get; set; } = true;
        }
    }
}
