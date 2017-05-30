using System.Collections.Generic;

namespace TableSnapper.Models
{
    internal sealed class Scheme
    {
        public string Name { get; }
        public List<Table> Tables { get; }

        public Scheme(string name, List<Table> tables)
        {
            Name = name;
            Tables = tables;
        }
    }
}
