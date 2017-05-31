using System.Collections.Generic;

namespace TableSnapper.Models
{
    internal sealed class Scheme
    {
        public Scheme(string name, List<Table> tables)
        {
            Name = name;
            Tables = tables;
        }

        public string Name { get; }
        public List<Table> Tables { get; }
    }
}
