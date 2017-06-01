using System.Collections.Generic;

namespace TableSnapper.Models
{
    internal sealed class Schema
    {
        public Schema(string name)
        {
            Name = name;
        }

        public Schema(string name, List<string> tables)
        {
            Name = name;
            Tables = tables;
        }

        public string Name { get; }
        public List<string> Tables { get; }
    }
}
