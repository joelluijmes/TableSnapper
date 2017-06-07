using System.Collections.Generic;

namespace tableshot.Models
{
    public sealed class Schema
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
