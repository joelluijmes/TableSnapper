using System.Collections.Generic;

namespace TableSnapper.Models
{
    internal sealed class Table
    {
        public Table(string name, List<Column> columns, List<Key> keys, List<Constraint> constraints)
        {
            Name = name;
            Columns = columns;
            Keys = keys;
            Constraints = constraints;
        }

        public string Name { get; }
        public List<Column> Columns { get; }
        public List<Key> Keys { get; }
        public List<Constraint> Constraints { get; }

        public override string ToString() => Name;
    }
}
