using System.Collections.Generic;
using System.Linq;

namespace tableshot.Models
{
    public sealed class Table : ShallowTable
    {
        public Table(string schemaName, string name, List<Column> columns, List<Key> keys, List<Constraint> constraints) : base(schemaName, name)
        {
            Columns = columns;
            Keys = keys;
            Constraints = constraints;
        }

        public List<Column> Columns { get; }
        public List<Key> Keys { get; }
        public List<Constraint> Constraints { get; }

        public static IEqualityComparer<Table> TableStructureComparer { get; } = new TableStructureEqualityComparer();

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;

            return obj is Table && Equals((Table) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (Columns != null ? Columns.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Constraints != null ? Constraints.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Keys != null ? Keys.GetHashCode() : 0);
                return hashCode;
            }
        }

        public static bool operator ==(Table left, Table right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Table left, Table right)
        {
            return !Equals(left, right);
        }

        private bool Equals(Table other)
        {
            return base.Equals(other) && Equals(Columns, other.Columns) && Equals(Constraints, other.Constraints) && Equals(Keys, other.Keys);
        }


        private sealed class TableStructureEqualityComparer : IEqualityComparer<Table>
        {
            public bool Equals(Table x, Table y)
            {
                if (ReferenceEquals(x, y))
                    return true;
                if (ReferenceEquals(x, null))
                    return false;
                if (ReferenceEquals(y, null))
                    return false;
                if (x.GetType() != y.GetType())
                    return false;

                return string.Equals(x.Name, y.Name) && x.Columns.SequenceEqual(y.Columns) && x.Keys.SequenceEqual(y.Keys, Key.KeyStructureComparer) && Equals(x.Constraints, y.Constraints);
            }

            public int GetHashCode(Table obj)
            {
                unchecked
                {
                    var hashCode = obj.Name != null ? obj.Name.GetHashCode() : 0;
                    hashCode = (hashCode * 397) ^ (obj.Columns != null ? obj.Columns.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (obj.Keys != null ? obj.Keys.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (obj.Constraints != null ? obj.Constraints.GetHashCode() : 0);
                    return hashCode;
                }
            }
        }
    }
}
