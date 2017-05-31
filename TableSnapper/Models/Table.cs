using System.Collections.Generic;

namespace TableSnapper.Models
{
    internal sealed class Table
    {
        public Table(string schemaName, string name, List<Column> columns, List<Key> keys, List<Constraint> constraints)
        {
            SchemaName = schemaName;
            Name = name;
            Columns = columns;
            Keys = keys;
            Constraints = constraints;
        }

        public string SchemaName { get; }
        public string Name { get; }
        public List<Column> Columns { get; }
        public List<Key> Keys { get; }
        public List<Constraint> Constraints { get; }

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
                var hashCode = Columns != null ? Columns.GetHashCode() : 0;
                hashCode = (hashCode * 397) ^ (Constraints != null ? Constraints.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Keys != null ? Keys.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (SchemaName != null ? SchemaName.GetHashCode() : 0);
                return hashCode;
            }
        }

        public static bool operator ==(Table left, Table right) => Equals(left, right);

        public static bool operator !=(Table left, Table right) => !Equals(left, right);

        public override string ToString() => $"{SchemaName}.{Name}";

        private bool Equals(Table other) => Equals(Columns, other.Columns) && Equals(Constraints, other.Constraints) && Equals(Keys, other.Keys) && string.Equals(Name, other.Name) && string.Equals(SchemaName, other.SchemaName);
    }
}
