namespace TableSnapper.Models
{
    internal class ShallowTable
    {
        public string SchemaName { get; }
        public string Name { get; }

        public ShallowTable(string schemaName, string name)
        {
            SchemaName = schemaName;
            Name = name;
        }

        public override string ToString() => $"{SchemaName}.{Name}";

        protected bool Equals(ShallowTable other) => string.Equals(Name, other.Name) && string.Equals(SchemaName, other.SchemaName);

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != this.GetType())
                return false;

            return Equals((ShallowTable) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Name != null ? Name.GetHashCode() : 0) * 397) ^ (SchemaName != null ? SchemaName.GetHashCode() : 0);
            }
        }

        public static bool operator ==(ShallowTable left, ShallowTable right) => Equals(left, right);

        public static bool operator !=(ShallowTable left, ShallowTable right) => !Equals(left, right);
    }
}
