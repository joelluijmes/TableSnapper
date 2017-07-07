using System;

namespace tableshot.Models
{
    public class ShallowTable
    {
        public ShallowTable(string schemaName, string name)
        {
            SchemaName = schemaName;
            Name = name;
        }

        public string SchemaName { get; }
        public string Name { get; }

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
                return ((Name != null ? StringComparer.CurrentCultureIgnoreCase.GetHashCode(Name) : 0) * 397) ^ (SchemaName != null ? StringComparer.CurrentCultureIgnoreCase.GetHashCode(SchemaName) : 0);
            }
        }

        public static bool operator ==(ShallowTable left, ShallowTable right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(ShallowTable left, ShallowTable right)
        {
            return !Equals(left, right);
        }

        public override string ToString()
        {
            return $"{SchemaName}.{Name}";
        }

        protected bool Equals(ShallowTable other)
        {
            return string.Equals(Name, other.Name, StringComparison.CurrentCultureIgnoreCase) && string.Equals(SchemaName, other.SchemaName, StringComparison.CurrentCultureIgnoreCase);
        }
    }
}
