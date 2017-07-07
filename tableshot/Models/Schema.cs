using System;
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

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;

            return obj is Schema && Equals((Schema) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Name != null ? StringComparer.CurrentCultureIgnoreCase.GetHashCode(Name) : 0) * 397) ^ (Tables != null ? Tables.GetHashCode() : 0);
            }
        }

        public static bool operator ==(Schema left, Schema right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Schema left, Schema right)
        {
            return !Equals(left, right);
        }

        private bool Equals(Schema other)
        {
            return string.Equals(Name, other.Name, StringComparison.CurrentCultureIgnoreCase) && Equals(Tables, other.Tables);
        }
    }
}
