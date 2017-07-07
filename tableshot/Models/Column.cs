using System;

namespace tableshot.Models
{
    public sealed class Column
    {
        public Column(string tableName, string name, int position, object defaultValue, bool isNullable, string dataTypeName, int? characterMaximumLength, int? numericPrecision, int? numericScale, bool isIdentity)
        {
            TableName = tableName;
            Name = name;
            Position = position;
            DefaultValue = defaultValue;
            IsNullable = isNullable;
            DataTypeName = dataTypeName;
            CharacterMaximumLength = characterMaximumLength;
            NumericPrecision = numericPrecision;
            NumericScale = numericScale;
            IsIdentity = isIdentity;
        }

        public string TableName { get; }
        public string Name { get; }
        public int Position { get; }
        public object DefaultValue { get; }
        public bool IsNullable { get; }
        public string DataTypeName { get; }
        public int? CharacterMaximumLength { get; }
        public int? NumericPrecision { get; }
        public int? NumericScale { get; }
        public bool IsIdentity { get; }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;

            return Equals((Column) obj);
        }

        public bool Equals(Column other)
        {
            return CharacterMaximumLength == other.CharacterMaximumLength &&
                   string.Equals(DataTypeName, other.DataTypeName, StringComparison.CurrentCultureIgnoreCase) &&
                   Equals(DefaultValue, other.DefaultValue) &&
                   IsIdentity == other.IsIdentity &&
                   IsNullable == other.IsNullable &&
                   string.Equals(Name, other.Name, StringComparison.CurrentCultureIgnoreCase) &&
                   NumericPrecision == other.NumericPrecision &&
                   NumericScale == other.NumericScale &&
                   Position == other.Position &&
                   string.Equals(TableName, other.TableName, StringComparison.CurrentCultureIgnoreCase);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = CharacterMaximumLength.GetHashCode();
                hashCode = (hashCode * 397) ^ (DataTypeName?.ToLower()?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (DefaultValue != null ? DefaultValue.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ IsIdentity.GetHashCode();
                hashCode = (hashCode * 397) ^ IsNullable.GetHashCode();
                hashCode = (hashCode * 397) ^ (Name?.ToLower()?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ NumericPrecision.GetHashCode();
                hashCode = (hashCode * 397) ^ NumericScale.GetHashCode();
                hashCode = (hashCode * 397) ^ Position;
                hashCode = (hashCode * 397) ^ (TableName?.ToLower()?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        public static bool operator ==(Column left, Column right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Column left, Column right)
        {
            return !Equals(left, right);
        }

        public override string ToString()
        {
            return $"{TableName}:{Name} ({DataTypeName})";
        }
    }
}
