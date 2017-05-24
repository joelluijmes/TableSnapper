using System;
using System.Text;

namespace TableSnapper.Models {
    internal class Column
    {
        public string Name { get; }
        public int Position { get; }
        public object DefaultValue { get; }
        public bool IsNullable { get; }
        public string DataTypeName { get; }
        public int? CharacterMaximumLength { get; }
        public int? NumericPrecision { get; }
        public int? NumericScale { get; }
        public bool IsIdentity { get; }

        public Column(string name, int position, object defaultValue, bool isNullable, string dataTypeName, int? characterMaximumLength, int? numericPrecision, int? numericScale, bool isIdentity)
        {
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

        public override string ToString()
        {
            var builder = new StringBuilder();
            builder.Append($"{Name} {DataTypeName} ");
            if (CharacterMaximumLength.HasValue)
                builder.Append($"({CharacterMaximumLength}) ");

            if (DataTypeName == "decimal")
            {
                if (NumericPrecision.HasValue && !NumericScale.HasValue)
                    builder.Append($"({NumericPrecision}) ");
                else if (NumericPrecision.HasValue && NumericScale.HasValue)
                    builder.Append($"({NumericPrecision}, {NumericScale}) ");
                else
                    throw new InvalidOperationException("Unable to parse decimal");
            }

            if (IsIdentity)
                builder.Append("IDENTITY ");

            if (!IsNullable)
                builder.Append("NOT NULL ");

            if (DefaultValue != null)
                builder.Append($"DEFAULT({DefaultValue}) ");

            return builder.ToString().Trim();
        }
    }
}