namespace TableSnapper.Models
{
    internal class Column
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

        public override string ToString() => $"{TableName}:{Name} ({DataTypeName})";
        
    }
}
