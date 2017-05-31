namespace TableSnapper.Models
{
    internal class Key
    {
        public Key(string schemaName, string tableName, string column, string keyName)
        {
            SchemaName = schemaName;
            TableName = tableName;
            Column = column;
            KeyName = keyName;
            IsPrimaryKey = true;
        }

        public Key(string schemaName, string tableName, string column, string keyName, string foreignSchemaName, string foreignTable, string foreignColumn)
        {
            SchemaName = schemaName;
            TableName = tableName;
            Column = column;
            KeyName = keyName;
            ForeignSchemaName = foreignSchemaName;
            ForeignTable = foreignTable;
            ForeignColumn = foreignColumn;
            IsForeignKey = true;
        }

        public string SchemaName { get; }
        public string TableName { get; }
        public string Column { get; }
        public string KeyName { get; }
        public string ForeignSchemaName { get; }
        public string ForeignTable { get; }
        public string ForeignColumn { get; }
        public bool IsPrimaryKey { get; }
        public bool IsForeignKey { get; }

        public override string ToString() => KeyName;
    }
}
