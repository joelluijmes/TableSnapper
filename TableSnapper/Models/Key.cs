namespace TableSnapper.Models {
    internal class Key
    {
        public string TableName { get; }
        public string Column { get; }
        public string KeyName { get; }
        public string ForeignTable { get; }
        public string ForeignColumn { get; }
        public bool IsPrimaryKey { get; }
        public bool IsForeignKey { get; }

        public Key(string tableName, string column, string keyName)
        {
            TableName = tableName;
            Column = column;
            KeyName = keyName;
            IsPrimaryKey = true;
        }

        public Key(string tableName, string column, string keyName, string foreignTable, string foreignColumn)
        {
            TableName = tableName;
            Column = column;
            KeyName = keyName;
            ForeignTable = foreignTable;
            ForeignColumn = foreignColumn;
            IsForeignKey = true;
        }
    }
}