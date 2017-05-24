namespace TableSnapper.Models {
    internal class Key
    {
        public string Column { get; }
        public string KeyName { get; }
        public string ForeignTable { get; }
        public string ForeignColumn { get; }
        public bool IsPrimaryKey { get; }
        public bool IsForeignKey { get; }

        public Key(string column, string keyName)
        {
            Column = column;
            KeyName = keyName;
            IsPrimaryKey = true;
        }

        public Key(string column, string keyName, string foreignTable, string foreignColumn)
        {
            Column = column;
            KeyName = keyName;
            ForeignTable = foreignTable;
            ForeignColumn = foreignColumn;
            IsForeignKey = true;
        }
    }
}