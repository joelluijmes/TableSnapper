using System.Collections.Generic;

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

        public static IEqualityComparer<Key> KeyComparer { get; } = new KeyEqualityComparer();

        public override string ToString() => KeyName;

        private sealed class KeyEqualityComparer : IEqualityComparer<Key>
        {
            public bool Equals(Key x, Key y)
            {
                if (ReferenceEquals(x, y))
                    return true;
                if (ReferenceEquals(x, null))
                    return false;
                if (ReferenceEquals(y, null))
                    return false;
                if (x.GetType() != y.GetType())
                    return false;

                return string.Equals(x.SchemaName, y.SchemaName) && string.Equals(x.TableName, y.TableName) && string.Equals(x.Column, y.Column) && string.Equals(x.KeyName, y.KeyName) && string.Equals(x.ForeignSchemaName, y.ForeignSchemaName) && string.Equals(x.ForeignTable, y.ForeignTable) && string.Equals(x.ForeignColumn, y.ForeignColumn) && x.IsPrimaryKey == y.IsPrimaryKey && x.IsForeignKey == y.IsForeignKey;
            }

            public int GetHashCode(Key obj)
            {
                unchecked
                {
                    var hashCode = obj.SchemaName != null ? obj.SchemaName.GetHashCode() : 0;
                    hashCode = (hashCode * 397) ^ (obj.TableName != null ? obj.TableName.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (obj.Column != null ? obj.Column.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (obj.KeyName != null ? obj.KeyName.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (obj.ForeignSchemaName != null ? obj.ForeignSchemaName.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (obj.ForeignTable != null ? obj.ForeignTable.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (obj.ForeignColumn != null ? obj.ForeignColumn.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ obj.IsPrimaryKey.GetHashCode();
                    hashCode = (hashCode * 397) ^ obj.IsForeignKey.GetHashCode();
                    return hashCode;
                }
            }
        }
    }
}
