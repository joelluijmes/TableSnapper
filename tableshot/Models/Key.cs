using System;
using System.Collections.Generic;

namespace tableshot.Models
{
    public sealed class Key
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

        public static IEqualityComparer<Key> KeyStructureComparer { get; } = new KeyStructureEqualityComparer();

        public override string ToString()
        {
            return KeyName;
        }

        private sealed class KeyStructureEqualityComparer : IEqualityComparer<Key>
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

                return string.Equals(x.TableName, y.TableName, StringComparison.CurrentCultureIgnoreCase) &&
                       string.Equals(x.Column, y.Column, StringComparison.CurrentCultureIgnoreCase) &&
                       string.Equals(x.ForeignTable, y.ForeignTable, StringComparison.CurrentCultureIgnoreCase) &&
                       string.Equals(x.ForeignColumn, y.ForeignColumn, StringComparison.CurrentCultureIgnoreCase) &&
                       x.IsPrimaryKey == y.IsPrimaryKey &&
                       x.IsForeignKey == y.IsForeignKey;
            }

            public int GetHashCode(Key obj)
            {
                unchecked
                {
                    var hashCode = obj.TableName?.ToLower()?.GetHashCode() ?? 0;
                    hashCode = (hashCode * 397) ^ (obj.Column?.ToLower()?.GetHashCode() ?? 0);
                    hashCode = (hashCode * 397) ^ (obj.ForeignTable?.ToLower()?.GetHashCode() ?? 0);
                    hashCode = (hashCode * 397) ^ (obj.ForeignColumn?.ToLower()?.GetHashCode() ?? 0);
                    hashCode = (hashCode * 397) ^ obj.IsPrimaryKey.GetHashCode();
                    hashCode = (hashCode * 397) ^ obj.IsForeignKey.GetHashCode();
                    return hashCode;
                }
            }
        }
    }
}
