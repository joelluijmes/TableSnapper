using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TableSnapper.Models
{
    internal sealed class Table
    {
        public Table(string name, List<Column> columns, List<Key> keys, List<Constraint> constraints)
        {
            Name = name;
            Columns = columns;
            Keys = keys;
            Constraints = constraints;
        }

        public string CreateTableSql()
        {
            var builder = new StringBuilder();
            builder.AppendLine($"CREATE TABLE {Name}(");

            var primaryKey = Keys.SingleOrDefault(key => key.IsPrimaryKey);
            var foreignKeys = Keys.Where(key => key.IsForeignKey).ToList();

            for (var i = 0; i < Columns.Count; i++)
            {
                var column = Columns[i];
                builder.Append($"  {column}");

                if (primaryKey != null && primaryKey.Column == column.Name)
                    builder.Append(" PRIMARY KEY");

                var foreignKey = foreignKeys.SingleOrDefault(key => key.Column == column.Name);
                if (foreignKey != null)
                    builder.Append($" REFERENCES {foreignKey.ForeignTable}({foreignKey.ForeignColumn})");

                // add the , if not last column
                builder.AppendLine(i < Columns.Count - 1 ? "," : "");
            }
            
            builder.AppendLine(");");
            return builder.ToString();
        }

        public override string ToString() => Name;

        public string Name { get; }
        public List<Column> Columns { get; }
        public List<Key> Keys { get; }
        public List<Constraint> Constraints { get; }
    }
}
