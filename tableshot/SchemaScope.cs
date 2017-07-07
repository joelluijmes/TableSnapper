using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace tableshot
{
    public sealed class SchemaScope : IEnumerable<string>
    {
        private readonly HashSet<string> _schemas;

        public SchemaScope(IEnumerable<string> schemas) : this(schemas.ToArray())
        { }

        public SchemaScope(IList<string> schemas)
        {
            if (schemas != null && schemas.Any())
                _schemas = new HashSet<string>(schemas.Select(s => s.ToLower()));
        }

        public bool Contains(string schema)
        {
            return _schemas == null || _schemas.Contains(schema.ToLower());
        }

        public static SchemaScope All { get; } = new SchemaScope(null);

        public IEnumerator<string> GetEnumerator()
        {
            return _schemas.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
