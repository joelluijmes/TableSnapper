using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace TableSnapper
{
    internal static class Extensions
    {
        public static int? GetNullableInt(this SqlDataReader dataReader, string column)
        {
            var ordinal = dataReader.GetOrdinal(column);
            return dataReader.IsDBNull(ordinal) ? (int?) null : int.Parse(dataReader[ordinal].ToString());
        }

        public static IEnumerable<T> TopologicalSort<T>(this IEnumerable<T> source, Func<T, IEnumerable<T>> dependencies, bool throwOnCycle = true)
        {
            var sorted = new List<T>();
            var visited = new HashSet<T>();

            foreach (var item in source)
                Visit(item, visited, sorted, dependencies, throwOnCycle);

            sorted.Reverse();
            return sorted;
        }

        private static void Visit<T>(T item, ISet<T> visited, ICollection<T> sorted, Func<T, IEnumerable<T>> dependencies, bool throwOnCycle)
        {
            if (!visited.Contains(item))
            {
                visited.Add(item);

                foreach (var dep in dependencies(item))
                    Visit(dep, visited, sorted, dependencies, throwOnCycle);

                sorted.Add(item);
            }
            else
            {
                if (throwOnCycle && !sorted.Contains(item))
                    throw new Exception("Cyclic dependency found");
            }
        }
    }
}
