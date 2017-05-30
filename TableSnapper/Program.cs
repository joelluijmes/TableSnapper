using System;
using System.IO;
using System.Threading.Tasks;
using Nito.AsyncEx;

namespace TableSnapper
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            AsyncContext.Run(MainImpl);
        }

        private static async Task MainImpl()
        {
            var repoA = await Repository.OpenDatabaseAsync("localhost", "TestA");
            var repoB = await Repository.OpenDatabaseAsync("localhost", "TestB");

            var tablesA = await repoA.ListTablesAsync();


            foreach (var table in tablesA)
            {
                var content = await repoA.CloneTableSqlAsync(table);
                File.WriteAllText($"{table.Name}.sql", content);
            }
        }
    }
}
