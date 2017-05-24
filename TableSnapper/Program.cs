using System;
using System.Linq;
using System.Runtime.InteropServices;
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
                Console.WriteLine(table.CreateTableSql());
                Console.WriteLine();
            }

            await repoB.SynchronizeWithAsync(repoA);
        }
    }
}
