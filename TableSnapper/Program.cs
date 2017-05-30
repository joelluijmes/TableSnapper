using System;
using System.IO;
using System.Linq;
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

            var tablesA = (await repoA.ListTablesAsync()).ToArray();

            var directory = Path.Combine(Environment.GetEnvironmentVariable("LocalAppData"), "TableSnapper");
            Directory.CreateDirectory(directory);

            foreach (var file in new DirectoryInfo(directory).GetFiles())
                file.Delete();

            for (var i = 0; i < tablesA.Length; i++)
            {
                var table = tablesA[i];
                var content = await repoA.CloneTableSqlAsync(table);

                var path = Path.Combine(directory, $"{i}_{table.Name}.sql");
                File.WriteAllText(path, content);
            }
        }
    }
}
