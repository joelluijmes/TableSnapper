using System;
using tableshot.Models;

namespace tableshot
{
    internal static class Util
    {
        public static ShallowTable ParseTableName(string tableName)
        {
            ShallowTable shallowTable;
            var splitted = tableName.Split('.');
            switch (splitted.Length)
            {
            case 0:
                shallowTable = new ShallowTable(null, tableName);
                break;
            case 2:
                shallowTable = new ShallowTable(splitted[0], splitted[1]);
                break;
            default:
                throw new InvalidOperationException();
            }

            return shallowTable;
        }
    }
}
