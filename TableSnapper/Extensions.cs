using System.Data.SqlClient;

namespace TableSnapper
{
    internal static class Extensions
    {
        public static int? GetNullableInt(this SqlDataReader dataReader, string column)
        {
            var ordinal = dataReader.GetOrdinal(column);
            return dataReader.IsDBNull(ordinal) ? (int?) null : int.Parse(dataReader[ordinal].ToString());
        }
    }
}
