using System;

namespace tableshot.Exceptions
{
    internal sealed class DatabaseException : Exception
    {
        public string Query { get; }

        public DatabaseException(string query, Exception innerException) : base($"Exception executing: \r\n{query}\r\n{innerException.Message}", innerException)
        {
            Query = query;
        }
    }
}
