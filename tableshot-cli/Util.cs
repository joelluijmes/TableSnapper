using System;
using tableshot.Models;

namespace tableshot
{
    internal static class Util
    {
        public static TableConfiguration ParseCloneTable(string cloneTableString)
        {
            var splitted = cloneTableString.Split(':');
            //if (splitted.Length != 2)
            //    throw new ArgumentException("Invalid format: [Schema].Table:<none|schema|full>");

            var table = ParseTableName(splitted[0]);
            var referencedBy = splitted.Length == 2
                ? ParseReferencedByOptions(splitted[1])
                : ReferencedByOptions.Descending;

            return new TableConfiguration
            {
                Table = table,
                ReferencedBy = referencedBy
            };
        }

        public static ServerCredentials ParseCredentials(string credentialString)
        {
            var serverCredentials = new ServerCredentials();

            var splitted = credentialString.Split('@');
            if (splitted.Length == 2)
            {
                var userPass = splitted[0];
                var serverDatabase = splitted[1];

                ParseUsernamePassword(serverCredentials, userPass);
                ParseServerDatabase(serverCredentials, serverDatabase);
            }
            else if (splitted.Length == 1)
                ParseServerDatabase(serverCredentials, credentialString);
            else
                throw new ArgumentException("Invalid credential format: <username>:<pass>@<server>:<database>", nameof(credentialString));

            return serverCredentials;
        }

        public static ReferencedByOptions ParseReferencedByOptions(string value)
        {
            switch (value)
            {
            case "schema":
            case "schema-only":
                return ReferencedByOptions.Schema;
            case "schema-ascend":
                return ReferencedByOptions.Schema | ReferencedByOptions.Ascending;
            case "schema-descend":
                return ReferencedByOptions.Schema | ReferencedByOptions.Descending;

            case "descend":
            case "descending":
            case "full-descend":
                return ReferencedByOptions.Descending;

            case "ascend":
            case "ascending":
            case "full-ascend":
                return ReferencedByOptions.Ascending;

                default:
                return ReferencedByOptions.Disabled;
            }
        }

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

        private static void ParseServerDatabase(ServerCredentials serverCredentials, string serverDatabase)
        {
            var endpoint = serverDatabase.Split(':');

            if (endpoint.Length == 2)
            {
                serverCredentials.Server = endpoint[0];
                serverCredentials.Database = endpoint[1];
            }
            else if (endpoint.Length == 1)
                serverCredentials.Database = endpoint[0];
            else
                throw new ArgumentException("Invalid credential format: <server>:<database>", nameof(serverDatabase));
        }

        private static void ParseUsernamePassword(ServerCredentials serverCredentials, string userPass)
        {
            var credentials = userPass.Split(':');
            switch (credentials.Length)
            {
            case 2:
                serverCredentials.Username = credentials[0];
                serverCredentials.Password = credentials[1];
                break;
            case 0:
                if (string.IsNullOrEmpty(userPass))
                    serverCredentials.Username = userPass;
                break;
            default:
                throw new ArgumentException("Invalid credential format: <username>:<pass>", nameof(userPass));
            }
        }
    }
}
