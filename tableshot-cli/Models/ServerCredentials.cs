using System.Data.SqlClient;
using Newtonsoft.Json;

namespace tableshot.Models
{
    internal sealed class ServerCredentials
    {
        [JsonProperty("server")]
        public string Server { get; set; }

        [JsonProperty("database")]
        public string Database { get; set; }

        [JsonProperty("username")]
        public string Username { get; set; }

        [JsonProperty("password")]
        public string Password { get; set; }

        public SqlConnectionStringBuilder ToConnectionStringBuilder()
        {
            var builder = new SqlConnectionStringBuilder()
            {
                InitialCatalog = Database,
                DataSource = Server
            };

            if (Username == null && Password == null)
            {
                builder.IntegratedSecurity = true;
            }
            else
            {
                builder.UserID = Username;
                builder.Password = Password;
            }

            return builder;
        }
    }
}
