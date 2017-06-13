using Newtonsoft.Json;

namespace tableshot.Models
{
    internal sealed class ConfigurationModel
    {
        [JsonProperty("source")]
        public ServerCredentials SourceCredentials { get; set; }

        [JsonProperty("target")]
        public ServerCredentials TargetCredentials { get; set; }

        [JsonProperty("tables")]
        public CloneTable[] Tables { get; set; }

        [JsonProperty("schema")]
        public string Schema { get; set; }
    }
}
