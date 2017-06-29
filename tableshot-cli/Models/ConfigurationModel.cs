using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace tableshot.Models
{
    internal sealed class ConfigurationModel
    {
        [JsonProperty("source")]
        public ServerCredentials SourceCredentials { get; set; }

        [JsonProperty("target")]
        public ServerCredentials TargetCredentials { get; set; }
        
        [JsonProperty("tables")]
        [JsonConverter(typeof(TablesConverter))]
        public IList<CloneTable> Tables { get; set; }

        [JsonProperty("columns")]
        public IList<string> Columns { get; set; }

        [JsonProperty("schema")]
        public string Schema { get; set; }

        private class TablesConverter : JsonConverter
        {
            public override bool CanWrite => false;

            public override bool CanConvert(Type objectType) => false;

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                var jsonArray = JArray.Load(reader);

                return jsonArray.Select(jsonTable => new CloneTable
                {
                    Table = Util.ParseTableName(jsonTable["name"].ToString()),
                    ReferencedBy = Util.ParseReferencedByOptions(jsonTable["referenced"].ToString())
                }).ToList();
            }

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                throw new NotImplementedException();
            }
        }
    }
}
