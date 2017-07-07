using System;
using Newtonsoft.Json;

namespace tableshot.Models
{
    internal sealed class CloneTableOption
    {
        [JsonProperty("name")]
        [JsonConverter(typeof(TableNameConverter))]
        public ShallowTable Table { get; set; }

        [JsonProperty("referenced")]
        [JsonConverter(typeof(ReferencedByOptionsConverter))]
        public ReferencedByOptions ReferencedBy { get; set; }

        public TableConfiguration ToCloneTable() => new TableConfiguration
        {
            ReferencedBy = ReferencedBy,
            Table = Table
        };
        
        private class TableNameConverter : JsonConverter
        {
            public override bool CanWrite => false;

            public override bool CanConvert(Type objectType) => false;

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer) => Util.ParseTableName(reader.Value.ToString());

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                throw new NotImplementedException();
            }
        }

        private class ReferencedByOptionsConverter : JsonConverter
        {
            public override bool CanWrite => false;

            public override bool CanConvert(Type objectType) => false;

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                var value = reader.Value.ToString();
                return Util.ParseReferencedByOptions(value);
            }
            
            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                throw new NotImplementedException();
            }
        }
    }
}
