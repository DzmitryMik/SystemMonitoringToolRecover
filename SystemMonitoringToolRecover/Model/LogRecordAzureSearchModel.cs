using System;
using Newtonsoft.Json;

namespace BlobStorageToAzureSearchPush.Model
{
    public class LogRecordAzureSearchModel
    {
        [JsonProperty("id", Required = Required.Always)]
        public string id { get; set; }

        [JsonProperty("eventCode", Required = Required.Always)]
        [SimpleField]
        public string eventCode { get; set; }
        
        [JsonProperty("eventCodeId", Required = Required.Always)]
        public int eventCodeId { get; set; }

        [JsonProperty("logRecordTypeId", Required = Required.Always)]
        public int logRecordTypeId { get; set; }
        
        [JsonProperty("logRecordTypeName", Required = Required.Always)]
        public string logRecordTypeName { get; set; }
        
        [JsonProperty("systemTypeName", Required = Required.Always)]
        public string systemTypeName { get; set; }
        
        [JsonProperty("severityTypeId", Required = Required.Always)]
        public int severityTypeId { get; set; }

        [JsonProperty("severityTypeName", Required = Required.Always)]
        public string severityTypeName { get; set; }

        [JsonProperty("eventData", Required = Required.AllowNull, NullValueHandling = NullValueHandling.Include)]
        public string eventData { get; set; }

        [JsonProperty("storeId", Required = Required.AllowNull, NullValueHandling = NullValueHandling.Include)]
        public Guid? storeId { get; set; }

        [JsonProperty("tenantId", Required = Required.AllowNull, NullValueHandling = NullValueHandling.Include)]
        public Guid? tenantId { get; set; }

        [JsonProperty("tenantName", Required = Required.AllowNull, NullValueHandling = NullValueHandling.Include)]
        public string tenantName { get; set; }
        
        [JsonProperty("storeNumber", Required = Required.AllowNull, NullValueHandling = NullValueHandling.Include)]
        public string storeNumber { get; set; }

        [JsonProperty("sessionId", Required = Required.AllowNull, NullValueHandling = NullValueHandling.Include)]
        public string sessionId { get; set; }

        [JsonProperty("machineId", Required = Required.AllowNull, NullValueHandling = NullValueHandling.Include)]
        public int? machineId { get; set; }

        [JsonProperty("machineName", Required = Required.AllowNull, NullValueHandling = NullValueHandling.Include)]
        public string machineName { get; set; }

        [JsonProperty("createdOn", Required = Required.Always)]
        public DateTime createdOn { get; set; }

        [JsonProperty("expirationTicks", Required = Required.Always)]
        public long expirationTicks { get; set; }

        [JsonProperty("eventDescription", Required = Required.AllowNull, NullValueHandling = NullValueHandling.Include)]
        public string eventDescription { get; set; }
    }
}
