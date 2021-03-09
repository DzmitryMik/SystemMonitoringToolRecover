namespace UpdateAppSettings.Models
{
    using Newtonsoft.Json;

    public class AzureConfiguration
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("value")]
        public string Value { get; set; }

        [JsonProperty("slotSetting")]
        public string SlotSetting { get; set; }
    }
}
