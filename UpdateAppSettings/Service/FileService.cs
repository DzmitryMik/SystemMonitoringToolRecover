namespace UpdateAppSettings.Service
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text.RegularExpressions;
    using System.Web;
    using System.Xml;
    using System.Xml.Serialization;
    using Newtonsoft.Json;
    using UpdateAppSettings.Models;

    public class FileService
    {
        public List<AzureConfiguration> GetAzureConfigurations(string completeFilePath)
        {
            string text = File.ReadAllText(completeFilePath);
            List<AzureConfiguration> azureConfigurations = new List<AzureConfiguration>();
            try
            {
                azureConfigurations = JsonConvert.DeserializeObject<List<AzureConfiguration>>(text);
            }
            catch(Exception)
            {
                Debugger.Break();
                throw;
            }
            return azureConfigurations;
        }

        public void GetConfig(string completeFilePath)
        {
            string text = File.ReadAllText(completeFilePath);
            try
            {
                XmlSerializer serializer = new XmlSerializer(typeof(object));
                TextReader tr = new StringReader(text);
                //XmlReader reader = XmlReader.Create(tr);
                var obj = serializer.Deserialize(tr);
            }
            catch (Exception ex)
            {
                Debugger.Break();
                throw ex;
            }
        }

        public void InjectConfigs(string completeFilePath, List<AzureConfiguration> azureConfigurations)
        {
            string tempLineValue;
            string output = "";
            Regex regexIsCorrectLine = new Regex("(<add).+(key=).+(value=).+(/>)");
            Regex regexQuotedWords = new Regex("([\"'])(?:(?=(\\\\?))\\2.)*?\\1");
            using (FileStream inputStream = File.OpenRead(completeFilePath))
            {
                using (StreamReader inputReader = new StreamReader(inputStream))
                {
                    while (null != (tempLineValue = inputReader.ReadLine()))
                    {
                        if (regexIsCorrectLine.IsMatch(tempLineValue))
                        {
                            var result = regexQuotedWords.Matches(tempLineValue);
                            if(result.Count == 2)
                            {
                                string key = result[0].Value.Replace("\"", "");
                                var azureConfig = azureConfigurations?.Where(a => a.Name == key).FirstOrDefault();
                                if(azureConfig?.Value != null)
                                {
                                    output += $"<add key=\"{HttpUtility.HtmlEncode(key)}\" value=\"{HttpUtility.HtmlEncode(azureConfig.Value)}\" />\n";
                                }
                                else
                                    output += tempLineValue + "\n";
                            }
                            else
                                output += tempLineValue + "\n";
                        }
                        else
                            output += tempLineValue + "\n";
                    }
                }
            }

            File.WriteAllText(completeFilePath, output);
        }
    }
}
