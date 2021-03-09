using System;
using System.Collections.Generic;
using System.Diagnostics;
using UpdateAppSettings.Models;
using UpdateAppSettings.Service;

namespace UpdateAppSettings
{
    static class Program
    {
        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("Insert complete path to Json file holding Azure Configs");
                var completeAzureConfigFilePath = Console.ReadLine();

                var fileService = new FileService();
                var azureConfigurations = fileService.GetAzureConfigurations(completeAzureConfigFilePath);

                Console.WriteLine("Insert complete path to .config file");
                var completeConfigFilePath = Console.ReadLine();
                fileService.InjectConfigs(completeConfigFilePath, azureConfigurations);
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }
    }
}
