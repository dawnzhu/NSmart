using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using DotNet.Standard.Utilities;
using Microsoft.Extensions.Configuration;

namespace DotNet.Standard.NSmart.Utilities
{
    public static class DoConfig
    {
        public static Dictionary<string, DoConfigDbs> Get()
        {
            var builder = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", true).Build();
            var config = builder.GetSection("ConnectionConfigs").GetChildren().ToDictionary(key => key["Name"], value =>
                new DoConfigDbs
                {
                    ConnectionString = value["ConnectionString"],
                    ProviderName = value["ProviderName"],
                    Adds = value.GetSection("Dbs").GetChildren().Select(obj => new DoConfigDb
                    {
                        Name = obj["Name"],
                        ReadConnectionString = obj["ReadConnectionString"] ?? obj["ConnectionString"],
                        WriteConnectionString = obj["WriteConnectionString"] ?? obj["ConnectionString"],
                        ProviderName = obj["ProviderName"]
                    }).ToList()
                });
            return config;
        }
    }

    public class DoConfigDbs
    {
        public string ConnectionString { get; set; }

        public string ProviderName { get; set; }

        public IList<DoConfigDb> Adds { get; set; }
    }

    public class DoConfigDb
    {
        public string Name { get; set; }

        public string ReadConnectionString { get; set; }

        public string WriteConnectionString { get; set; }

        public string ProviderName { get; set; }
    }
}
