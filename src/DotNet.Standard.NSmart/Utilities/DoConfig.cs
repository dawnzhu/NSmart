﻿using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Configuration;

namespace DotNet.Standard.NSmart.Utilities
{
    public static class DoConfig
    {
        public static Dictionary<string, DoConfigDbs> Get()
        {
            var configuration = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", true).Build();
            return Get(configuration);
        }

        public static Dictionary<string, DoConfigDbs> Get(IConfiguration configuration)
        {
            var config = configuration.GetSection("ConnectionConfigs").GetChildren().ToDictionary(key => key["Name"], value =>
                new DoConfigDbs
                {
                    ConnectionString = value["ConnectionString"],
                    ProviderName = value["ProviderName"],
                    Adds = value.GetSection("Dbs").GetChildren().Any()
                        ? value.GetSection("Dbs").GetChildren().Select(obj => new DoConfigDb
                        {
                            Name = obj["Name"],
                            ReadConnectionString = obj["ReadConnectionString"] ?? obj["ConnectionString"],
                            WriteConnectionString = obj["WriteConnectionString"] ?? obj["ConnectionString"],
                            ProviderName = obj["ProviderName"]
                        }).ToList()
                        : new List<DoConfigDb>
                        {
                            new DoConfigDb
                            {
                                Name = "default",
                                ReadConnectionString = value["ConnectionString"],
                                WriteConnectionString = value["ConnectionString"],
                                ProviderName = value["ProviderName"]
                            }
                        }
                });
            return config;
        }

        public static DoOptions DoOptions { get; internal set; }
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
