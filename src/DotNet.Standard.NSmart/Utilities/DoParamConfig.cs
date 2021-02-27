using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Configuration;

namespace DotNet.Standard.NSmart.Utilities
{
    public static class DoParamConfig
    {
        public static Dictionary<string, ParamConfig> Get()
        {
            return Get(Directory.GetCurrentDirectory() + "/Configs", "*.json");
        }

        public static Dictionary<string, ParamConfig> Get(IConfiguration configuration)
        {
            var path = configuration["ParamConfig:Path"];
            if (path.StartsWith("~"))
            {
                path = Directory.GetCurrentDirectory() + path.TrimStart('~');
            }
            return Get(path, configuration["ParamConfig:SearchPattern"]);
        }

        public static Dictionary<string, ParamConfig> Get(string path, string searchPattern)
        {
            var dic = new Dictionary<string, ParamConfig>();
            var files = Directory.GetFiles(path, searchPattern);
            foreach (var file in files)
            {
                var builder = new ConfigurationBuilder().SetBasePath(path)
                    .AddJsonFile(file, true).Build();
                var configs = builder.GetSection("ParamConfig").GetChildren();
                foreach (var config in configs)
                {
                    foreach (var method in config.GetSection("Method").GetChildren())
                    {
                        dic.Add(config["Class"] + "." + method["Key"], new ParamConfig
                        {
                            Params = method.GetSection("Param").GetChildren().ToDictionary(key => key["Key"], value => new ParamInfo
                            {
                                Name = value["Name"],
                                TypeString = value["Type"],
                                Symbol = value["Symbol"]
                            }),
                            GroupParams = method.GetSection("GroupParam").GetChildren().ToDictionary(key => key["Key"], value => new ParamInfo
                            {
                                Name = value["Name"],
                                TypeString = value["Type"],
                                Symbol = value["Symbol"]
                            }),
                            Sorts = method.GetSection("Sort").GetChildren().ToDictionary(key => key["Key"] ?? Guid.NewGuid().ToString(), value => new ParamInfo
                            {
                                Name = value["Name"],
                                Symbol = value["Symbol"]
                            })
                        });
                    }
                }
            }
            return dic;
        }
    }

    public class ParamConfig
    {
        public IDictionary<string, ParamInfo> Params { get; set; }
        public IDictionary<string, ParamInfo> GroupParams { get; set; }
        public IDictionary<string, ParamInfo> Sorts { get; set; }
    }

    public class ParamInfo
    {
        public string Name { get; set; }
        public string TypeString { get; set; }
        public string Symbol { get; set; }
    }
}
