using System.Collections.Generic;
using Microsoft.Extensions.Configuration;

namespace DotNet.Standard.NSmart.Utilities
{
    public class DoOptions
    {
        public Dictionary<string, DoConfigDbs> DbConfigs { get; set; }

        public Dictionary<string, ParamConfig> ParamConfigs { get; set; }

        public DoOptions Use(IConfiguration configuration)
        {
            DbConfigs = DoConfig.Get(configuration);
            ParamConfigs = DoParamConfig.Get(configuration);
            ParamConfigs.Initialize();
            return this;
        }
    }
}
