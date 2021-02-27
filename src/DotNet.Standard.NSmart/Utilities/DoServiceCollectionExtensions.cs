using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;

namespace DotNet.Standard.NSmart.Utilities
{
    public static class DoServiceCollectionExtensions
    {
        public static IServiceCollection AddNSmart(this IServiceCollection services, Action<DoOptions> setupAction)
        {
            var options = new DoOptions
            {
                DbConfigs = new Dictionary<string, DoConfigDbs>(),
                ParamConfigs = new Dictionary<string, ParamConfig>()
            };
            setupAction(options);
            services.AddSingleton(options);
            DoConfig.DoOptions = options;
            return services;
        }
    }
}
