using System.IO;
using System.Reflection;
using DotNet.Standard.NParsing.DbUtilities;
using DotNet.Standard.NParsing.Interface;
using DotNet.Standard.NSmart.UnitTest.Models;
using DotNet.Standard.NSmart.UnitTest.Services;
using DotNet.Standard.NSmart.Utilities;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DotNet.Standard.NSmart.UnitTest
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            var configuration = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", true).Build();
            var services = new ServiceCollection();
            services.AddNSmart(options =>
            {
                options.Use(configuration);
            });
            new TestService().QueryList();
            //var a = DoParam.GetProperty<Employe>(new Employe(), "Id");
            //var a = new EmployeService().GetModel(new int[]{1,2}).Result;
        }
    }
}
