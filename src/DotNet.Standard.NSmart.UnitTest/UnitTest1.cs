using System.Collections.Generic;
using System.Globalization;
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
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;

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
            var aaaaa = new EmployeService()
            {
                RequestParam = new RequestParamInfo
                {
                    Params = new Dictionary<string, object>()
                }
            }.GetList();
            var aaaa = "TRADE_NO_CREATE_PAY".ToJsonString().ToObject(typeof(Status));
            var aa = "UserName".ToSnakeCaseNaming();
            var bb = aa.ToCamelCaseNaming(false);
            var cc = bb.ToCamelCaseNaming();
            var dd = cc.ToSnakeCaseNaming();
            var ee = dd.ToCamelCaseNaming();
            var a = new TradeInfo
            {
                Name = "abc",
                Status = Status.WAIT_SELLER_SEND_GOODS,
                StatusMessage = "msg"
            };
            var b = a.ToJsonString();
            var c = b.ToObject<TradeInfo>();

            new TestService().Update();
            //var a = DoParam.GetProperty<Employe>(new Employe(), "Id");
            //var a = new EmployeService().GetModel(new int[]{1,2}).Result;
        }
    }

    public class TradeInfo
    {
        public string Name { get; set; }

        public Status Status { get; set; }

        public string StatusMessage { get; set; }
    }

    public enum Status
    {
        TRADE_NO_CREATE_PAY = 1,

        WAIT_BUYER_PAY = 2,

        SELLER_CONSIGNED_PART = 4,

        WAIT_SELLER_SEND_GOODS = 8,

        WAIT_BUYER_CONFIRM_GOODS = 16,

        TRADE_BUYER_SIGNED = 32,

        TRADE_FINISHED = 64,

        TRADE_CLOSED = 128
    }
}
