using System.Reflection;
using DotNet.Standard.NParsing.Interface;
using DotNet.Standard.NSmart.UnitTest.Models;
using DotNet.Standard.NSmart.Utilities;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DotNet.Standard.NSmart.UnitTest
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            new TestService().Update();
        }
    }
}
