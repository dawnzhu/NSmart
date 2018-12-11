using System;

namespace DotNet.Standard.NSmart.ComponentModel
{
    [AttributeUsage(AttributeTargets.Class)]
    public class DoServiceAttribute : Attribute
    {
        public string DbsName { get; set; }
    }
}
