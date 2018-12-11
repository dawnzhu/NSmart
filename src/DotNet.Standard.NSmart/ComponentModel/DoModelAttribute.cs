using DotNet.Standard.NParsing.ComponentModel;

namespace DotNet.Standard.NSmart.ComponentModel
{
    public class DoModelAttribute : ObModelAttribute
    {
        public DoType DoType { get; set; }
    }

    public enum DoType
    {
        Id = 1,

        Minute = 2,

        Hour = 3,

        Day = 4
    }
}
