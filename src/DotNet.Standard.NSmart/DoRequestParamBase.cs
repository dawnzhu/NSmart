using System;
using System.Collections.Generic;

namespace DotNet.Standard.NSmart
{
    [Serializable]
    public class DoRequestParamBase
    {
        public int? PageIndex { get; set; }
        public int? PageSize { get; set; }
        public IDictionary<string, string> Sorts { get; set; }
        public IDictionary<string, object> Params { get; set; }
    }
}
