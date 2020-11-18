using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace DotNet.Standard.NSmart.UnitTest.Models
{
    public class ResultInfo
    {
        public ResultInfo()
        {

        }

        public ResultInfo(ResultInfo ret)
        {

        }

        [JsonIgnore]
        public OperationCategory OperationCategory { get; set; }

        public bool IsSuccess()
        {
            return true;
        }

        public virtual void Auto()
        {
        }
    }

    public class ResultInfo<T> : ResultInfo
    {
        public ResultInfo()
        {

        }

        public ResultInfo(ResultInfo ret)
        {

        }

        public T Data { get; set; }

        public int Total { get; set; }
    }
}
