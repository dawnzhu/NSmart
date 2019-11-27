using System;
using DotNet.Standard.NParsing.ComponentModel;

namespace DotNet.Standard.NSmart.ComponentModel
{
    public class DoModelAttribute : ObModelAttribute
    {
        public DoType DoType { get; set; }
    }

    [Flags]
    public enum DoType
    {
        /// <summary>
        /// 存储到所有库
        /// </summary>
        None = 1,

        /// <summary>
        /// 根据数据编号存储，降低数据量
        /// </summary>
        Id = 2,

        /// <summary>
        /// 根据当前分钟存储，降低数据量
        /// </summary>
        Minute = 4,

        /// <summary>
        /// 根据当前小时存储，降低数据量
        /// </summary>
        Hour = 8,

        /// <summary>
        /// 根据当前日期存储，降低数据量
        /// </summary>
        Day = 16,

        /// <summary>
        /// 根据业务编号存储，降低数据量，降低查询并发
        /// </summary>
        Business = 32
    }
}
