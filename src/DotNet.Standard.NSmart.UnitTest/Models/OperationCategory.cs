using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DotNet.Standard.NSmart.UnitTest.Models
{
    /// <summary>
    /// 操作类别
    /// </summary>
    [Flags]
    public enum OperationCategory
    {
        /// <summary>
        /// 添加
        /// </summary>
        [Description("添加")]
        Add = 1,

        /// <summary>
        /// 修改
        /// </summary>
        [Description("修改")]
        Mod = 2,

        /// <summary>
        /// 删除
        /// </summary>
        [Description("删除")]
        Del = 4,

        /// <summary>
        /// 登录
        /// </summary>
        [Description("登录")]
        Login = 16
    }
}
