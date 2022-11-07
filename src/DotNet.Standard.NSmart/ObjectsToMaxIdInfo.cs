using System.Reflection;
using DotNet.Standard.NParsing.ComponentModel;
using DotNet.Standard.NParsing.Factory;
using DotNet.Standard.NParsing.Interface;

namespace DotNet.Standard.NSmart
{
    /// <summary>
    /// 帐户最大编号表实体类
    /// </summary>
    [ObModel(Name = "CompanysToMaxID", Extra = "WITH (TABLOCKX)")]
    public class ObjectsToMaxIdInfo : ObModelBase
    {
        private long _maxId;

        /// <summary>
        /// 最大编号
        /// </summary>
        [ObProperty(Name = "MaxID", Length = 4, Nullable = true)]
        public long MaxId
        {
            get => _maxId;
            set
            {
                SetPropertyValid(MethodBase.GetCurrentMethod());
                _maxId = value;
            }
        }

    }

    /// <summary>
    /// 帐户最大编号表条件类
    /// </summary>	
    public class ObjectsToMaxId : ObTermBase
    {
        public ObjectsToMaxId(string tableName) : base(typeof(ObjectsToMaxIdInfo), tableName)
        { }

        /// <summary>
        /// 最大编号
        /// </summary>
        public ObProperty MaxId => GetProperty(MethodBase.GetCurrentMethod());
    }
}
