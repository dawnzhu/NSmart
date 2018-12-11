using System;
using System.Reflection;
using DotNet.Standard.NParsing.Factory;
using DotNet.Standard.NParsing.Interface;

namespace DotNet.Standard.NSmart
{
    [Serializable]
    public abstract class DoModelBase : ObModelBase
    {
        private int _id;

        /// <summary>
        /// 编号
        /// </summary>
        public virtual int Id
        {
            get => _id;
            set
            {
                SetPropertyValid(MethodBase.GetCurrentMethod());
                _id = value;
            }
        }
    }

    public abstract class DoTermBase: ObTermBase
    {
        protected DoTermBase(Type modelType) : base(modelType)
        { }

        protected DoTermBase(Type modelType, string rename) : base(modelType, rename)
        { }

        protected DoTermBase(Type modelType, ObTermBase parent, MethodBase currentMethod) : base(modelType, parent, currentMethod)
        { }

        /// <summary>
        /// 编号
        /// </summary>
        public virtual ObProperty Id => GetProperty(MethodBase.GetCurrentMethod());
    }
}
