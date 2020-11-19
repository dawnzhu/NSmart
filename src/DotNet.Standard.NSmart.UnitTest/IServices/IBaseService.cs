using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DotNet.Standard.NSmart.UnitTest.Models;

namespace DotNet.Standard.NSmart.UnitTest.IServices
{
    public interface IBaseService<T> : IBaseService
        where T: BaseInfo
    {
        Task<ResultInfo<T>> Add(T model);
        Task<ResultInfo<T>> Update(T model);
        Task<ResultInfo<IList<T>>> Delete(int[] ids);
        Task<ResultInfo<T>> GetModel(int[] id);
        Task<ResultInfo<IList<T>>> GetList();
        Task<IList<T>> GetDataList(Action<int> fileNameMetadataAccessor = null);
    }

    public interface IBaseService
    {
        void Initialize(IApiBase iApiBase);
    }

    public interface IApiBase
    {
        RequestParamInfo RequestParam { get; }
    }
}
