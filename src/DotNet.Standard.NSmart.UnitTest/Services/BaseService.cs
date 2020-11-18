using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using DotNet.Standard.NParsing.Factory;
using DotNet.Standard.NParsing.Interface;
using DotNet.Standard.NSmart.UnitTest.IServices;
using DotNet.Standard.NSmart.UnitTest.Models;
using DotNet.Standard.NSmart.Utilities;

namespace DotNet.Standard.NSmart.UnitTest.Services
{
    public class BaseService<TM, TT> : DoServiceBase<TM, TT>, IBaseService<TM>
        where TM: BaseInfo, new()
        where TT: BaseTerm, new()
    {
        public BaseService() : base(new TT().Of())
        {
            if (!DoParam.Initialized)
            {
                DoParam.Initialize();
            }
        }

        public RequestParamInfo RequestParam { get; protected set; }

        protected virtual ResultInfo OnAddingJudge(TM model, ref IObQueryable<TM, TT> queryable)
        {
            OnGlobalExecuting(ref queryable);
            return new ResultInfo();
        }

        protected virtual ResultInfo OnUpdatingJudge(TM model, ref IObQueryable<TM, TT> queryable)
        {
            OnGlobalExecuting(ref queryable);
            return new ResultInfo();
        }

        protected virtual ResultInfo OnDeletingJudge(ref IObQueryable<TM, TT> queryable)
        {
            OnGlobalExecuting(ref queryable);
            return new ResultInfo();
        }

        public new async Task<ResultInfo<TM>> Add(TM model)
        {
            IObQueryable<TM, TT> queryable = null;
            var ret = OnAddingJudge(model, ref queryable);
            var result = new ResultInfo<TM>(ret)
            {
                OperationCategory = OperationCategory.Add
            };
            if (!ret.IsSuccess())
                return result;
            model.Id = Convert.ToInt32(await AddAsync(model));
            result.Data = model;
            return result;
        }

        public async Task<ResultInfo<TM>> Update(TM model)
        {
            IObQueryable<TM, TT> queryable = null;
            var ret = OnUpdatingJudge(model, ref queryable);
            var result = new ResultInfo<TM>(ret)
            {
                OperationCategory = OperationCategory.Mod
            };
            if (!ret.IsSuccess())
                return result;
            await UpdateAsync(model, o => o.Where(k => k.Id == model.Id));
            result.Data = model;
            return result;
        }

        public async Task<ResultInfo<IList<TM>>> Delete(int[] ids)
        {
            IObQueryable<TM, TT> queryable = null;
            var result = OnDeletingJudge(ref queryable);
            var ret = new ResultInfo<IList<TM>>(result)
            {
                OperationCategory = OperationCategory.Del
            };
            if (!ret.IsSuccess())
                return ret;
            await DeleteAsync(o => o.Where(k => k.Id.In(ids)));
            return new ResultInfo<IList<TM>>
            {
                Data = ids.Select(id => new TM
                {
                    Id = id
                }).ToList()
            };
        }

        public async Task<ResultInfo<TM>> GetModel(int id)
        {
            var data = await GetModelAsync(o => o.Where(k => k.Id == id));
            return new ResultInfo<TM>
            {
                Data = data
            };
        }

        public async Task<ResultInfo<IList<TM>>> GetList()
        {
            var result = new ResultInfo<IList<TM>>();
            result.Data = await GetDataList(count => { result.Total = count; });
            return result;
        }

        public async Task<IList<TM>> GetDataList(Action<int> countAccessor)
        {
            return await GetListAsync(null, RequestParam.Params, RequestParam.Sorts, RequestParam.PageSize, RequestParam.PageIndex, countAccessor);
        }

        public void Initialize(IApiBase iApiBase)
        {
            foreach (var property in GetType()
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Where(obj => typeof(IBaseService).IsAssignableFrom(obj.PropertyType)))
            {
                var obj = property.GetValue(this);
                if (obj == null) continue;
                var baseService = obj as IBaseService;
                baseService?.Initialize(iApiBase);
            }
            RequestParam = iApiBase.RequestParam;
        }
    }
}
