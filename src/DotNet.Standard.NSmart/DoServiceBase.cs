using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DotNet.Standard.NParsing.Factory;
using DotNet.Standard.NParsing.Interface;
using System.Linq;
using System.Reflection;
using DotNet.Standard.NParsing.ComponentModel;
using DotNet.Standard.NSmart.ComponentModel;
using DotNet.Standard.NSmart.Utilities;

namespace DotNet.Standard.NSmart
{
    public abstract class DoServiceBase<TM, TT>
        where TM : DoModelBase
        where TT : DoTermBase, new()
    {
        /// <summary>
        /// 数据库操作对象
        /// </summary>
        protected TT Term;
        protected IList<IObHelper<TM, TT>> BaseDals;
        private readonly DoConfigDbs _doConfigDb;

        /// <summary>
        /// 拆分数据关键字
        /// </summary>
        public int SplitDataKey { get; set; }

        protected DoServiceBase() : this(null, DoConfig.Get(), "MainDbs")
        { }

        protected DoServiceBase(TT term) : this(term, DoConfig.Get(), "MainDbs")
        { }

        protected DoServiceBase(string dbsName) : this(null, DoConfig.Get(), dbsName)
        { }

        protected DoServiceBase(TT term, string dbsName) : this(term, DoConfig.Get(), dbsName)
        { }

        protected DoServiceBase(Dictionary<string, DoConfigDbs> doConfigDbs) : this(null, doConfigDbs, "MainDbs")
        { }

        protected DoServiceBase(TT term, Dictionary<string, DoConfigDbs> doConfigDbs) : this(term, doConfigDbs, "MainDbs")
        { }

        protected DoServiceBase(TT term, Dictionary<string, DoConfigDbs> doConfigDbs, string dbsName)
        {
            Term = term ?? new TT();
            BaseDals = new List<IObHelper<TM, TT>>();
            if (doConfigDbs == null || doConfigDbs.Count == 0)
            {
                throw new Exception("使用NSmart框架后，数据库连接配置必须在ConnectionConfigs中配置。");
            }
            /*var dbsName = "MainDbs";*/
            var doService = (DoServiceAttribute)GetType().GetCustomAttribute(typeof(DoServiceAttribute), true);
            if (doService != null)
            {
                dbsName = doService.DbsName;
            }
            if (!doConfigDbs.ContainsKey(dbsName))
            {
                throw new Exception($"{dbsName}在ConnectionConfigs配置中不存在。");
            }
            _doConfigDb = doConfigDbs[dbsName];
            foreach (var config in _doConfigDb.Adds)
            {
               var dal = config.ReadConnectionString == config.WriteConnectionString
                    ? ObHelper.Create<TM, TT>(Term, config.ReadConnectionString, config.ProviderName)
                    : ObHelper.Create<TM, TT>(Term, config.ReadConnectionString, config.WriteConnectionString, config.ProviderName);
                BaseDals.Add(dal);
            }
        }

        private bool GetDal(string type, out IObHelper<TM, TT> dal)
        {
            dal = null;
            var doType = DoType.None;
            var doModel = (DoModelAttribute)typeof(TM).GetCustomAttribute(typeof(DoModelAttribute), true);
            if (doModel != null)
            {
                doType = doModel.DoType;
            }
            var index = -1;
            switch (type)
            {
                case "ADD": //INSERT
                    switch (doType)
                    {
                        case DoType.None:
                            return false;
                        case DoType.Id:
                        case DoType.Business:
                            if (SplitDataKey == 0)
                            {
                                throw new Exception("请设置SplitDataKey值。");
                            }
                            index = SplitDataKey % BaseDals.Count;
                            break;
                        case DoType.Minute:
                            index = DateTime.Now.Minute % BaseDals.Count;
                            break;
                        case DoType.Hour:
                            index = DateTime.Now.Hour % BaseDals.Count;
                            break;
                        case DoType.Day:
                            index = DateTime.Now.Day % BaseDals.Count;
                            break;
                    }
                    break;
                case "MOD": //UPDATE DELETE
                    switch (doType)
                    {
                        case DoType.None:
                        case DoType.Minute:
                        case DoType.Hour:
                        case DoType.Day:
                            return false;
                        case DoType.Id:
                        case DoType.Business:
                            if (SplitDataKey == 0)
                            {
                                return false;
                            }
                            index = SplitDataKey % BaseDals.Count;
                            break;
                    }
                    break;
                case "QUERY": //SELECT
                    switch (doType)
                    {
                        case DoType.None:
                            index = new Random((int)DateTime.Now.Ticks).Next(0, BaseDals.Count - 1);
                            break;
                        case DoType.Id:
                        case DoType.Minute:
                        case DoType.Hour:
                        case DoType.Day:
                            return false;
                        case DoType.Business:
                            if (SplitDataKey == 0)
                            {
                                return false;
                            }
                            index = SplitDataKey % BaseDals.Count;
                            break;
                    }
                    break;
            }
            dal = BaseDals[index];
            return true;
        }

        /// <summary>
        /// 添加
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        protected object Add(TM model)
        {
            ObParameterBase param = null;
            ObJoinBase join = null;
            OnAdding(model, Term, ref join, ref param);
            object ret = null;
            var obIdentity = (ObIdentityAttribute)typeof(TM).GetProperty("Id", BindingFlags.Instance | BindingFlags.Public)?.GetCustomAttribute(typeof(ObIdentityAttribute), true);
            if (obIdentity == null || obIdentity.ObIdentity == ObIdentity.Program)
            {
                model.Id = NewIdentity();
            }
            if (SplitDataKey == 0)
            {
                SplitDataKey = model.Id;
            }
            if (GetDal("ADD", out var dal))
            {
                ret = dal.Add(model);
            }
            else
            {
                Parallel.For(0, BaseDals.Count, new ParallelOptions
                {
                    MaxDegreeOfParallelism = BaseDals.Count
                }, i =>
                {
                    var r = BaseDals[i].Add(model);
                    if (r != null)
                    {
                        ret = r;
                    }
                });
            }
            OnAdded(model, ret);
            return ret;
        }

        protected int Update(TM model, IObParameter param)
        {
            return Update(model, null, param);
        }

        /// <summary>
        /// 修改
        /// </summary>
        /// <param name="model"></param>
        /// <param name="join"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        protected int Update(TM model, IObJoin join, IObParameter param)
        {
            var paramBase = (ObParameterBase) param;
            var joinBase = (ObJoinBase) join;
            OnUpdating(model, Term, ref joinBase, ref paramBase);
            param = paramBase;
            var ret = 0;
            if (GetDal("MOD", out var dal))
            {
                ret = dal.Update(model, joinBase, param);
            }
            else
            {
                Parallel.For(0, BaseDals.Count, new ParallelOptions
                {
                    MaxDegreeOfParallelism = BaseDals.Count
                }, i =>
                {
                    var r = BaseDals[i].Update(model, joinBase, param);
                    if (r > ret)
                    {
                        ret = r;
                    }
                });
            }
            OnUpdated(model, ret);
            return ret;
        }

        protected int Update(TM model, Func<TT, IObParameter> keySelector)
        {
            return Update(model, keySelector(Term));
        }

        protected int Update<TKey>(TM model, Func<TT, TKey> joinSelector, Func<TT, IObParameter> keySelector)
        {
            return Update(model, BaseDals.First().Join(joinSelector).ObJoin, keySelector(Term));
        }

        /// <summary>
        /// 删除
        /// </summary>
        /// <param name="param"></param>
        /// <returns></returns>
        protected int Delete(IObParameter param)
        {
            return Delete(null, param);
        }

        protected int Delete(IObJoin join, IObParameter param)
        {
            var paramBase = (ObParameterBase) param;
            var joinBase = (ObJoinBase) join;
            OnDeleting(Term, ref joinBase, ref paramBase);
            param = paramBase;
            var ret = 0;
            if (GetDal("MOD", out var dal))
            {
                ret = dal.Delete(joinBase, param);
            }
            else
            {
                Parallel.For(0, BaseDals.Count, new ParallelOptions
                {
                    MaxDegreeOfParallelism = BaseDals.Count
                }, i =>
                {
                    var r = BaseDals[i].Delete(joinBase, param);
                    if (r > ret)
                    {
                        ret = r;
                    }
                });
            }
            OnDeleted(ret);
            return ret;
        }

        protected int Delete(Func<TT, IObParameter> keySelector)
        {
            return Delete(keySelector(Term));
        }

        protected int Delete<TKey>(Func<TT, TKey> joinSelector, Func<TT, IObParameter> keySelector)
        {
            return Delete(BaseDals.First().Join(joinSelector).ObJoin, keySelector(Term));
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="keySelector"></param>
        /// <returns></returns>
        protected IList<TM> GetList(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector)
        {
            return GetList(keySelector, null, null, out _);
        }

        protected IList<TM> GetList(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector, int? pageSize, int? pageIndex, out int count)
        {
            var query = keySelector != null
                ? keySelector(BaseDals.First()) ?? BaseDals.First().Where(o => null)
                : BaseDals.First().Where(o => null);
            return GetList(query.ObJoin, query.ObParameter, query.ObGroup, query.ObGroupParameter, query.ObSort, pageSize, pageIndex, out count);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <returns></returns>
        protected IList<TM> GetList(IObParameter param)
        {
            return GetList(param, (IObSort)null);
        }

        protected IList<TM> GetList(IObJoin join, IObParameter param)
        {
            return GetList(join, param, (IObSort)null);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="sort"></param>
        /// <returns></returns>
        protected IList<TM> GetList(IObParameter param, IObSort sort)
        {
            return GetList(param, sort, null, null, out _);
        }

        protected IList<TM> GetList(IObJoin join, IObParameter param, IObSort sort)
        {
            return GetList(join, param, sort, null, null, out _);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="sort"></param>
        /// <param name="pageSize"></param>
        /// <param name="pageIndex"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        protected IList<TM> GetList(IObParameter param, IObSort sort, int? pageSize, int? pageIndex, out int count)
        {
            return GetList(null, param, null, null, sort, pageSize, pageIndex, out count);
        }

        protected IList<TM> GetList(IObJoin join, IObParameter param, IObSort sort, int? pageSize, int? pageIndex, out int count)
        {
            return GetList(join, param, null, null, sort, pageSize, pageIndex, out count);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="group"></param>
        /// <param name="groupParam"></param>
        /// <param name="sort"></param>
        /// <returns></returns>
        protected IList<TM> GetList(IObParameter param, IObGroup group, IObParameter groupParam, IObSort sort)
        {
            return GetList(null, param, group, groupParam, sort, null, null, out _);
        }

        protected IList<TM> GetList(IObJoin join, IObParameter param, IObGroup group, IObParameter groupParam, IObSort sort)
        {
            return GetList(join, param, group, groupParam, sort, null, null, out _);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="join"></param>
        /// <param name="param"></param>
        /// <param name="group"></param>
        /// <param name="groupParam"></param>
        /// <param name="sort"></param>
        /// <param name="pageSize"></param>
        /// <param name="pageIndex"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        protected IList<TM> GetList(IObJoin join, IObParameter param, IObGroup group, IObParameter groupParam, IObSort sort, int? pageSize, 
            int? pageIndex, out int count)
        {
            var total = 0;
            var paramBase = (ObParameterBase) param;
            var joinBase = (ObJoinBase) join;
            OnListing(Term, ref joinBase, ref paramBase);
            param = paramBase;
            var isDal = GetDal("QUERY", out var dal);
            var sourceList = new List<TM>();
            if (pageSize.HasValue)
            {
                if (!pageIndex.HasValue)
                {
                    pageIndex = 1;
                }
                if (isDal)
                {
                    sourceList = (List<TM>)dal.Query(joinBase, param, group, groupParam, sort).ToList(pageSize.Value, pageIndex.Value, out total);
                }
                else
                {
                    Parallel.For(0, BaseDals.Count, new ParallelOptions
                    {
                        MaxDegreeOfParallelism = BaseDals.Count
                    }, i =>
                    {
                        var subList = BaseDals[i].Query(joinBase, param, group, groupParam, sort).ToList(pageSize.Value * pageIndex.Value, 1, out var c);
                        lock (sourceList)
                        {
                            total += c;
                            sourceList.AddRange(subList);
                        }
                    });
                }
            }
            else
            {
                if (isDal)
                {
                    sourceList = (List<TM>)dal.Query(joinBase, param, group, groupParam, sort).ToList();
                    total = sourceList.Count;
                }
                else
                {
                    Parallel.For(0, BaseDals.Count, new ParallelOptions
                    {
                        MaxDegreeOfParallelism = BaseDals.Count
                    }, i =>
                    {
                        var query = BaseDals[i].Query(joinBase, param, group, groupParam, sort);
                        var subList = query.ToList();
                        lock (sourceList)
                        {
                            total += subList.Count;
                            sourceList.AddRange(subList);
                        }
                    });
                }
            }
            IList<TM> list;
            if (sort != null && !isDal)
            {
                //排序
                IOrderedEnumerable<TM> order = null;
                foreach (var dbSort in sort.List)
                {
                    var properties = $"{dbSort.TableName}_{dbSort.ObProperty.PropertyName}".Split('_').Skip(1).ToArray();
                    order = order == null
                        ? (dbSort.IsAsc
                            ? sourceList.OrderBy(obj => GetValue(obj, properties))
                            : sourceList.OrderByDescending(obj => GetValue(obj, properties)))
                        : (dbSort.IsAsc
                            ? order.ThenBy(obj => GetValue(obj, properties))
                            : order.ThenByDescending(obj => GetValue(obj, properties)));
                }
                list = pageSize.HasValue
                    ? order?.Skip(pageSize.Value * (pageIndex.Value - 1)).Take(pageSize.Value).ToList()
                    : order?.ToList();
            }
            else
            {
                list = sourceList;
            }
            count = total;
            OnListed(list);
            return list;
        }

        private static object GetValue(object obj, string[] properties)
        {
            var index = 0;
            while (true)
            {
                if (obj == null) return null;
                obj = obj.GetType().GetProperty(properties[index], BindingFlags.Instance | BindingFlags.Public)?.GetValue(obj);
                if (index + 1 == properties.Length) return obj;
                index++;
            }
        }

        protected IList<TM> GetList(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector, IDictionary<string, object> requestParams)
        {
            return GetList(keySelector, requestParams, null, null, null, null, out _);
        }

        protected IList<TM> GetList(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector, IDictionary<string, object> requestParams, 
            IDictionary<string, object> requestGroupParams)
        {
            return GetList(keySelector, requestParams, requestGroupParams, null, null, null, out _);
        }

        protected IList<TM> GetList(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector, IDictionary<string, object> requestParams,
            IDictionary<string, string> requestSorts)
        {
            return GetList(keySelector, requestParams, null, requestSorts, null, null, out _);
        }

        protected IList<TM> GetList(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector, IDictionary<string, object> requestParams, 
            IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts)
        {
            return GetList(keySelector, requestParams, requestGroupParams, requestSorts, null, null, out _);
        }

        protected IList<TM> GetList(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector, IDictionary<string, object> requestParams,
            IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, out int count)
        {
            return GetList(keySelector, requestParams, null, requestSorts, pageSize, pageIndex, out count);
        }

        protected IList<TM> GetList(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector, IDictionary<string, object> requestParams, 
            IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, out int count)
        {
            var query = keySelector != null 
                ? keySelector(BaseDals.First()) ?? BaseDals.First().Where(o => null)
                : BaseDals.First().Where(o => null);
            return GetList(query.ObJoin, query.ObParameter, requestParams, query.ObGroup, query.ObGroupParameter, requestGroupParams, query.ObSort, requestSorts, pageSize, pageIndex, out count);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="requestParams"></param>
        /// <returns></returns>
        protected IList<TM> GetList(IObParameter param, IDictionary<string, object> requestParams)
        {
            return GetList(param, requestParams, null, null);
        }

        protected IList<TM> GetList(IObJoin join, IObParameter param, IDictionary<string, object> requestParams)
        {
            return GetList(join, param, requestParams, null, null);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="requestParams"></param>
        /// <param name="sort"></param>
        /// <param name="requestSorts"></param>
        /// <returns></returns>
        protected IList<TM> GetList(IObParameter param, IDictionary<string, object> requestParams, IObSort sort, IDictionary<string, string> requestSorts)
        {
            return GetList(param, requestParams, sort, requestSorts, null, null, out _);
        }

        protected IList<TM> GetList(IObJoin join, IObParameter param, IDictionary<string, object> requestParams, IObSort sort, IDictionary<string, string> requestSorts)
        {
            return GetList(join, param, requestParams, sort, requestSorts, null, null, out _);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="requestParams"></param>
        /// <param name="sort"></param>
        /// <param name="requestSorts"></param>
        /// <param name="pageSize"></param>
        /// <param name="pageIndex"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        protected IList<TM> GetList(IObParameter param, IDictionary<string, object> requestParams, IObSort sort, IDictionary<string, string> requestSorts, 
            int? pageSize, int? pageIndex, out int count)
        {
            return GetList(param, requestParams, null, null, null, sort, requestSorts, pageSize, pageIndex, out count);
        }

        protected IList<TM> GetList(IObJoin join, IObParameter param, IDictionary<string, object> requestParams, IObSort sort, 
            IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, out int count)
        {
            return GetList(join, param, requestParams, null, null, null, sort, requestSorts, pageSize, pageIndex, out count);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="requestParams"></param>
        /// <param name="group"></param>
        /// <param name="groupParam"></param>
        /// <param name="requestGroupParams"></param>
        /// <param name="sort"></param>
        /// <param name="requestSorts"></param>
        /// <returns></returns>
        protected IList<TM> GetList(IObParameter param, IDictionary<string, object> requestParams, IObGroup group, IObParameter groupParam, 
            IDictionary<string, object> requestGroupParams, IObSort sort, IDictionary<string, string> requestSorts)
        {
            return GetList(null, param, requestParams, group, groupParam, requestGroupParams, sort, requestSorts);
        }

        protected IList<TM> GetList(IObJoin join, IObParameter param, IDictionary<string, object> requestParams, IObGroup group, IObParameter groupParam, 
            IDictionary<string, object> requestGroupParams, IObSort sort, IDictionary<string, string> requestSorts)
        {
            return GetList(join, param, requestParams, group, groupParam, requestGroupParams, sort, requestSorts, null, null, out _);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="requestParams"></param>
        /// <param name="group"></param>
        /// <param name="groupParam"></param>
        /// <param name="requestGroupParams"></param>
        /// <param name="sort"></param>
        /// <param name="requestSorts"></param>
        /// <param name="pageSize"></param>
        /// <param name="pageIndex"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        protected IList<TM> GetList(IObParameter param, IDictionary<string, object> requestParams, IObGroup group, IObParameter groupParam, 
            IDictionary<string, object> requestGroupParams, IObSort sort, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, out int count)
        {
            return GetList(null, param, requestParams, group, groupParam, requestGroupParams, sort, requestSorts, pageSize, pageIndex, out count);
        }

        protected IList<TM> GetList(IObJoin join, IObParameter param, IDictionary<string, object> requestParams, IObGroup group, IObParameter groupParam, 
            IDictionary<string, object> requestGroupParams, IObSort sort, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, out int count)
        {
            var paramBase = (ObParameterBase) param;
            var groupParamBase = (ObParameterBase) groupParam;
            GetList(ref paramBase, requestParams, ref groupParamBase, requestGroupParams, ref sort, requestSorts);
            param = paramBase;
            groupParam = groupParamBase;
            return GetList(join, param, group, groupParam, sort, pageSize, pageIndex, out count);
        }

        /// <summary>
        /// Param
        /// </summary>
        /// <param name="param"></param>
        /// <param name="requestParams"></param>
        /// <param name="groupParam"></param>
        /// <param name="requestGroupParams"></param>
        /// <param name="sort"></param>
        /// <param name="requestSorts"></param>
        protected virtual void GetList(ref ObParameterBase param, IDictionary<string, object> requestParams, ref ObParameterBase groupParam, 
            IDictionary<string, object> requestGroupParams, ref IObSort sort, IDictionary<string, string> requestSorts)
        { }

        /// <summary>
        /// 获取模型数据
        /// </summary>
        /// <param name="param"></param>
        /// <returns></returns>
        protected TM GetModel(IObParameter param)
        {
            return GetModel(null, param);
        }

        protected TM GetModel(IObJoin join, IObParameter param)
        {
            var paramBase = (ObParameterBase)param;
            var joinBase = (ObJoinBase)join;
            OnModeling(Term, ref joinBase, ref paramBase);
            param = paramBase;
            TM model = null;
            if (GetDal("QUERY", out var dal))
            {
                model = dal.Query(joinBase, param).ToModel();
            }
            else
            {
                Parallel.For(0, BaseDals.Count, new ParallelOptions
                {
                    MaxDegreeOfParallelism = BaseDals.Count
                }, i =>
                {
                    var ret = BaseDals[i].Query(joinBase, param).ToModel();
                    if (ret != null)
                    {
                        model = ret;
                    }
                });
            }
            OnModeled(model);
            return model;
        }

        /// <summary>
        /// 获取模型数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="sort"></param>
        /// <returns></returns>
        protected TM GetModel(IObParameter param, IObSort sort)
        {
            return GetModel(null, param, sort);
        }

        protected TM GetModel(IObJoin join, IObParameter param, IObSort sort)
        {
            var paramBase = (ObParameterBase)param;
            var joinBase = (ObJoinBase)join;
            OnModeling(Term, ref joinBase, ref paramBase);
            param = paramBase;
            TM model;
            var list = new List<TM>();
            if (GetDal("QUERY", out var dal))
            {
                model = dal.Query(joinBase, param, sort).ToModel();
            }
            else
            {
                Parallel.For(0, BaseDals.Count, new ParallelOptions
                {
                    MaxDegreeOfParallelism = BaseDals.Count
                }, i =>
                {
                    var m = BaseDals[i].Query(joinBase, param, sort).ToModel();
                    if (m != null)
                    {
                        list.Add(m);
                    }
                });
                if (sort != null)
                {
                    //排序
                    IOrderedEnumerable<TM> order = null;
                    foreach (var dbSort in sort.List)
                    {
                        var properties = $"{dbSort.TableName}_{dbSort.ObProperty.PropertyName}".Split('_').Skip(1)
                            .ToArray();
                        order = order == null
                            ? (dbSort.IsAsc
                                ? list.OrderBy(obj => GetValue(obj, properties))
                                : list.OrderByDescending(obj => GetValue(obj, properties)))
                            : (dbSort.IsAsc
                                ? order.ThenBy(obj => GetValue(obj, properties))
                                : order.ThenByDescending(obj => GetValue(obj, properties)));
                    }

                    model = order?.ToList().FirstOrDefault();
                }
                else
                {
                    model = list.FirstOrDefault();
                }
            }
            OnModeled(model);
            return model;
        }

        protected TM GetModel(Func<TT, IObParameter> keySelector)
        {
            return GetModel(keySelector(Term));
        }

        protected TM GetModel(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector)
        {
            var query = keySelector != null
                ? keySelector(BaseDals.First()) ?? BaseDals.First().Where(o => null)
                : BaseDals.First().Where(o => null);
            var paramBase = (ObParameterBase) query.ObParameter;
            var joinBase = (ObJoinBase) query.ObJoin;
            OnModeling(Term, ref joinBase, ref paramBase);
            TM model = null;
            Parallel.For(0, BaseDals.Count, new ParallelOptions
            {
                MaxDegreeOfParallelism = BaseDals.Count
            }, i =>
            {
                var ret = BaseDals[i].Query(joinBase, paramBase, query.ObGroup, query.ObGroupParameter, query.ObSort).ToModel();
                if (ret != null)
                {
                    model = ret;
                }
            });
            OnModeled(model);
            return model;
        }

        /// <summary>
        /// 判断是否存在
        /// </summary>
        /// <param name="param"></param>
        /// <returns></returns>
        protected bool Exists(IObParameter param)
        {
            return Exists(null, param);
        }

        protected bool Exists(IObJoin join, IObParameter param)
        {
            var paramBase = (ObParameterBase)param;
            var joinBase = (ObJoinBase)join;
            OnExisting(Term, ref joinBase, ref paramBase);
            param = paramBase;
            var ret = false;
            if (GetDal("QUERY", out var dal))
            {
                ret = dal.Query(joinBase, param).Exists();
            }
            else
            {
                Parallel.For(0, BaseDals.Count, new ParallelOptions
                {
                    MaxDegreeOfParallelism = BaseDals.Count
                }, i =>
                {
                    if (BaseDals[i].Query(joinBase, param).Exists())
                    {
                        ret = true;
                    }
                });
            }
            OnExisted(ret);
            return ret;
        }

        protected bool Exists(Func<TT, IObParameter> keySelector)
        {
            return Exists(keySelector(Term));
        }

        protected bool Exists(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector)
        {
            var query = keySelector != null
                ? keySelector(BaseDals.First()) ?? BaseDals.First().Where(o => null)
                : BaseDals.First().Where(o => null);
            var paramBase = (ObParameterBase) query.ObParameter;
            var joinBase = (ObJoinBase) query.ObJoin;
            OnExisting(Term, ref joinBase, ref paramBase);
            var ret = false;
            Parallel.For(0, BaseDals.Count, new ParallelOptions
            {
                MaxDegreeOfParallelism = BaseDals.Count
            }, i =>
            {
                if (BaseDals[i].Query(joinBase, paramBase, query.ObGroup, query.ObGroupParameter, query.ObSort).Exists())
                {
                    ret = true;
                }
            });
            OnExisted(ret);
            return ret;
        }

        #region 触发事件

        /// <summary>
        /// 查询前
        /// </summary>
        /// <param name="term"></param>
        /// <param name="join"></param>
        /// <param name="param"></param>
        protected virtual void OnGlobalExecuting(TT term, ref ObJoinBase join, ref ObParameterBase param)
        {
        }

        /// <summary>
        /// 添加前
        /// </summary>
        /// <param name="model"></param>
        /// <param name="join"></param>
        /// <param name="param"></param>
        /// <param name="term"></param>
        protected virtual void OnAdding(TM model, TT term, ref ObJoinBase join, ref ObParameterBase param)
        {
            OnGlobalExecuting(term, ref join, ref param);
        }

        /// <summary>
        /// 添加后
        /// </summary>
        /// <param name="model"></param>
        /// <param name="result"></param>
        protected virtual void OnAdded(TM model, object result)
        {
        }

        /// <summary>
        /// 更新前
        /// </summary>
        /// <param name="model"></param>
        /// <param name="term"></param>
        /// <param name="join"></param>
        /// <param name="param"></param>
        protected virtual void OnUpdating(TM model, TT term, ref ObJoinBase join, ref ObParameterBase param)
        {
            OnGlobalExecuting(term, ref join, ref param);
        }

        /// <summary>
        /// 更新后
        /// </summary>
        /// <param name="model"></param>
        /// <param name="result"></param>
        protected virtual void OnUpdated(TM model, int result)
        {
        }

        /// <summary>
        /// 删除前
        /// </summary>
        /// <param name="term"></param>
        /// <param name="join"></param>
        /// <param name="param"></param>
        protected virtual void OnDeleting(TT term, ref ObJoinBase join, ref ObParameterBase param)
        {
            OnGlobalExecuting(term, ref join, ref param);
        }

        /// <summary>
        /// 删除后
        /// </summary>
        /// <param name="result"></param>
        protected virtual void OnDeleted(int result)
        {
        }

        /// <summary>
        /// 获取列表前
        /// </summary>
        /// <param name="term"></param>
        /// <param name="join"></param>
        /// <param name="param"></param>
        protected virtual void OnListing(TT term, ref ObJoinBase join, ref ObParameterBase param)
        {
            OnGlobalExecuting(term, ref join, ref param);
        }

        /// <summary>
        /// 获取列表后
        /// </summary>
        /// <param name="list"></param>
        protected virtual void OnListed(IList<TM> list)
        {
        }

        /// <summary>
        /// 获取模型前
        /// </summary>
        /// <param name="term"></param>
        /// <param name="join"></param>
        /// <param name="param"></param>
        protected virtual void OnModeling(TT term, ref ObJoinBase join, ref ObParameterBase param)
        {
            OnGlobalExecuting(term, ref join, ref param);
        }

        /// <summary>
        /// 获取模型后
        /// </summary>
        /// <param name="model"></param>
        protected virtual void OnModeled(TM model)
        {
        }

        /// <summary>
        /// 判断是否存在前
        /// </summary>
        /// <param name="term"></param>
        /// <param name="join"></param>
        /// <param name="param"></param>
        protected virtual void OnExisting(TT term, ref ObJoinBase join, ref ObParameterBase param)
        {
            OnGlobalExecuting(term, ref join, ref param);
        }

        /// <summary>
        /// 判断是否存在后
        /// </summary>
        /// <param name="result"></param>
        protected virtual void OnExisted(bool result)
        {
        }

        #endregion

        protected int NewIdentity()
        {
            var obRedefine = ObRedefine.Create<ObjectsToMaxIdInfo>($"{Term.ObTableName}ToMaxID");
            var dal = ObHelper.Create<ObjectsToMaxIdInfo>(_doConfigDb.ConnectionString, _doConfigDb.ProviderName, obRedefine);
            int id;
            using (var ot = ObConnection.BeginTransaction(_doConfigDb.ConnectionString, _doConfigDb.ProviderName))
            {
                try
                {
                    var model = dal.Query(ot).ToModel();
                    if (model == null)
                    {
                        model = new ObjectsToMaxIdInfo
                        {
                            MaxId = 1
                        };
                        dal.Add(ot, model);
                    }
                    else
                    {
                        model.MaxId += 1;
                        dal.Update(ot, model);
                    }
                    id = model.MaxId;
                    ot.Commit();
                }
                catch (Exception)
                {
                    ot.Rollback();
                    throw;
                }
            }
            return id;
        }

        protected async Task<object> AddAsync(TM model)
        {
            return await Task.Run(() => Add(model));
        }

        protected async Task<int> UpdateAsync(TM model, IObParameter param)
        {
            return await Task.Run(() => Update(model, param));
        }

        protected async Task<int> UpdateAsync(TM model, IObJoin join, IObParameter param)
        {
            return await Task.Run(() => Update(model, join, param));
        }

        protected async Task<int> UpdateAsync(TM model, Func<TT, IObParameter> keySelector)
        {
            return await Task.Run(() => Update(model, keySelector));
        }

        protected async Task<int> UpdateAsync<TKey>(TM model, Func<TT, TKey> joinSelector, Func<TT, IObParameter> keySelector)
        {
            return await Task.Run(() => Update(model, joinSelector, keySelector));
        }

        protected async Task<int> DeleteAsync(IObParameter param)
        {
            return await Task.Run(() => Delete(param));
        }

        protected async Task<int> DeleteAsync(IObJoin join, IObParameter param)
        {
            return await Task.Run(() => Delete(join, param));
        }

        protected async Task<int> DeleteAsync(Func<TT, IObParameter> keySelector)
        {
            return await Task.Run(() => Delete(keySelector));
        }

        protected async Task<int> DeleteAsync<TKey>(Func<TT, TKey> joinSelector, Func<TT, IObParameter> keySelector)
        {
            return await Task.Run(() => Delete(joinSelector, keySelector));
        }

        protected async Task<IList<TM>> GetListAsync(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector)
        {
            return await GetListAsync(keySelector, (int?)null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await Task.Run(() =>
            {
                var list = GetList(keySelector, pageSize, pageIndex, out var count);
                countAccessor?.Invoke(count);
                return list;
            });
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <returns></returns>
        protected async Task<IList<TM>> GetListAsync(IObParameter param)
        {
            return await GetListAsync(param, (IObSort)null);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, IObParameter param)
        {
            return await GetListAsync(join, param, (IObSort)null);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="sort"></param>
        /// <returns></returns>
        protected async Task<IList<TM>> GetListAsync(IObParameter param, IObSort sort)
        {
            return await GetListAsync(param, sort, null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, IObParameter param, IObSort sort)
        {
            return await GetListAsync(join, param, sort, null, null);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="sort"></param>
        /// <param name="pageSize"></param>
        /// <param name="pageIndex"></param>
        /// <param name="countAccessor"></param>
        /// <returns></returns>
        protected async Task<IList<TM>> GetListAsync(IObParameter param, IObSort sort, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await GetListAsync(null, param, null, null, sort, pageSize, pageIndex, countAccessor);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, IObParameter param, IObSort sort, int? pageSize, int? pageIndex, Action<int> countAccessor = null)
        {
            return await GetListAsync(join, param, null, null, sort, pageSize, pageIndex, countAccessor);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="group"></param>
        /// <param name="groupParam"></param>
        /// <param name="sort"></param>
        /// <returns></returns>
        protected async Task<IList<TM>> GetListAsync(IObParameter param, IObGroup group, IObParameter groupParam, IObSort sort)
        {
            return await GetListAsync(null, param, group, groupParam, sort, null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, IObParameter param, IObGroup group, IObParameter groupParam, IObSort sort)
        {
            return await GetListAsync(join, param, group, groupParam, sort, null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, IObParameter param, IObGroup group, IObParameter groupParam,
            IObSort sort, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await Task.Run(() =>
            {
                var list = GetList(join, param, group, groupParam, sort, pageSize, pageIndex, out var count);
                countAccessor?.Invoke(count);
                return list;
            });
        }


        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="keySelector"></param>
        /// <param name="requestParams"></param>
        /// <returns></returns>
        protected async Task<IList<TM>> GetListAsync(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector, IDictionary<string, object> requestParams)
        {
            return await GetListAsync(keySelector, requestParams, null, null, null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector, IDictionary<string, object> requestParams, 
            IDictionary<string, object> requestGroupParams)
        {
            return await GetListAsync(keySelector, requestParams, requestGroupParams, null, null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector, IDictionary<string, object> requestParams,
            IDictionary<string, string> requestSorts)
        {
            return await GetListAsync(keySelector, requestParams, null, requestSorts, null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector, IDictionary<string, object> requestParams,
            IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts)
        {
            return await GetListAsync(keySelector, requestParams, requestGroupParams, requestSorts, null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector, IDictionary<string, object> requestParams, 
            IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await GetListAsync(keySelector, requestParams, null, requestSorts, pageSize, pageIndex, countAccessor);
        }

        protected async Task<IList<TM>> GetListAsync(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector, IDictionary<string, object> requestParams, 
            IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await Task.Run(() =>
            {
                var list = GetList(keySelector, requestParams, requestGroupParams, requestSorts, pageSize, pageIndex, out var count);
                countAccessor?.Invoke(count);
                return list;
            });
        }


        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="requestParams"></param>
        /// <returns></returns>
        protected async Task<IList<TM>> GetListAsync(IObParameter param, IDictionary<string, object> requestParams)
        {
            return await GetListAsync(param, requestParams, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, IObParameter param, IDictionary<string, object> requestParams)
        {
            return await GetListAsync(join, param, requestParams, null, null);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="requestParams"></param>
        /// <param name="sort"></param>
        /// <param name="requestSorts"></param>
        /// <returns></returns>
        protected async Task<IList<TM>> GetListAsync(IObParameter param, IDictionary<string, object> requestParams, IObSort sort, 
            IDictionary<string, string> requestSorts)
        {
            return await GetListAsync(param, requestParams, sort, requestSorts, null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, IObParameter param, IDictionary<string, object> requestParams, IObSort sort, 
            IDictionary<string, string> requestSorts)
        {
            return await GetListAsync(join, param, requestParams, sort, requestSorts, null, null, null);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="requestParams"></param>
        /// <param name="sort"></param>
        /// <param name="requestSorts"></param>
        /// <param name="pageSize"></param>
        /// <param name="pageIndex"></param>
        /// <param name="countAccessor"></param>
        /// <returns></returns>
        protected async Task<IList<TM>> GetListAsync(IObParameter param, IDictionary<string, object> requestParams, IObSort sort, 
            IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await GetListAsync(param, requestParams, null, null, null, sort, requestSorts, pageSize, pageIndex, countAccessor);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, IObParameter param, IDictionary<string, object> requestParams, IObSort sort, 
            IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await GetListAsync(join, param, requestParams, null, null, null, sort, requestSorts, pageSize, pageIndex, countAccessor);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="requestParams"></param>
        /// <param name="group"></param>
        /// <param name="groupParam"></param>
        /// <param name="requestGroupParams"></param>
        /// <param name="sort"></param>
        /// <param name="requestSorts"></param>
        /// <returns></returns>
        protected async Task<IList<TM>> GetListAsync(IObParameter param, IDictionary<string, object> requestParams, IObGroup group, 
            IObParameter groupParam, IDictionary<string, object> requestGroupParams, IObSort sort, IDictionary<string, string> requestSorts)
        {
            return await GetListAsync(null, param, requestParams, group, groupParam, requestGroupParams, sort, requestSorts);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, IObParameter param, IDictionary<string, object> requestParams, IObGroup group,
            IObParameter groupParam, IDictionary<string, object> requestGroupParams, IObSort sort, IDictionary<string, string> requestSorts)
        {
            return await GetListAsync(join, param, requestParams, group, groupParam, requestGroupParams, sort, requestSorts, null, null, null);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="requestParams"></param>
        /// <param name="group"></param>
        /// <param name="groupParam"></param>
        /// <param name="requestGroupParams"></param>
        /// <param name="sort"></param>
        /// <param name="requestSorts"></param>
        /// <param name="pageSize"></param>
        /// <param name="pageIndex"></param>
        /// <param name="countAccessor"></param>
        /// <returns></returns>
        protected async Task<IList<TM>> GetListAsync(IObParameter param, IDictionary<string, object> requestParams, IObGroup group, 
            IObParameter groupParam, IDictionary<string, object> requestGroupParams, IObSort sort, IDictionary<string, string> requestSorts, 
            int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await GetListAsync(null, param, requestParams, group, groupParam, requestGroupParams, sort, requestSorts, pageSize, pageIndex, countAccessor);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, IObParameter param, IDictionary<string, object> requestParams, IObGroup group,
            IObParameter groupParam, IDictionary<string, object> requestGroupParams, IObSort sort, IDictionary<string, string> requestSorts,
            int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await Task.Run(() =>
            {
                var paramBase = (ObParameterBase)param;
                var groupParamBase = (ObParameterBase)groupParam;
                GetList(ref paramBase, requestParams, ref groupParamBase, requestGroupParams, ref sort, requestSorts);
                param = paramBase;
                groupParam = groupParamBase;
                var list = GetList(join, param, group, groupParam, sort, pageSize, pageIndex, out var count);
                countAccessor?.Invoke(count);
                return list;
            });
        }

        protected async Task<TM> GetModelAsync(IObParameter param)
        {
            return await Task.Run(() => GetModel(param));
        }

        protected async Task<TM> GetModelAsync(IObJoin join, IObParameter param)
        {
            return await Task.Run(() => GetModel(join, param));
        }

        protected async Task<TM> GetModelAsync(Func<TT, IObParameter> keySelector)
        {
            return await Task.Run(() => GetModel(keySelector));
        }

        protected async Task<TM> GetModelAsync(IObParameter param, IObSort sort)
        {
            return await Task.Run(() => GetModel(param, sort));
        }

        protected async Task<TM> GetModelAsync(IObJoin join, IObParameter param, IObSort sort)
        {
            return await Task.Run(() => GetModel(join, param, sort));
        }

        protected async Task<TM> GetModelAsync(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector)
        {
            return await Task.Run(() => GetModel(keySelector));
        }

        protected async Task<bool> ExistsAsync(IObParameter param)
        {
            return await Task.Run(() => Exists(param));
        }

        protected async Task<bool> ExistsAsync(IObJoin join, IObParameter param)
        {
            return await Task.Run(() => Exists(join, param));
        }

        protected async Task<bool> ExistsAsync(Func<TT, IObParameter> keySelector)
        {
            return await Task.Run(() => Exists(keySelector));
        }

        protected async Task<bool> ExistsAsync(Func<IObHelper<TM, TT>, IObSelect<TM, TT>> keySelector)
        {
            return await Task.Run(() => Exists(keySelector));
        }

        protected async Task<int> NewIdentityAsync()
        {
            return await Task.Run(NewIdentity);
        }
    }
}
