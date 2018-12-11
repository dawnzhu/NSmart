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

        protected IList<IObHelper<TM>> BaseDals;
        private readonly DoConfigDbs _doConfigDb;
        
        protected DoServiceBase() : this(DoConfig.Get(), "MainDbs")
        { }

        protected DoServiceBase(string dbsName) : this(DoConfig.Get(), dbsName)
        { }

        protected DoServiceBase(Dictionary<string, DoConfigDbs> doConfigDbs) : this(doConfigDbs, "MainDbs")
        { }

        protected DoServiceBase(Dictionary<string, DoConfigDbs> doConfigDbs, string dbsName)
        {
            Term = new TT();
            BaseDals = new List<IObHelper<TM>>();
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
                    ? ObHelper.Create<TM>(config.ReadConnectionString, config.ProviderName)
                    : ObHelper.Create<TM>(config.ReadConnectionString, config.WriteConnectionString, config.ProviderName);
                BaseDals.Add(dal);
            }
        }

        /// <summary>
        /// 添加
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        protected object Add(TM model)
        {
            ObParameterBase p = null;
            OnAdding(model, ref p);
            object ret = null;
            var obIdentity = (ObIdentityAttribute)typeof(TM).GetProperty("Id", BindingFlags.Instance | BindingFlags.Public)?.GetCustomAttribute(typeof(ObIdentityAttribute), true);
            if (obIdentity == null || obIdentity.ObIdentity == ObIdentity.Program)
            {
                model.Id = NewIdentity();
            }
            var doModel = (DoModelAttribute)typeof(TM).GetCustomAttribute(typeof(DoModelAttribute), true);
            if (doModel != null)
            {
                int index;
                if (doModel.DoType == DoType.Id && model.Id == 0)
                {
                    throw new Exception("当模型类DoModel.DoType设置为Id时，主键Id值不能由数据库生成。");
                }
                switch (doModel.DoType)
                {
                    case DoType.Id:
                        index = model.Id % BaseDals.Count;
                        break;
                    case DoType.Hour:
                        index = DateTime.Now.Hour % BaseDals.Count;
                        break;
                    case DoType.Day:
                        index = DateTime.Now.Day % BaseDals.Count;
                        break;
                    //case DoType.Minute:
                    default:
                        index = DateTime.Now.Minute % BaseDals.Count;
                        break;
                }
                ret = BaseDals[index].Add(model);
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

        /// <summary>
        /// 修改
        /// </summary>
        /// <param name="model"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        protected int Update(TM model, ObParameterBase param)
        {
            OnUpdateing(model, ref param);
            var ret = 0;
            Parallel.For(0, BaseDals.Count, new ParallelOptions
            {
                MaxDegreeOfParallelism = BaseDals.Count
            }, i =>
            {
                var r = BaseDals[i].Update(model, param);
                if (r > ret)
                {
                    ret = r;
                }
            });
            OnUpdated(model, ret);
            return ret;
        }

        /// <summary>
        /// 删除
        /// </summary>
        /// <param name="param"></param>
        /// <returns></returns>
        protected int Delete(ObParameterBase param)
        {
            OnDeleteing(ref param);
            var ret = 0;
            Parallel.For(0, BaseDals.Count, new ParallelOptions
            {
                MaxDegreeOfParallelism = BaseDals.Count
            }, i =>
            {
                var r = BaseDals[i].Delete(param);
                if (r > ret)
                {
                    ret = r;
                }
            });
            OnDeleted(ret);
            return ret;
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <returns></returns>
        protected IList<TM> GetList(ObParameterBase param)
        {
            return GetList(param, (IObSort)null);
        }

        protected IList<TM> GetList(IObJoin join, ObParameterBase param)
        {
            return GetList(join, param, (IObSort)null);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="sort"></param>
        /// <returns></returns>
        protected IList<TM> GetList(ObParameterBase param, IObSort sort)
        {
            return GetList(param, sort, null, null, out _);
        }

        protected IList<TM> GetList(IObJoin join, ObParameterBase param, IObSort sort)
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
        protected IList<TM> GetList(ObParameterBase param, IObSort sort, int? pageSize, int? pageIndex, out int count)
        {
            return GetList(null, param, null, null, sort, pageSize, pageIndex, out count);
        }

        protected IList<TM> GetList(IObJoin join, ObParameterBase param, IObSort sort, int? pageSize, int? pageIndex, out int count)
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
        protected IList<TM> GetList(ObParameterBase param, IObGroup group, ObParameterBase groupParam, IObSort sort)
        {
            return GetList(null, param, group, groupParam, sort, null, null, out _);
        }

        protected IList<TM> GetList(IObJoin join, ObParameterBase param, IObGroup group, ObParameterBase groupParam, IObSort sort)
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
        protected IList<TM> GetList(IObJoin join, ObParameterBase param, IObGroup group, ObParameterBase groupParam, IObSort sort, int? pageSize, int? pageIndex, out int count)
        {
            var total = 0;
            OnListing(ref param);
            var doModel = (DoModelAttribute)typeof(TM).GetCustomAttribute(typeof(DoModelAttribute), true);
            var sourceList = new List<TM>();
            if (pageSize.HasValue)
            {
                if (!pageIndex.HasValue)
                {
                    pageIndex = 1;
                }
                if (doModel != null)
                {
                    Parallel.For(0, BaseDals.Count, new ParallelOptions
                    {
                        MaxDegreeOfParallelism = BaseDals.Count
                    }, i =>
                    {
                        var subList = BaseDals[i].Query(join, param, group, groupParam, sort).ToList(pageSize.Value * pageIndex.Value, 1, out var c);
                        lock (sourceList)
                        {
                            total += c;
                            sourceList.AddRange(subList);
                        }
                    });
                }
                else
                {
                    var index = new Random((int) DateTime.Now.Ticks).Next(0, BaseDals.Count - 1);
                    sourceList = (List<TM>)BaseDals[index].Query(join, param, group, groupParam, sort).ToList(pageSize.Value, pageIndex.Value, out total);
                }
            }
            else
            {
                if (doModel != null)
                {
                    Parallel.For(0, BaseDals.Count, new ParallelOptions
                    {
                        MaxDegreeOfParallelism = BaseDals.Count
                    }, i =>
                    {
                        var query = BaseDals[i].Query(join, param, group, groupParam, sort);
                        var subList = query.ToList();
                        lock (sourceList)
                        {
                            total += subList.Count;
                            sourceList.AddRange(subList);
                        }
                    });
                }
                else
                {
                    var index = new Random((int)DateTime.Now.Ticks).Next(0, BaseDals.Count - 1);
                    sourceList = (List<TM>)BaseDals[index].Query(join, param, group, groupParam, sort).ToList();
                    total = sourceList.Count;
                }
            }
            IList<TM> list;
            if (sort != null && doModel != null)
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

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="requestParams"></param>
        /// <returns></returns>
        protected IList<TM> GetList(ObParameterBase param, IDictionary<string, object> requestParams)
        {
            return GetList(param, requestParams, null, null);
        }

        protected IList<TM> GetList(IObJoin join, ObParameterBase param, IDictionary<string, object> requestParams)
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
        protected IList<TM> GetList(ObParameterBase param, IDictionary<string, object> requestParams,
            IObSort sort, IDictionary<string, string> requestSorts)
        {
            return GetList(param, requestParams, sort, requestSorts, null, null, out _);
        }

        protected IList<TM> GetList(IObJoin join, ObParameterBase param, IDictionary<string, object> requestParams,
            IObSort sort, IDictionary<string, string> requestSorts)
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
        protected IList<TM> GetList(ObParameterBase param, IDictionary<string, object> requestParams,
            IObSort sort, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, out int count)
        {
            return GetList(param, requestParams, null, null, null, sort, requestSorts, pageSize, pageIndex, out count);
        }

        protected IList<TM> GetList(IObJoin join, ObParameterBase param, IDictionary<string, object> requestParams,
            IObSort sort, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, out int count)
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
        /// <param name="s"></param>
        /// <param name="requestSorts"></param>
        /// <returns></returns>
        protected IList<TM> GetList(ObParameterBase param, IDictionary<string, object> requestParams, IObGroup group,
            ObParameterBase groupParam, IDictionary<string, object> requestGroupParams,
            IObSort s, IDictionary<string, string> requestSorts)
        {
            GetList(ref param, requestParams, ref groupParam, requestGroupParams, ref s, requestSorts);
            return GetList(param, group, groupParam, s);
        }

        protected IList<TM> GetList(IObJoin join, ObParameterBase param, IDictionary<string, object> requestParams, IObGroup group,
            ObParameterBase gp, IDictionary<string, object> requestGroupParams,
            IObSort s, IDictionary<string, string> requestSorts)
        {
            GetList(ref param, requestParams, ref gp, requestGroupParams, ref s, requestSorts);
            return GetList(join, param, group, gp, s);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="requestParams"></param>
        /// <param name="group"></param>
        /// <param name="groupParam"></param>
        /// <param name="requestGroupParams"></param>
        /// <param name="s"></param>
        /// <param name="requestSorts"></param>
        /// <param name="pageSize"></param>
        /// <param name="pageIndex"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        protected IList<TM> GetList(ObParameterBase param, IDictionary<string, object> requestParams, IObGroup group,
            ObParameterBase groupParam, IDictionary<string, object> requestGroupParams,
            IObSort s, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, out int count)
        {
            GetList(ref param, requestParams, ref groupParam, requestGroupParams, ref s, requestSorts);
            return GetList(null, param, group, groupParam, s, pageSize, pageIndex, out count);
        }

        protected IList<TM> GetList(IObJoin join, ObParameterBase param, IDictionary<string, object> requestParams, IObGroup group,
            ObParameterBase gp, IDictionary<string, object> requestGroupParams,
            IObSort s, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, out int count)
        {
            GetList(ref param, requestParams, ref gp, requestGroupParams, ref s, requestSorts);
            return GetList(join, param, group, gp, s, pageSize, pageIndex, out count);
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
        protected virtual void GetList(ref ObParameterBase param, IDictionary<string, object> requestParams,
            ref ObParameterBase groupParam, IDictionary<string, object> requestGroupParams,
            ref IObSort sort, IDictionary<string, string> requestSorts)
        { }

        /// <summary>
        /// 获取模型数据
        /// </summary>
        /// <param name="param"></param>
        /// <returns></returns>
        protected TM GetModel(ObParameterBase param)
        {
            OnModeling(ref param);
            TM model = null;
            Parallel.For(0, BaseDals.Count, new ParallelOptions
            {
                MaxDegreeOfParallelism = BaseDals.Count
            }, i =>
            {
                var ret = BaseDals[i].Query(param).ToModel();
                if (ret != null)
                {
                    model = ret;
                }
            });
            OnModeled(model);
            return model;
        }

        /// <summary>
        /// 获取模型数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="sort"></param>
        /// <returns></returns>
        protected TM GetModel(ObParameterBase param, ObSortBase sort)
        {
            OnModeling(ref param);
            TM model;
            var list = new List<TM>();
            Parallel.For(0, BaseDals.Count, new ParallelOptions
            {
                MaxDegreeOfParallelism = BaseDals.Count
            }, i =>
            {
                var m = BaseDals[i].Query(param, sort).ToModel();
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
                    var properties = $"{dbSort.TableName}_{dbSort.ObProperty.PropertyName}".Split('_').Skip(1).ToArray();
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
            OnModeled(model);
            return model;
        }

        /// <summary>
        /// 判断是否存在
        /// </summary>
        /// <param name="param"></param>
        /// <returns></returns>
        protected bool Exists(ObParameterBase param)
        {
            OnExistling(ref param);
            var ret = false;
            Parallel.For(0, BaseDals.Count, new ParallelOptions
            {
                MaxDegreeOfParallelism = BaseDals.Count
            }, i =>
            {
                if (BaseDals[i].Query(param).Exists())
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
        /// <param name="param"></param>
        protected virtual void OnGlobalExecuting(ref ObParameterBase param)
        {
        }

        /// <summary>
        /// 添加前
        /// </summary>
        /// <param name="model"></param>
        /// <param name="param"></param>
        protected virtual void OnAdding(TM model, ref ObParameterBase param)
        {
            OnGlobalExecuting(ref param);
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
        /// <param name="param"></param>
        protected virtual void OnUpdateing(TM model, ref ObParameterBase param)
        {
            OnGlobalExecuting(ref param);
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
        /// <param name="param"></param>
        protected virtual void OnDeleteing(ref ObParameterBase param)
        {
            OnGlobalExecuting(ref param);
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
        /// <param name="param"></param>
        protected virtual void OnListing(ref ObParameterBase param)
        {
            OnGlobalExecuting(ref param);
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
        /// <param name="param"></param>
        protected virtual void OnModeling(ref ObParameterBase param)
        {
            OnGlobalExecuting(ref param);
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
        /// <param name="param"></param>
        protected virtual void OnExistling(ref ObParameterBase param)
        {
            OnGlobalExecuting(ref param);
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

        protected async Task<int> UpdateAsync(TM model, ObParameterBase param)
        {
            return await Task.Run(() => Update(model, param));
        }

        protected async Task<int> DeleteAsync(ObParameterBase param)
        {
            return await Task.Run(() => Delete(param));
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <returns></returns>
        protected async Task<IList<TM>> GetListAsync(ObParameterBase param)
        {
            return await GetListAsync(param, (IObSort)null);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, ObParameterBase param)
        {
            return await GetListAsync(join, param, (IObSort)null);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="sort"></param>
        /// <returns></returns>
        protected async Task<IList<TM>> GetListAsync(ObParameterBase param, IObSort sort)
        {
            return await GetListAsync(param, sort, null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, ObParameterBase param, IObSort sort)
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
        protected async Task<IList<TM>> GetListAsync(ObParameterBase param, IObSort sort, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await GetListAsync(null, param, null, null, sort, pageSize, pageIndex, countAccessor);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, ObParameterBase param, IObSort sort, int? pageSize, int? pageIndex, Action<int> countAccessor = null)
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
        protected async Task<IList<TM>> GetListAsync(ObParameterBase param, IObGroup group, ObParameterBase groupParam, IObSort sort)
        {
            return await GetListAsync(null, param, group, groupParam, sort, null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, ObParameterBase param, IObGroup group, ObParameterBase groupParam, IObSort sort)
        {
            return await GetListAsync(join, param, group, groupParam, sort, null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, ObParameterBase param, IObGroup group, ObParameterBase groupParam,
            IObSort sort, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            var ret = await Task.Run(() =>
            {
                var list = GetList(join, param, group, groupParam, sort, pageSize, pageIndex, out var count);
                countAccessor?.Invoke(count);
                return list;
            });
            return ret;
        }


        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="requestParams"></param>
        /// <returns></returns>
        protected async Task<IList<TM>> GetListAsync(ObParameterBase param, IDictionary<string, object> requestParams)
        {
            return await GetListAsync(param, requestParams, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, ObParameterBase param, IDictionary<string, object> requestParams)
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
        protected async Task<IList<TM>> GetListAsync(ObParameterBase param, IDictionary<string, object> requestParams,
            IObSort sort, IDictionary<string, string> requestSorts)
        {
            return await GetListAsync(param, requestParams, sort, requestSorts, null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, ObParameterBase param, IDictionary<string, object> requestParams,
            IObSort sort, IDictionary<string, string> requestSorts)
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
        protected async Task<IList<TM>> GetListAsync(ObParameterBase param, IDictionary<string, object> requestParams,
            IObSort sort, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await GetListAsync(param, requestParams, null, null, null, sort, requestSorts, pageSize, pageIndex, countAccessor);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, ObParameterBase param, IDictionary<string, object> requestParams,
            IObSort sort, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, Action<int> countAccessor)
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
        /// <param name="s"></param>
        /// <param name="requestSorts"></param>
        /// <returns></returns>
        protected async Task<IList<TM>> GetListAsync(ObParameterBase param, IDictionary<string, object> requestParams, IObGroup group,
            ObParameterBase groupParam, IDictionary<string, object> requestGroupParams,
            IObSort s, IDictionary<string, string> requestSorts)
        {
            GetList(ref param, requestParams, ref groupParam, requestGroupParams, ref s, requestSorts);
            return await GetListAsync(param, group, groupParam, s);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, ObParameterBase param, IDictionary<string, object> requestParams, IObGroup group,
            ObParameterBase gp, IDictionary<string, object> requestGroupParams,
            IObSort s, IDictionary<string, string> requestSorts)
        {
            GetList(ref param, requestParams, ref gp, requestGroupParams, ref s, requestSorts);
            return await GetListAsync(join, param, group, gp, s);
        }

        /// <summary>
        /// 获取列表数据
        /// </summary>
        /// <param name="param"></param>
        /// <param name="requestParams"></param>
        /// <param name="group"></param>
        /// <param name="groupParam"></param>
        /// <param name="requestGroupParams"></param>
        /// <param name="s"></param>
        /// <param name="requestSorts"></param>
        /// <param name="pageSize"></param>
        /// <param name="pageIndex"></param>
        /// <param name="countAccessor"></param>
        /// <returns></returns>
        protected async Task<IList<TM>> GetListAsync(ObParameterBase param, IDictionary<string, object> requestParams, IObGroup group,
            ObParameterBase groupParam, IDictionary<string, object> requestGroupParams,
            IObSort s, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            GetList(ref param, requestParams, ref groupParam, requestGroupParams, ref s, requestSorts);
            return await GetListAsync(null, param, group, groupParam, s, pageSize, pageIndex, countAccessor);
        }

        protected async Task<IList<TM>> GetListAsync(IObJoin join, ObParameterBase param, IDictionary<string, object> requestParams, IObGroup group,
            ObParameterBase gp, IDictionary<string, object> requestGroupParams,
            IObSort s, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            GetList(ref param, requestParams, ref gp, requestGroupParams, ref s, requestSorts);
            return await GetListAsync(join, param, group, gp, s, pageSize, pageIndex, countAccessor);
        }

        protected async Task<TM> GetModelAsync(ObParameterBase param)
        {
            return await Task.Run(() => GetModel(param));
        }

        protected async Task<TM> GetModelAsync(ObParameterBase param, ObSortBase sort)
        {
            return await Task.Run(() => GetModel(param, sort));
        }

        protected async Task<bool> ExistsAsync(ObParameterBase param)
        {
            return await Task.Run(() => Exists(param));
        }

        protected async Task<int> NewIdentityAsync()
        {
            return await Task.Run(() => NewIdentity());
        }
    }
}
