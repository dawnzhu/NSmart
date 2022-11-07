using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DotNet.Standard.NParsing.Factory;
using DotNet.Standard.NParsing.Interface;
using System.Linq;
using System.Reflection;
using DotNet.Standard.NParsing.ComponentModel;
using DotNet.Standard.NParsing.Utilities;
using DotNet.Standard.NSmart.ComponentModel;
using DotNet.Standard.NSmart.Utilities;
using System.Security.Principal;

namespace DotNet.Standard.NSmart
{
    public abstract class DoServiceBase<TM, TT, TH, TQ>
        where TM : DoModelBase
        where TT : DoTermBase
        where TH : IObHelper<TM>
        where TQ : IObQueryable<TM>
    {
        protected TH MainDal;
        protected IList<TH> BaseDals;
        private readonly DoConfigDbs _doConfigDb;

        /// <summary>
        /// 拆分数据关键字
        /// </summary>
        public long SplitDataKey { get; set; }

        protected DoServiceBase(string dbsName) : this(DoConfig.DoOptions.DbConfigs, dbsName)
        { }

        protected DoServiceBase(Dictionary<string, DoConfigDbs> dbConfigs, string dbsName)
        {
            BaseDals = new List<TH>();
            if (dbConfigs == null || dbConfigs.Count == 0)
            {
                throw new Exception("使用NSmart框架后，数据库连接配置必须在ConnectionConfigs中配置。");
            }
            /*var dbsName = "MainDbs";*/
            var doService = (DoServiceAttribute)GetType().GetCustomAttribute(typeof(DoServiceAttribute), true);
            if (doService != null)
            {
                dbsName = doService.DbsName;
            }
            if (!dbConfigs.ContainsKey(dbsName))
            {
                throw new Exception($"{dbsName}在ConnectionConfigs配置中不存在。");
            }
            _doConfigDb = dbConfigs[dbsName];
            if (typeof(TH) == typeof(IObHelper<TM, TT>))
            {
                MainDal = (TH)ObHelper.Create<TM, TT>(_doConfigDb.ConnectionString, _doConfigDb.ProviderName);
            }
            else
            {
                MainDal = (TH)ObHelper.Create<TM>(_doConfigDb.ConnectionString, _doConfigDb.ProviderName);
            }
            foreach (var config in _doConfigDb.Adds)
            {
                TH dal;
                if (typeof(TH) == typeof(IObHelper<TM, TT>))
                {
                    dal = (TH)(config.ReadConnectionString == config.WriteConnectionString
                        ? ObHelper.Create<TM, TT>(config.ReadConnectionString, config.ProviderName)
                        : ObHelper.Create<TM, TT>(config.ReadConnectionString, config.WriteConnectionString, config.ProviderName));
                }
                else
                {
                    dal = (TH)(config.ReadConnectionString == config.WriteConnectionString
                        ? ObHelper.Create<TM>(config.ReadConnectionString, config.ProviderName)
                        : ObHelper.Create<TM>(config.ReadConnectionString, config.WriteConnectionString, config.ProviderName));
                }
                BaseDals.Add(dal);
            }
            if (BaseDals.Count == 0)
            {
                BaseDals.Add(MainDal);
            }
        }

        private bool GetDal(string type, out IObHelper<TM> dal)
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
                            index = (int)(SplitDataKey % BaseDals.Count);
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
                            index = (int)(SplitDataKey % BaseDals.Count);
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
                            index = (int)(SplitDataKey % BaseDals.Count);
                            break;
                    }
                    break;
            }
            dal = BaseDals[index];
            return true;
        }

        private TQ GetQueryable()
        {
            var queryable = (TQ)BaseDals.First().Queryable();
            queryable.CreateEmptyObject = false;
            queryable.Join();
            return queryable;
        }

        #region 触发事件

        /// <summary>
        /// 查询前
        /// </summary>
        /// <param name="queryable"></param>
        protected virtual void OnGlobalExecuting(ref TQ queryable)
        {
            if (queryable == null)
            {
                queryable = GetQueryable();
            }
        }

        /// <summary>
        /// 添加前
        /// </summary>
        /// <param name="model"></param>
        /// <param name="queryable"></param>
        protected virtual void OnAdding(TM model, ref TQ queryable)
        {
            OnGlobalExecuting(ref queryable);
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
        /// <param name="queryable"></param>
        protected virtual void OnUpdating(TM model, ref TQ queryable)
        {
            OnGlobalExecuting(ref queryable);
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
        /// <param name="queryable"></param>
        protected virtual void OnDeleting(ref TQ queryable)
        {
            OnGlobalExecuting(ref queryable);
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
        /// <param name="queryable"></param>
        protected virtual void OnListing(ref TQ queryable)
        {
            OnGlobalExecuting(ref queryable);
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
        /// <param name="queryable"></param>
        protected virtual void OnModeling(ref TQ queryable)
        {
            OnGlobalExecuting(ref queryable);
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
        /// <param name="queryable"></param>
        protected virtual void OnExisting(ref TQ queryable)
        {
            OnGlobalExecuting(ref queryable);
        }

        /// <summary>
        /// 判断是否存在后
        /// </summary>
        /// <param name="result"></param>
        protected virtual void OnExisted(bool result)
        {
        }

        #endregion

        /// <summary>
        /// Param
        /// </summary>
        /// <param name="requestParams"></param>
        /// <param name="requestGroupParams"></param>
        /// <param name="requestSorts"></param>
        /// <param name="queryable"></param>
        protected virtual void GetList(ref TQ queryable, IDictionary<string, object> requestParams, IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts)
        {
            if (queryable == null)
            {
                queryable = GetQueryable();
            }
        }

        protected long NewIdentity()
        {
            var property = typeof(TM).GetProperty("Id", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            var obSettled = (ObSettledAttribute)property?.GetCustomAttribute(typeof(ObSettledAttribute), true);
            var obIdentity = (ObIdentityAttribute)property?.GetCustomAttribute(typeof(ObIdentityAttribute), true);
            if (obSettled == null && obIdentity != null && obIdentity.ObIdentity == ObIdentity.Program)
            {
                return NewIdentity(obIdentity);
            }
            return 0;
        }

        private long NewIdentity(ObIdentityAttribute obIdentity)
        {
            var obRedefine = ObRedefine.Create<ObjectsToMaxIdInfo>($"{typeof(TM).ToTableName()}ToMaxID");
            var dal = ObHelper.Create<ObjectsToMaxIdInfo>(_doConfigDb.ConnectionString, _doConfigDb.ProviderName, obRedefine);
            long id;
            using var ot = ObConnection.BeginTransaction(_doConfigDb.ConnectionString, _doConfigDb.ProviderName);
            try
            {
                var model = dal.Query(ot).ToModel();
                if (model == null)
                {
                    model = new ObjectsToMaxIdInfo
                    {
                        MaxId = obIdentity.Seed
                    };
                    dal.Add(ot, model);
                }
                else
                {
                    model.MaxId += obIdentity.Increment;
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
            return id;
        }

        protected async Task<long> NewIdentityAsync()
        {
            return await Task.Run(NewIdentity);
        }

        protected async Task<long> NewIdentityAsync(ObIdentityAttribute obIdentity)
        {
            return await Task.Run(() => NewIdentity(obIdentity));
        }

        /// <summary>
        /// 添加
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        protected object Add(TM model)
        {
            var keySelector = default(TQ);
            OnAdding(model, ref keySelector);
            object ret = null;
            var property = typeof(TM).GetProperty("Id", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            var obSettled = (ObSettledAttribute)property?.GetCustomAttribute(typeof(ObSettledAttribute), true);
            var obIdentity = (ObIdentityAttribute)property?.GetCustomAttribute(typeof(ObIdentityAttribute), true);
            if (obSettled == null && obIdentity != null && obIdentity.ObIdentity == ObIdentity.Program)
            {
                model.Id = NewIdentity(obIdentity);
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

        protected async Task<object> AddAsync(TM model)
        {
            return await Task.Run(() => Add(model));
        }

        protected int UpdateAll(TM model)
        {
            return Update(model, null);
        }

        protected int Update(TM model, Func<TQ, TQ> keySelector)
        {
            var queryable = keySelector != null ? keySelector(GetQueryable()) : GetQueryable();
            return Update(model, queryable);
        }

        protected int Update(TM model, TQ queryable)
        {
            queryable ??= GetQueryable();
            OnUpdating(model, ref queryable);
            var join = queryable.ObJoin;
            var param = queryable.ObParameter;
            var ret = 0;
            if (GetDal("MOD", out var dal))
            {
                ret = dal.Update(model, join, param);
            }
            else
            {
                Parallel.For(0, BaseDals.Count, new ParallelOptions
                {
                    MaxDegreeOfParallelism = BaseDals.Count
                }, i =>
                {
                    var r = BaseDals[i].Update(model, join, param);
                    if (r > ret)
                    {
                        ret = r;
                    }
                });
            }
            OnUpdated(model, ret);
            return ret;
        }

        protected async Task<int> UpdateAllAsync(TM model)
        {
            return await Task.Run(() => UpdateAll(model));
        }

        protected async Task<int> UpdateAsync(TM model, Func<TQ, TQ> keySelector)
        {
            return await Task.Run(() => Update(model, keySelector));
        }

        protected async Task<int> UpdateAsync(TM model, TQ queryable)
        {
            return await Task.Run(() => Update(model, queryable));
        }

        protected int DeleteAll()
        {
            return Delete(null);
        }

        protected int Delete(Func<TQ, TQ> keySelector)
        {
            var queryable = keySelector != null ? keySelector(GetQueryable()) : GetQueryable();
            return Delete(queryable);
        }

        protected int Delete(TQ queryable)
        {
            queryable ??= GetQueryable();
            OnDeleting(ref queryable);
            var join = queryable.ObJoin;
            var param = queryable.ObParameter;
            var ret = 0;
            if (GetDal("MOD", out var dal))
            {
                ret = dal.Delete(join, param);
            }
            else
            {
                Parallel.For(0, BaseDals.Count, new ParallelOptions
                {
                    MaxDegreeOfParallelism = BaseDals.Count
                }, i =>
                {
                    var r = BaseDals[i].Delete(join, param);
                    if (r > ret)
                    {
                        ret = r;
                    }
                });
            }
            OnDeleted(ret);
            return ret;
        }

        protected async Task<int> DeleteAllAsync()
        {
            return await Task.Run(DeleteAll);
        }

        protected async Task<int> DeleteAsync(Func<TQ, TQ> keySelector)
        {
            return await Task.Run(() => Delete(keySelector));
        }

        protected async Task<int> DeleteAsync(TQ queryable)
        {
            return await Task.Run(() => Delete(queryable));
        }

        protected IList<TM> GetListAll()
        {
            return GetList(null, null, null, out _);
        }

        protected IList<TM> GetList(Func<TQ, TQ> keySelector)
        {
            return GetList(keySelector, null, null, out _);
        }

        protected IList<TM> GetList(TQ queryable)
        {
            return GetList(queryable, null, null, out _);
        }

        protected IList<TM> GetList(int? pageSize, int? pageIndex, out int count)
        {
            return GetList(null,null, null, null, pageSize, pageIndex, out count);
        }

        protected IList<TM> GetList(Func<TQ, TQ> keySelector, int? pageSize, int? pageIndex, out int count)
        {
            return GetList(keySelector, null, null, null, pageSize, pageIndex, out count);
        }

        protected IList<TM> GetList(TQ queryable, int? pageSize, int? pageIndex, out int count)
        {
            return GetList(queryable, null, null, null, pageSize, pageIndex, out count);
        }

        protected IList<TM> GetList(IDictionary<string, object> requestParams,
            IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, out int count)
        {
            return GetList(null, requestParams, null, requestSorts, pageSize, pageIndex, out count);
        }

        protected IList<TM> GetList(Func<TQ, TQ> keySelector, IDictionary<string, object> requestParams,
            IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, out int count)
        {
            return GetList(keySelector, requestParams, null, requestSorts, pageSize, pageIndex, out count);
        }

        protected IList<TM> GetList(TQ queryable, IDictionary<string, object> requestParams,
            IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, out int count)
        {
            return GetList(queryable, requestParams, null, requestSorts, pageSize, pageIndex, out count);
        }

        protected IList<TM> GetList(IDictionary<string, object> requestParams,
            IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, out int count)
        {
            return GetList(null, requestParams, requestGroupParams, requestSorts, pageSize, pageIndex, out count);
        }

        protected IList<TM> GetList(Func<TQ, TQ> keySelector, IDictionary<string, object> requestParams,
            IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, out int count)
        {
            var queryable = keySelector != null ? keySelector(GetQueryable()) : GetQueryable();
            return GetList(queryable, requestParams, requestGroupParams, requestSorts, pageSize, pageIndex, out count);
        }

        protected IList<TM> GetList(TQ queryable, IDictionary<string, object> requestParams,
            IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, out int count)
        {
            queryable ??= GetQueryable();
            GetList(ref queryable, requestParams, requestGroupParams, requestSorts);
            var total = 0;
            OnListing(ref queryable);
            var join = queryable.ObJoin;
            var param = queryable.ObParameter;
            var group = queryable.ObGroup;
            var groupParam = queryable.ObGroupParameter;
            var sort = queryable.ObSort;
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
                    var query = dal.Query(join, param, group, groupParam, sort);
                    query.CreateEmptyObject = queryable.CreateEmptyObject;
                    sourceList = (List<TM>)query.ToList(pageSize.Value, pageIndex.Value, out total);
                }
                else
                {
                    Parallel.For(0, BaseDals.Count, new ParallelOptions
                    {
                        MaxDegreeOfParallelism = BaseDals.Count
                    }, i =>
                    {
                        var query = BaseDals[i].Query(join, param, group, groupParam, sort);
                        query.CreateEmptyObject = queryable.CreateEmptyObject;
                        var subList = query.ToList(pageSize.Value * pageIndex.Value, 1, out var c);
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
                    var query = dal.Query(join, param, group, groupParam, sort);
                    query.CreateEmptyObject = queryable.CreateEmptyObject;
                    sourceList = (List<TM>)query.ToList();
                    total = sourceList.Count;
                }
                else
                {
                    Parallel.For(0, BaseDals.Count, new ParallelOptions
                    {
                        MaxDegreeOfParallelism = BaseDals.Count
                    }, i =>
                    {
                        var query = BaseDals[i].Query(join, param, group, groupParam, sort);
                        query.CreateEmptyObject = queryable.CreateEmptyObject;
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

        protected async Task<IList<TM>> GetListAllAsync()
        {
            return await Task.Run(GetListAll);
        }

        protected async Task<IList<TM>> GetListAsync(Func<TQ, TQ> keySelector)
        {
            return await Task.Run(() => GetList(keySelector));
        }

        protected async Task<IList<TM>> GetListAsync(TQ queryable)
        {
            return await Task.Run(() => GetList(queryable));
        }

        protected async Task<IList<TM>> GetListAsync(int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await Task.Run(() =>
            {
                var list = GetList(pageSize, pageIndex, out var count);
                countAccessor?.Invoke(count);
                return list;
            });
        }

        protected async Task<IList<TM>> GetListAsync(Func<TQ, TQ> keySelector, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await Task.Run(() =>
            {
                var list = GetList(keySelector, pageSize, pageIndex, out var count);
                countAccessor?.Invoke(count);
                return list;
            });
        }

        protected async Task<IList<TM>> GetListAsync(TQ queryable, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await Task.Run(() =>
            {
                var list = GetList(queryable, pageSize, pageIndex, out var count);
                countAccessor?.Invoke(count);
                return list;
            });
        }

        protected async Task<IList<TM>> GetListAsync(IDictionary<string, object> requestParams, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await Task.Run(() =>
            {
                var list = GetList(requestParams, requestSorts, pageSize, pageIndex, out var count);
                countAccessor?.Invoke(count);
                return list;
            });
        }

        protected async Task<IList<TM>> GetListAsync(Func<TQ, TQ> keySelector,
            IDictionary<string, object> requestParams, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await Task.Run(() =>
            {
                var list = GetList(keySelector, requestParams, requestSorts, pageSize, pageIndex, out var count);
                countAccessor?.Invoke(count);
                return list;
            });
        }

        protected async Task<IList<TM>> GetListAsync(TQ queryable,
            IDictionary<string, object> requestParams, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await Task.Run(() =>
            {
                var list = GetList(queryable, requestParams, requestSorts, pageSize, pageIndex, out var count);
                countAccessor?.Invoke(count);
                return list;
            });
        }

        protected async Task<IList<TM>> GetListAsync(IDictionary<string, object> requestParams,
            IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await Task.Run(() =>
            {
                var list = GetList(requestParams, requestGroupParams, requestSorts, pageSize, pageIndex, out var count);
                countAccessor?.Invoke(count);
                return list;
            });
        }

        protected async Task<IList<TM>> GetListAsync(Func<TQ, TQ> keySelector, IDictionary<string, object> requestParams,
            IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await Task.Run(() =>
            {
                var list = GetList(keySelector, requestParams, requestGroupParams, requestSorts, pageSize, pageIndex, out var count);
                countAccessor?.Invoke(count);
                return list;
            });
        }

        protected async Task<IList<TM>> GetListAsync(TQ queryable, IDictionary<string, object> requestParams,
            IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await Task.Run(() =>
            {
                var list = GetList(queryable, requestParams, requestGroupParams, requestSorts, pageSize, pageIndex, out var count);
                countAccessor?.Invoke(count);
                return list;
            });
        }

        protected TM GetModel()
        {
            return GetModel(null);
        }

        protected TM GetModel(Func<TQ, TQ> keySelector)
        {
            var queryable = keySelector != null ? keySelector(GetQueryable()) : GetQueryable();
            return GetModel(queryable);
        }

        protected TM GetModel(TQ queryable)
        {
            queryable ??= GetQueryable();
            OnModeling(ref queryable);
            var join = queryable.ObJoin;
            var param = queryable.ObParameter;
            var group = queryable.ObGroup;
            var groupParam = queryable.ObGroupParameter;
            var sort = queryable.ObSort;
            TM model = null;
            Parallel.For(0, BaseDals.Count, new ParallelOptions
            {
                MaxDegreeOfParallelism = BaseDals.Count
            }, i =>
            {
                var query = BaseDals[i].Query(join, param, group, groupParam, sort);
                query.CreateEmptyObject = queryable.CreateEmptyObject;
                var ret = query.ToModel();
                if (ret != null)
                {
                    model = ret;
                }
            });
            OnModeled(model);
            return model;
        }

        protected async Task<TM> GetModelAsync()
        {
            return await Task.Run(GetModel);
        }

        protected async Task<TM> GetModelAsync(Func<TQ, TQ> keySelector)
        {
            return await Task.Run(() => GetModel(keySelector));
        }

        protected async Task<TM> GetModelAsync(TQ queryable)
        {
            return await Task.Run(() => GetModel(queryable));
        }

        protected bool Exists()
        {
            return Exists(null);
        }

        protected bool Exists(Func<TQ, TQ> keySelector)
        {
            var queryable = keySelector != null ? keySelector(GetQueryable()) ?? GetQueryable() : GetQueryable();
            return Exists(queryable);
        }

        protected bool Exists(TQ queryable)
        {
            queryable ??= GetQueryable();
            OnExisting(ref queryable);
            var join = queryable.ObJoin;
            var param = queryable.ObParameter;
            var group = queryable.ObGroup;
            var groupParam = queryable.ObGroupParameter;
            var sort = queryable.ObSort;
            var ret = false;
            Parallel.For(0, BaseDals.Count, new ParallelOptions
            {
                MaxDegreeOfParallelism = BaseDals.Count
            }, i =>
            {
                if (BaseDals[i].Query(join, param, group, groupParam, sort).Exists())
                {
                    ret = true;
                }
            });
            OnExisted(ret);
            return ret;
        }

        protected async Task<bool> ExistsAsync()
        {
            return await Task.Run(Exists);
        }

        protected async Task<bool> ExistsAsync(Func<TQ, TQ> keySelector)
        {
            return await Task.Run(() => Exists(keySelector));
        }

        protected async Task<bool> ExistsAsync(TQ queryable)
        {
            return await Task.Run(() => Exists(queryable));
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

    }

    public abstract class DoServiceBase<TM> : DoServiceBase<TM, DoTermBase, IObHelper<TM>, IObQueryable<TM>>
        where TM : DoModelBase
    {
        protected DoServiceBase() : this("MainDbs")
        {
            //Update(new TM(), o => o.Where(a => a.Id == 0).Join(a => a));
        }

        protected DoServiceBase(string dbsName) : base(dbsName)
        { }

        protected DoServiceBase(Dictionary<string, DoConfigDbs> doConfigDbs) : this(doConfigDbs, "MainDbs")
        { }

        protected DoServiceBase(Dictionary<string, DoConfigDbs> doConfigDbs, string dbsName) : base(doConfigDbs, dbsName)
        { }
    }

    public abstract class DoServiceBase<TM, TT> : DoServiceBase<TM, TT, IObHelper<TM, TT>, IObQueryable<TM, TT>>
        where TM : DoModelBase
        where TT : DoTermBase, new()
    {
        protected TT Term;

        protected DoServiceBase() : this(null, "MainDbs")
        {
            //Update(new TM(), o => o.Where(a => a.Id == 0).Join(a => a));
        }

        protected DoServiceBase(TT term) : this(term, "MainDbs")
        { }

        protected DoServiceBase(string dbsName) : this(null, dbsName)
        { }

        protected DoServiceBase(TT term, string dbsName) : base(dbsName)
        {
            Term = term ?? new TT();
        }

        protected DoServiceBase(Dictionary<string, DoConfigDbs> doConfigDbs) : this(null, doConfigDbs, "MainDbs")
        { }

        protected DoServiceBase(TT term, Dictionary<string, DoConfigDbs> doConfigDbs) : this(term, doConfigDbs, "MainDbs")
        { }

        protected DoServiceBase(TT term, Dictionary<string, DoConfigDbs> doConfigDbs, string dbsName) : base(doConfigDbs, dbsName)
        {
            Term = term ?? new TT();
        }
    }


    public abstract class DoServiceBase2<TM, TT>
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
        public long SplitDataKey { get; set; }

        protected DoServiceBase2() : this(null, DoConfig.Get(), "MainDbs")
        { }

        protected DoServiceBase2(TT term) : this(term, DoConfig.Get(), "MainDbs")
        { }

        protected DoServiceBase2(string dbsName) : this(null, DoConfig.Get(), dbsName)
        { }

        protected DoServiceBase2(TT term, string dbsName) : this(term, DoConfig.Get(), dbsName)
        { }

        protected DoServiceBase2(Dictionary<string, DoConfigDbs> doConfigDbs) : this(null, doConfigDbs, "MainDbs")
        { }

        protected DoServiceBase2(TT term, Dictionary<string, DoConfigDbs> doConfigDbs) : this(term, doConfigDbs, "MainDbs")
        { }

        protected DoServiceBase2(TT term, Dictionary<string, DoConfigDbs> doConfigDbs, string dbsName)
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
                            index = (int)(SplitDataKey % BaseDals.Count);
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
                            index = (int)(SplitDataKey % BaseDals.Count);
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
                            index = (int)(SplitDataKey % BaseDals.Count);
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
            var property = typeof(TM).GetProperty("Id", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            var obSettled = (ObSettledAttribute)property?.GetCustomAttribute(typeof(ObSettledAttribute), true);
            var obIdentity = (ObIdentityAttribute)property?.GetCustomAttribute(typeof(ObIdentityAttribute), true);
            if (obSettled == null && obIdentity != null && obIdentity.ObIdentity == ObIdentity.Program)
            {
                model.Id = NewIdentity(obIdentity);
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
        protected IList<TM> GetList(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector)
        {
            return GetList(keySelector, null, null, out _);
        }

        protected IList<TM> GetList(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector, int? pageSize, int? pageIndex, out int count)
        {
            var query = keySelector != null
                ? keySelector(BaseDals.First()) ?? BaseDals.First().Queryable()
                : BaseDals.First().Queryable();
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

        protected IList<TM> GetList(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector, IDictionary<string, object> requestParams)
        {
            return GetList(keySelector, requestParams, null, null, null, null, out _);
        }

        protected IList<TM> GetList(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector, IDictionary<string, object> requestParams, 
            IDictionary<string, object> requestGroupParams)
        {
            return GetList(keySelector, requestParams, requestGroupParams, null, null, null, out _);
        }

        protected IList<TM> GetList(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector, IDictionary<string, object> requestParams,
            IDictionary<string, string> requestSorts)
        {
            return GetList(keySelector, requestParams, null, requestSorts, null, null, out _);
        }

        protected IList<TM> GetList(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector, IDictionary<string, object> requestParams, 
            IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts)
        {
            return GetList(keySelector, requestParams, requestGroupParams, requestSorts, null, null, out _);
        }

        protected IList<TM> GetList(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector, IDictionary<string, object> requestParams,
            IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, out int count)
        {
            return GetList(keySelector, requestParams, null, requestSorts, pageSize, pageIndex, out count);
        }

        protected IList<TM> GetList(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector, IDictionary<string, object> requestParams, 
            IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, out int count)
        {
            var query = keySelector != null
                ? keySelector(BaseDals.First()) ?? BaseDals.First().Queryable()
                : BaseDals.First().Queryable();
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

        protected TM GetModel(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector)
        {
            var query = keySelector != null
                ? keySelector(BaseDals.First()) ?? BaseDals.First().Queryable()
                : BaseDals.First().Queryable();
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

        protected bool Exists(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector)
        {
            var query = keySelector != null
                ? keySelector(BaseDals.First()) ?? BaseDals.First().Queryable()
                : BaseDals.First().Queryable();
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

        protected long NewIdentity()
        {
            var property = typeof(TM).GetProperty("Id", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            var obSettled = (ObSettledAttribute)property?.GetCustomAttribute(typeof(ObSettledAttribute), true);
            var obIdentity = (ObIdentityAttribute)property?.GetCustomAttribute(typeof(ObIdentityAttribute), true);
            if (obSettled == null && obIdentity != null && obIdentity.ObIdentity == ObIdentity.Program)
            {
                return NewIdentity(obIdentity);
            }
            return 0;
        }

        protected long NewIdentity(ObIdentityAttribute obIdentity)
        {
            var obRedefine = ObRedefine.Create<ObjectsToMaxIdInfo>($"{Term.ObTableName}ToMaxID");
            var dal = ObHelper.Create<ObjectsToMaxIdInfo>(_doConfigDb.ConnectionString, _doConfigDb.ProviderName, obRedefine);
            long id;
            using (var ot = ObConnection.BeginTransaction(_doConfigDb.ConnectionString, _doConfigDb.ProviderName))
            {
                try
                {
                    var model = dal.Query(ot).ToModel();
                    if (model == null)
                    {
                        model = new ObjectsToMaxIdInfo
                        {
                            MaxId = obIdentity.Seed
                        };
                        dal.Add(ot, model);
                    }
                    else
                    {
                        model.MaxId += obIdentity.Increment;
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

        protected async Task<IList<TM>> GetListAsync(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector)
        {
            return await GetListAsync(keySelector, (int?)null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector, int? pageSize, int? pageIndex, Action<int> countAccessor)
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
        protected async Task<IList<TM>> GetListAsync(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector, IDictionary<string, object> requestParams)
        {
            return await GetListAsync(keySelector, requestParams, null, null, null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector, IDictionary<string, object> requestParams, 
            IDictionary<string, object> requestGroupParams)
        {
            return await GetListAsync(keySelector, requestParams, requestGroupParams, null, null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector, IDictionary<string, object> requestParams,
            IDictionary<string, string> requestSorts)
        {
            return await GetListAsync(keySelector, requestParams, null, requestSorts, null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector, IDictionary<string, object> requestParams,
            IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts)
        {
            return await GetListAsync(keySelector, requestParams, requestGroupParams, requestSorts, null, null, null);
        }

        protected async Task<IList<TM>> GetListAsync(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector, IDictionary<string, object> requestParams, 
            IDictionary<string, string> requestSorts, int? pageSize, int? pageIndex, Action<int> countAccessor)
        {
            return await GetListAsync(keySelector, requestParams, null, requestSorts, pageSize, pageIndex, countAccessor);
        }

        protected async Task<IList<TM>> GetListAsync(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector, IDictionary<string, object> requestParams, 
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

        protected async Task<TM> GetModelAsync(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector)
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

        protected async Task<bool> ExistsAsync(Func<IObHelper<TM, TT>, IObQueryable<TM, TT>> keySelector)
        {
            return await Task.Run(() => Exists(keySelector));
        }

        protected async Task<long> NewIdentityAsync()
        {
            return await Task.Run(NewIdentity);
        }

        protected async Task<long> NewIdentityAsync(ObIdentityAttribute obIdentity)
        {
            return await Task.Run(() => NewIdentity(obIdentity));
        }
    }
}
