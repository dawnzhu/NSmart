using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using DotNet.Standard.NParsing.Factory;
using DotNet.Standard.NParsing.Interface;
using DotNet.Standard.NParsing.Utilities;
using DotNet.Standard.Common.Utilities;

namespace DotNet.Standard.NSmart.Utilities
{
    public static class DoParam
    {
        public static bool Initialized { get; private set; }

        public static void Initialize()
        {
            Initialize(DoParamConfig.Get());
        }

        public static void Initialize(this Dictionary<string, ParamConfig> config)
        {
            Config = config;
            Initialized = true;
        }

        public static Dictionary<string, ParamConfig> Config { get; private set; }

        public static T ToRequestParam<T>(this IEnumerable<string> strRequestJsons)
            where T : DoRequestParamBase, new()
        {
            return ToRequestParam<T>(strRequestJsons, "");
        }

        public static T ToRequestParam<T>(this IEnumerable<string> strRequestJsons, string url)
            where T: DoRequestParamBase, new ()
        {
            var rp = new T
            {
                Params = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            };
            var rpps = rp.GetType()
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Where(rpp => !string.Equals(rpp.Name, "Params"))
                .ToList();
            foreach (var strJson in strRequestJsons)
            {
                try
                {
                    var dict = strJson.ToObject<IDictionary<string, object>>();
                    foreach (var d in dict)
                    {
                        var rpp = rpps.FirstOrDefault(obj => string.Equals(obj.Name, d.Key, StringComparison.OrdinalIgnoreCase));
                        if (rpp != null)
                        {
                            if (string.Equals(rpp.Name, "Sorts", StringComparison.OrdinalIgnoreCase))
                            {
                                var dictSortFields = d.Value.ToJsonString().ToObject<Dictionary<string, string>>();
                                rp.Sorts = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                                foreach (var sortField in dictSortFields)
                                {
                                    rp.Sorts.Add(sortField.Key, sortField.Value);
                                }
                            }
                            else
                            {
                                rpp.SetValue(rp, d.Value.ToChangeType(rpp.PropertyType));
                            }
                        }
                        else
                        {
                            rp.Params.Add(d.Key, d.Value);
                        }
                    }
                }
                catch (Exception er)
                {
                    LogUtil.WriteLog(url + "\r\n" +strJson, er);
                }
            }
            return rp;
        }

        public static Dictionary<string, object> ToProxyArguments<T>(this T requestParam, Dictionary<string, object> args)
            where T : DoRequestParamBase
        {
            return ToProxyArguments(requestParam.Params, args);
        }

        private static Dictionary<string, object> ToProxyArguments(IDictionary<string, object> requestParams, Dictionary<string, object> args)
        {
            foreach (var key in args.Keys.ToList())
            {
                var value = args[key];
                var valueType = value.GetType();
                if (valueType.IsGenericType && valueType.GetGenericTypeDefinition() == typeof(List<>))
                {
                    var subRequestParams = requestParams.First(o => string.Equals(o.Key, key, StringComparison.OrdinalIgnoreCase)).Value.ToJsonString().ToObject<IList<Dictionary<string, object>>>();
                    foreach (var subRequestParam in subRequestParams)
                    {
                        var subObj = Activator.CreateInstance(valueType.GenericTypeArguments.First());
                        valueType.GetMethod("Add", BindingFlags.Instance | BindingFlags.Public)?.Invoke(value, new[] { ToProxyArgument(subObj, subRequestParam) });
                    }
                    args[key] = value;
                }
                else
                {
                    args[key] = ToProxyArgument(value, requestParams);
                }
            }
            return args;
        }

        private static object ToProxyArgument(object value, IDictionary<string, object> requestParams)
        {
            if (!(value is DoModelBase)) return value;
            value = typeof(ObModel).GetMethod("Of", BindingFlags.Static | BindingFlags.Public)
                ?.MakeGenericMethod(value.GetType()).Invoke(null, new [] { value });
            if (value == null) return null;
            foreach (var param in requestParams)
            {
                var property = value.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public)
                    .FirstOrDefault(o => string.Equals(o.Name, param.Key, StringComparison.OrdinalIgnoreCase));
                if (property == null) continue;
                if (property.PropertyType.IsSystem())
                {
                    property.SetValue(value, param.Value);
                }
                else
                {
                    var subRequestParams = param.Value is Newtonsoft.Json.Linq.JArray
                        ? new Dictionary<string, object> { { param.Key, param.Value } }
                        : param.Value.ToJsonString().ToObject<Dictionary<string, object>>();
                    var subObj = ToProxyArguments(subRequestParams, new Dictionary<string, object> { { property.Name, Activator.CreateInstance(property.PropertyType) } }).First();
                    property.SetValue(value, subObj.Value);
                }
            }
            return value;
        }

        private static bool TryKey(MethodBase method, out string key)
        {
            key = null;
            if (!Initialized || method.DeclaringType == null) return false;
            key = method.DeclaringType.FullName + "." + method.Name;
            return Config.ContainsKey(key);
        }

        public static IObQueryable<T> CreateQueryable<T>(this MethodBase currentMethodBase, IObQueryable<T> queryable,
            IDictionary<string, object> requestParams, IDictionary<string, object> requestGroupParams,
            IDictionary<string, string> requestSorts)
            where T : ObModelBase
        {
            queryable.ObParameter = CreateParameter<T>(currentMethodBase, queryable.ObParameter, requestParams);
            queryable.ObGroupParameter = CreateGroupParameter<T>(currentMethodBase, queryable.ObGroupParameter, requestGroupParams);
            queryable.ObSort = CreateSort<T>(currentMethodBase, queryable.ObSort, requestSorts);
            return queryable;
        }

        public static IObQueryable<TM, TT> CreateQueryable<TM, TT>(this MethodBase currentMethodBase, TT obTerm, IObQueryable<TM, TT> queryable,
            IDictionary<string, object> requestParams, IDictionary<string, object> requestGroupParams,
            IDictionary<string, string> requestSorts)
            where TM : ObModelBase
            where TT : ObTermBase
        {
            queryable.ObParameter = CreateParameter(currentMethodBase, obTerm, queryable.ObParameter, requestParams);
            queryable.ObGroupParameter = CreateGroupParameter(currentMethodBase, obTerm, queryable.ObGroupParameter, requestGroupParams);
            queryable.ObSort = CreateSort(currentMethodBase, obTerm, queryable.ObSort, requestSorts);
            return queryable;
        }

        public static ObParameterBase CreateParameter<T>(this MethodBase currentMethod, IDictionary<string, object> requestParams)
            where T : ObModelBase
        {
            return CreateParameter<T>(currentMethod, null, requestParams);
        }

        public static ObParameterBase CreateParameter<T>(this MethodBase currentMethod, IObParameter obParameter, IDictionary<string, object> requestParams)
            where T : ObModelBase
        {
            return CreateParameter<T>(currentMethod, (ObParameterBase)obParameter, requestParams);
        }

        public static ObParameterBase CreateParameter<T>(this MethodBase currentMethod, ObParameterBase obParameter, IDictionary<string, object> requestParams)
            where T : ObModelBase
        {
            return TryKey(currentMethod, out var key)
                ? CreateParameter<T>(null, obParameter, requestParams, Config[key].Params)
                : obParameter;
        }

        public static ObParameterBase CreateGroupParameter<T>(this MethodBase currentMethod, IDictionary<string, object> requestParams)
            where T : ObModelBase
        {
            return CreateGroupParameter<T>(currentMethod, null, requestParams);
        }

        public static ObParameterBase CreateGroupParameter<T>(this MethodBase currentMethod, IObParameter obParameter, IDictionary<string, object> requestParams)
            where T : ObModelBase
        {
            return CreateGroupParameter<T>(currentMethod, (ObParameterBase)obParameter, requestParams);
        }

        public static ObParameterBase CreateGroupParameter<T>(this MethodBase currentMethod, ObParameterBase obParameter, IDictionary<string, object> requestParams)
            where T : ObModelBase
        {
            return TryKey(currentMethod, out var key)
                ? CreateParameter<T>(null, obParameter, requestParams, Config[key].GroupParams)
                : obParameter;
        }

        public static IObSort CreateSort<T>(this MethodBase currentMethod, IDictionary<string, string> requestSorts)
            where T : ObModelBase
        {
            return CreateSort<T>(currentMethod, null, requestSorts);
        }

        public static IObSort CreateSort<T>(this MethodBase currentMethod, IObSort iObSort, IDictionary<string, string> requestSorts)
            where T : ObModelBase
        {
            return TryKey(currentMethod, out var key)
                ? CreateSort<T>(null, iObSort, requestSorts, Config[key].Sorts)
                : iObSort;
        }

        public static ObParameterBase CreateParameter<T>(this MethodBase currentMethod, T obTerm, IDictionary<string, object> requestParams)
            where T : ObTermBase
        {
            return CreateParameter(currentMethod, obTerm, null, requestParams);
        }

        public static ObParameterBase CreateParameter<T>(this MethodBase currentMethod, T obTerm, IObParameter obParameter, IDictionary<string, object> requestParams)
            where T : ObTermBase
        {
            return CreateParameter(currentMethod, obTerm, (ObParameterBase)obParameter, requestParams);
        }

        public static ObParameterBase CreateParameter<T>(this MethodBase currentMethod, T obTerm, ObParameterBase obParameter, IDictionary<string, object> requestParams)
            where T : ObTermBase
        {
            return TryKey(currentMethod, out var key) 
                ? CreateParameter(obTerm, obParameter, requestParams, Config[key].Params)
                : obParameter;
        }

        public static ObParameterBase CreateGroupParameter<T>(this MethodBase currentMethod, T obTerm, IDictionary<string, object> requestParams)
            where T : ObTermBase
        {
            return CreateGroupParameter(currentMethod, obTerm, null, requestParams);
        }

        public static ObParameterBase CreateGroupParameter<T>(this MethodBase currentMethod, T obTerm, IObParameter obParameter, IDictionary<string, object> requestParams)
            where T : ObTermBase
        {
            return CreateGroupParameter(currentMethod, obTerm, (ObParameterBase) obParameter, requestParams);
        }

        public static ObParameterBase CreateGroupParameter<T>(this MethodBase currentMethod, T obTerm, ObParameterBase obParameter, IDictionary<string, object> requestParams)
            where T : ObTermBase
        {
            return TryKey(currentMethod, out var key)
                ? CreateParameter(obTerm, obParameter, requestParams, Config[key].GroupParams)
                : obParameter;
        }

        private static ObParameterBase CreateParameter<T>(T obTerm, ObParameterBase obParameter, IDictionary<string, object> requestParams, IDictionary<string, ParamInfo> dictParams)
        {
            if (requestParams == null) return obParameter;
            foreach (var param in dictParams.Where(param => requestParams.ContainsKey(param.Key.Split(',')[0])))
            {
                ObParameterBase subParameter = null;
                foreach (var key in param.Key.Split(','))
                {
                    Type t;
                    var pSymbol = param.Value.Symbol;
                    object value;
                    switch (key.ToUpper())
                    {
                        case "NULL":
                            value = null;
                            pSymbol = "==";
                            t = typeof(object);
                            break;
                        case "NOT NULL":
                            value = null;
                            pSymbol = "!=";
                            t = typeof(object);
                            break;
                        default:
                            try
                            {
                                var requestJsonString = requestParams[key].ToJsonString();
                                if (param.Value.TypeString.Contains("[]") && !Regex.IsMatch(requestJsonString, @"^\[.+\]$"))
                                {
                                    requestJsonString = $"[{requestJsonString}]";
                                }
                                t = Type.GetType(param.Value.TypeString);
                                value = requestJsonString.ToObject(t);
                            }
                            catch (Exception /* er*/)
                            {
                                /*LogUtil.WriteLog("requestJsonString=" + requestJsonString +
                                                 ",paramKey=" + param.Key +
                                                 ",typeString=" + param.Value.TypeString, er);*/
                                continue;
                            }
                            break;
                    }
                    int sret;
                    if ((sret = SymbolTryParse(pSymbol,  value, out var symbol, out var dbvalue)) == 0) continue;
                    var names = param.Value.Name.Split(',');
                    foreach (var name in names)
                    {
                        var property = GetProperty(obTerm, name);
                        if (property == null) continue;

                        #region 如果in或not in时，当数组只有1个值时，转成=或<>

                        var vs = value as ICollection;
                        switch (symbol)
                        {
                            case DbSymbol.In:
                                if (vs != null && vs.Count == 0)
                                    continue;
                                if (vs != null && vs.Count == 1)
                                {
                                    symbol = DbSymbol.Equal;
                                    foreach (var v in vs)
                                    {
                                        value = v;
                                        break;
                                    }
                                }
                                break;
                            case DbSymbol.NotIn:
                                if (vs != null && vs.Count == 0)
                                    continue;
                                if (vs != null && vs.Count == 1)
                                {
                                    symbol = DbSymbol.NotEqual;
                                    foreach (var v in vs)
                                    {
                                        value = v;
                                        break;
                                    }
                                }
                                break;
                            case DbSymbol.Between:
                                if (vs != null && vs.Count != 2)
                                    continue;
                                /* if (vs != null && t == typeof(DateTime))
                                 {
                                     if (vs != null && t == typeof(DateTime))
                                     {
                                         var ds = vs.OfType<DateTime>().ToList();
                                         if (ds.Count > 1 &&
                                             ds[0].TimeOfDay.Ticks == 0 &&
                                             ds[1].TimeOfDay.Ticks == 0)
                                         {
                                             ds[1] = ds[1].Date.AddDays(1).AddSeconds(-1);
                                             value = ds;
                                         }
                                     }
                                 }*/
                                if (vs != null && t == typeof(DateTime[]))
                                {
                                    var ds = vs.OfType<DateTime>().ToList();
                                    if (ds.Count > 1 &&
                                        ds[0].TimeOfDay.Ticks == 0 &&
                                        ds[1].TimeOfDay.Ticks == 0)
                                    {
                                        ds[1] = ds[1].Date.AddDays(1).AddSeconds(-1);
                                        value = ds;
                                    }
                                }
                                break;
                        }

                        #endregion

                        if (subParameter == null)
                        {
                            subParameter = sret == 1 ? ObParameter.Create(property, symbol, value) : ObParameter.Create(property, dbvalue);
                        }
                        else
                        {
                            subParameter = subParameter || (sret == 1 ? ObParameter.Create(property, symbol, value) : ObParameter.Create(property, dbvalue));
                        }
                    }
                }
                if (subParameter == null) continue;
                if (obParameter == null)
                {
                    obParameter = ObParameter.Create(subParameter);
                }
                else
                {
                    obParameter = obParameter && ObParameter.Create(subParameter);
                }
            }
            return obParameter;
        }

        public static IObSort CreateSort<T>(this MethodBase currentMethod, T obTerm, IDictionary<string, string> requestSorts)
            where T : ObTermBase
        {
            return CreateSort(currentMethod, obTerm, null, requestSorts);
        }

        public static IObSort CreateSort<T>(this MethodBase currentMethod, T obTerm, IObSort iObSort, IDictionary<string, string> requestSorts)
            where T : ObTermBase
        {
            return TryKey(currentMethod, out var key)
                ? CreateSort(obTerm, iObSort, requestSorts, Config[key].Sorts)
                : iObSort;
        }

        private static IObSort CreateSort<T>(T obTerm, IObSort iObSort, IDictionary<string, string> requestSorts, IDictionary<string, ParamInfo> dictSorts)
        {
            if (dictSorts == null || dictSorts.Count == 0) return iObSort;
            IObSort obSort = null;
            //用户排序
            if (requestSorts != null)
            {
                foreach (var rs in requestSorts)
                {
                    if (!dictSorts.ContainsKey(rs.Key)) continue;
                    var sort = dictSorts[rs.Key];
                    var property = GetProperty(obTerm, sort.Name);
                    if (!SortTryParse(rs.Value, out var symbol)) //判断客户端提交的排序方式
                    {
                        if (!SortTryParse(sort.Symbol, out symbol)) //判断默认的排序方式
                        {
                            symbol = Sort.Ascending;
                        }
                    }
                    if (obSort == null)
                    {
                        obSort = ObSort.Create(property, symbol);
                    }
                    else
                    {
                        obSort.Add(ObSort.Create(property, symbol));
                    }
                }
            }
            //固定排序
            foreach (var sort in dictSorts.Where(sort => Regex.IsMatch(sort.Key, @"^(\{{0,1}([0-9a-fA-F]){8}-([0-9a-fA-F]){4}-([0-9a-fA-F]){4}-([0-9a-fA-F]){4}-([0-9a-fA-F]){12}\}{0,1})$")))
            {
                //key为GUID表示固定排序
                if (!SortTryParse(sort.Value.Symbol, out var symbol)) continue;
                var property = GetProperty(obTerm, sort.Value.Name);
                if (property == null) continue;
                if (obSort == null)
                {
                    obSort = ObSort.Create(property, symbol);
                }
                else
                {
                    obSort.Add(ObSort.Create(property, symbol));
                }
            }
            //传入排序
            if (obSort == null && iObSort != null)
            {
                obSort = iObSort;
            }
            else if (obSort != null && iObSort != null)
            {
                obSort.Add(iObSort);
            }
            //没有任排序
            if (obSort == null)
            {
                //取第一个配置排序
                foreach (var sort in dictSorts)
                {
                    var property = GetProperty(obTerm, sort.Value.Name);
                    if (property == null) continue;
                    if (!SortTryParse(sort.Value.Symbol, out var symbol))
                    {
                        symbol = Sort.Ascending;
                    }
                    obSort = ObSort.Create(property, symbol);
                    break;
                }
            }
            return obSort;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="symbolString"></param>
        /// <param name="value"></param>
        /// <param name="symbol"></param>
        /// <param name="dbValue"></param>
        /// <returns>0失败 1正常 2is null或is not null</returns>
        private static int SymbolTryParse(string symbolString, object value, out DbSymbol symbol, out DbValue dbValue)
        {
            var ret = 1;
            dbValue = DbValue.IsNull;
            switch (symbolString)
            {
                case "==":
                    symbol = DbSymbol.Equal;
                    if (value == null)
                    {
                        ret = 2;
                        dbValue = DbValue.IsNull;
                    }
                    break;
                case ">":
                    symbol = DbSymbol.Than;
                    break;
                case "<":
                    symbol = DbSymbol.Less;
                    break;
                case ">=":
                    symbol = DbSymbol.ThanEqual;
                    break;
                case "<=":
                    symbol = DbSymbol.LessEqual;
                    break;
                case "!=":
                    symbol = DbSymbol.NotEqual;
                    if (value == null)
                    {
                        ret = 2;
                        dbValue = DbValue.IsNotNull;
                    }
                    break;
                default:
                    if (Enum.TryParse(symbolString, true, out symbol))
                    {
                        switch (symbol)
                        {
                            case DbSymbol.Equal:
                                if (value == null)
                                {
                                    ret = 2;
                                    dbValue = DbValue.IsNull;
                                }
                                break;
                            case DbSymbol.NotEqual:
                                if (value == null)
                                {
                                    ret = 2;
                                    dbValue = DbValue.IsNotNull;
                                }
                                break;
                            case DbSymbol.Like:
                            case DbSymbol.LikeLeft:
                            case DbSymbol.LikeRight:
                                if (value == null || value.ToString() == "")
                                {
                                    ret = 0;
                                }
                                break;
                        }
                    }
                    else
                    {
                        ret = 0;
                    }
                    break;
            }
            return ret;
        }

        private static bool SortTryParse(string sortString, out Sort sort)
        {
            if (string.Equals(sortString, "asc", StringComparison.OrdinalIgnoreCase))
            {
                sort = Sort.Ascending;
            }
            else if (string.Equals(sortString, "desc", StringComparison.OrdinalIgnoreCase))
            {
                sort = Sort.Descending;
            }
            else if (!Enum.TryParse(sortString, true, out sort))
            {
                return false;
            }
            return true;
        }

        private static ObProperty GetProperty<T>(string propertyName)
        {
            if (string.IsNullOrEmpty(propertyName)) return null;
            var type = typeof(T);
            var tableName = type.ToTableName();
            var index = 0;
            var pns = propertyName.Split('.').ToList();
            while (index < pns.Count)
            {
                propertyName = pns[index];
                var property = type.GetProperties(BindingFlags.Instance | BindingFlags.Public).FirstOrDefault(obj => obj.Name == propertyName);
                if (property == null) return null;
                if (index + 1 < pns.Count)
                {
                    type = property.PropertyType;
                }
                index++;
            }
            pns.Insert(0, tableName);
            pns.RemoveAt(pns.Count - 1);
            tableName = string.Join("_", pns);
            var obRedefine = ObRedefine.Create(type, tableName);
            return ObProperty.Create(type, obRedefine, propertyName);
        }

        private static ObProperty GetProperty<T>(T m, string propertyName)
        {
            if (string.IsNullOrEmpty(propertyName)) return null;
            object model = m;
            var type = typeof(T);
            var tableName = type.ToTableName();
            var index = 0;
            var pns = propertyName.Split('.').ToList();
            while (index < pns.Count)
            {
                propertyName = pns[index];
                var property = type.GetProperties(BindingFlags.Instance | BindingFlags.Public).FirstOrDefault(obj => obj.Name == propertyName);
                if (property == null) return null;
                if (index + 1 < pns.Count)
                {
                    type = property.PropertyType;
                    if (typeof(ObTermBase).IsAssignableFrom(type))
                    {
                        model = property.GetValue(model);
                        if (m == null) return null;
                    }
                }
                index++;
            }
            if (model is ObTermBase obTerm)
            {
                return obTerm.GetProperty(propertyName);
            }
            pns.Insert(0, tableName);
            pns.RemoveAt(pns.Count - 1);
            tableName = string.Join("_", pns);
            var obRedefine = ObRedefine.Create(type, tableName);
            return ObProperty.Create(type, obRedefine, propertyName);
        }
    }
}
