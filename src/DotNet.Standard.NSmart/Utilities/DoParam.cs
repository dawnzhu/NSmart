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

        public static void Initialize(Dictionary<string, ParamConfig> config)
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

        private static bool TryKey(this MethodBase method, out string key)
        {
            key = null;
            if (!Initialized || method.DeclaringType == null) return false;
            key = method.DeclaringType.FullName + "." + method.Name;
            return Config.ContainsKey(key);
        }

        public static ObParameterBase CreateParameter(this ObTermBase obTerm, MethodBase currentMethod,
            IDictionary<string, object> requestParams)
        {
            return CreateParameter(obTerm, currentMethod, null, requestParams);
        }

        public static ObParameterBase CreateParameter(this ObTermBase obTerm, MethodBase currentMethod, ObParameterBase obParameter,
            IDictionary<string, object> requestParams)
        {
            return currentMethod.TryKey(out var key) 
                ? CreateParameter(obTerm, obParameter, requestParams, Config[key].Params)
                : obParameter;
        }

        public static ObParameterBase CreateGroupParameter(this ObTermBase obTerm, MethodBase currentMethod,
            IDictionary<string, object> requestParams)
        {
            return CreateGroupParameter(obTerm, currentMethod, null, requestParams);
        }
         
        public static ObParameterBase CreateGroupParameter(this ObTermBase obTerm, MethodBase currentMethod, ObParameterBase obParameter,
            IDictionary<string, object> requestParams)
        {
            return currentMethod.TryKey(out var key)
                ? CreateParameter(obTerm, obParameter, requestParams, Config[key].GroupParams)
                : obParameter;
        }

        private static ObParameterBase CreateParameter(ObTermBase obTerm, ObParameterBase obParameter, IDictionary<string, object> requestParams, IDictionary<string, ParamInfo> dictParams)
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

        public static IObSort CreateSort(this ObTermBase obTerm, MethodBase currentMethod,
            IDictionary<string, string> requestSorts)
        {
            return CreateSort(obTerm, currentMethod, null, requestSorts);
        }

        public static IObSort CreateSort(this ObTermBase obTerm, MethodBase currentMethod, IObSort iObSort,
            IDictionary<string, string> requestSorts)
        {
            if (!currentMethod.TryKey(out var key)) return iObSort;
            var dictSorts = Config[key].Sorts;
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
        /// <param name="dbvalue"></param>
        /// <returns>0失败 1正常 2is null或is not null</returns>
        private static int SymbolTryParse(string symbolString, object value, out DbSymbol symbol, out DbValue dbvalue)
        {
            var ret = 1;
            dbvalue = DbValue.IsNull;
            switch (symbolString)
            {
                case "==":
                    symbol = DbSymbol.Equal;
                    if (value == null)
                    {
                        ret = 2;
                        dbvalue = DbValue.IsNull;
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
                        dbvalue = DbValue.IsNotNull;
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
                                    dbvalue = DbValue.IsNull;
                                }
                                break;
                            case DbSymbol.NotEqual:
                                if (value == null)
                                {
                                    ret = 2;
                                    dbvalue = DbValue.IsNotNull;
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

        private static ObProperty GetProperty(ObTermBase obTerm, string propertyName)
        {
            while (true)
            {
                if (string.IsNullOrEmpty(propertyName)) return null;
                var pns = propertyName.Split('.').ToList();
                if (pns.Count == 1) return obTerm.GetProperty(propertyName);
                var pn = pns[0];
                var propertyInfo = obTerm.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public).FirstOrDefault(obj => obj.Name == pn);
                if (propertyInfo == null) return null;
                var subObTrem = (ObTermBase) propertyInfo.GetValue(obTerm);
                obTerm = subObTrem;
                pns.RemoveAt(0);
                propertyName = string.Join(".", pns);
            }
        }
    }
}
