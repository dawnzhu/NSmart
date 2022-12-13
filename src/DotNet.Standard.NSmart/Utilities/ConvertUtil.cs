using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Web;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;

namespace DotNet.Standard.NSmart.Utilities
{
    public static class ConvertUtil
    {
        private static JsonSerializerSettings _settings;

        public static string ToJsonString(this object value)
        {
            return ToJsonString(value, Formatting.None);
        }

        /// <summary>
        /// 返回JSON字符串
        /// </summary>
        /// <param name="value"></param>
        /// <param name="format"></param>
        /// <returns></returns>
        public static string ToJsonString(this object value, Formatting format)
        {
            return JsonConvert.SerializeObject(value, format, CreateJsonSerializerSettings());
        }

        public static T ToObject<T>(this string strJson)
        {
            return JsonConvert.DeserializeObject<T>(strJson, CreateJsonSerializerSettings());
        }

        public static object ToObject(this string strJson, Type t)
        {
            return JsonConvert.DeserializeObject(strJson, t, CreateJsonSerializerSettings());
        }

        public static IDictionary<string, object> ToDictionary(this string queryString)
        {
            var dic = new Dictionary<string, object>();
            var queryStrings = queryString.TrimStart('?').Split('&');
            foreach (var qs in queryStrings)
            {
                var nv = qs.Split('=');
                if (nv.Length != 2) continue;
                nv[1] = HttpUtility.UrlDecode(nv[1]);
                if (dic.ContainsKey(nv[0]))
                {
                    if (dic[nv[0]] is IList<string>)
                    {
                        ((IList<string>)dic[nv[0]]).Add(nv[1]);
                    }
                    else if (dic[nv[0]] is string)
                    {
                        dic[nv[0]] = new List<string>
                        {
                            dic[nv[0]].ToString(),
                            nv[1]
                        };
                    }
                }
                else
                {
                    dic.Add(nv[0], nv[1]);
                }
            }
            return dic;
        }

        /// <summary>
        /// 转蛇形命名
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public static string ToSnakeCaseNaming(this string name)
        {
            var builder = new StringBuilder();
            var previousUpper = false;
            for (var i = 0; i < name.Length; i++)
            {
                var c = name[i];
                if (char.IsUpper(c))
                {
                    if (i > 0 && !previousUpper)
                    {
                        builder.Append("_");
                    }
                    builder.Append(char.ToLowerInvariant(c));
                    previousUpper = true;
                }
                else
                {
                    builder.Append(c);
                    previousUpper = false;
                }
            }
            return builder.ToString();
        }

        /// <summary>
        /// 转小驼峰命名
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public static string ToCamelCaseNaming(this string name)
        {
            return ToCamelCaseNaming(name, true);
        }

        /// <summary>
        /// 转驼峰命名
        /// </summary>
        /// <param name="name"></param>
        /// <param name="isLower"></param>
        /// <returns></returns>
        public static string ToCamelCaseNaming(this string name, bool isLower)
        {
            var builder = new StringBuilder();
            var previousSplit = false;
            for (var i = 0; i < name.Length; i++)
            {
                var c = name[i];
                if (i == 0)
                {
                    c = isLower ?  char.ToLowerInvariant(c) : char.ToUpperInvariant(c);
                }
                if (c == '_')
                {
                    previousSplit = true;
                }
                else
                {
                    if (previousSplit)
                    {
                        c = char.ToUpperInvariant(c);
                    }
                    builder.Append(c);
                    previousSplit = false;
                }
            }
            return builder.ToString();
        }

        private static JsonSerializerSettings CreateJsonSerializerSettings()
        {
            return _settings ?? new JsonSerializerSettings
            {
                Converters = new List<JsonConverter>()
            }.UseNSmart();
        }

        /// <summary>
        /// 使用外部设置JSON格式
        /// </summary>
        /// <param name="settings"></param>
        /// <returns></returns>
        public static JsonSerializerSettings MapNSmart(this JsonSerializerSettings settings)
        {
            _settings = settings;
            return settings;
        }

        /// <summary>
        /// 使用默认设置JSON格式
        /// </summary>
        /// <param name="settings"></param>
        /// <returns></returns>
        public static JsonSerializerSettings UseNSmart(this JsonSerializerSettings settings)
        {
            //忽略null属性
            settings.NullValueHandling = NullValueHandling.Ignore;
            //日期格式
            settings.DateFormatString = "yyyy-MM-dd HH:mm:ss.fff";
            //小驼峰命名
            //settings.ContractResolver = new CamelCasePropertyNamesContractResolver();
            //属性蛇形命名
            settings.ContractResolver = new DefaultContractResolver { NamingStrategy = new SnakeCaseNamingStrategy() };
            //DoModel对象转换
            settings.Converters.Add(new DoModelConverter());
            //枚举转字符串
            settings.Converters.Add(new StringEnumConverter());
            _settings = settings;
            return settings;
        }
    }
}
