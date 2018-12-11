using System;
using System.Collections.Generic;
using System.Web;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;

namespace DotNet.Standard.NSmart.Utilities
{
    public static class ConvertUtil
    {
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
            //return value == null ? "" : new JavaScriptSerializer().Serialize(value);
            return JsonConvert.SerializeObject(value,
                format,
                new JsonSerializerSettings
                {
                    ContractResolver = new CamelCasePropertyNamesContractResolver(), //首字母小写
                    NullValueHandling = NullValueHandling.Ignore, //不显示值为null的属性
                    //DateFormatHandling = DateFormatHandling.MicrosoftDateFormat, //时间格式
                    Converters = new JsonConverter[]
                    {
                        new IsoDateTimeConverter {DateTimeFormat = "yyyy-MM-dd HH:mm:ss.fff"} //日期格式化
                    }
                }
            );
        }

        public static T ToObject<T>(this string strJson)
        {
            return JsonConvert.DeserializeObject<T>(strJson);
        }

        public static object ToObject(this string strJson, Type t)
        {
            return JsonConvert.DeserializeObject(strJson, t);
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
    }
}
