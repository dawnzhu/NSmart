using System.Collections.Generic;
using System.Reflection;
using DotNet.Standard.NParsing.Interface;
using DotNet.Standard.NSmart.UnitTest.IServices;
using DotNet.Standard.NSmart.UnitTest.Models;
using DotNet.Standard.NSmart.Utilities;

namespace DotNet.Standard.NSmart.UnitTest.Services
{
    public class DepartmentService : BaseService<DepartmentInfo>, IDepartmentService
    {
        protected override void GetList(ref IObQueryable<DepartmentInfo> queryable, IDictionary<string, object> requestParams, IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts)
        {
            base.GetList(ref queryable, requestParams, requestGroupParams, requestSorts);
            queryable = MethodBase.GetCurrentMethod().CreateQueryable(queryable, requestParams, requestGroupParams, requestSorts);
            /*queryable.ObParameter = MethodBase.GetCurrentMethod().CreateParameter(Term, queryable.ObParameter, requestParams);
            queryable.ObGroupParameter = MethodBase.GetCurrentMethod().CreateGroupParameter(Term, queryable.ObGroupParameter, requestParams);
            queryable.ObSort = MethodBase.GetCurrentMethod().CreateSort(Term, queryable.ObSort, requestSorts);*/
        }
    }
}
