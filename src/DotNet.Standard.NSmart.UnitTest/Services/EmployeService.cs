using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using DotNet.Standard.NParsing.Factory;
using DotNet.Standard.NParsing.Interface;
using DotNet.Standard.NSmart.UnitTest.IServices;
using DotNet.Standard.NSmart.UnitTest.Models;
using DotNet.Standard.NSmart.Utilities;

namespace DotNet.Standard.NSmart.UnitTest.Services
{
    public class EmployeService : BaseService<EmployeInfo, Employe>, IEmployeService
    {
        protected override void GetList(ref IObQueryable<EmployeInfo, Employe> queryable, IDictionary<string, object> requestParams, IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts)
        {
            base.GetList(ref queryable, requestParams, requestGroupParams, requestSorts);
            queryable.ObParameter = MethodBase.GetCurrentMethod().CreateParameter(Term, queryable.ObParameter, requestParams);
            queryable.ObGroupParameter = MethodBase.GetCurrentMethod().CreateGroupParameter(Term, queryable.ObGroupParameter, requestParams);
            queryable.ObSort = MethodBase.GetCurrentMethod().CreateSort(Term, queryable.ObSort, requestSorts);
        }

        protected override void OnAdding(EmployeInfo model, ref IObQueryable<EmployeInfo, Employe> queryable)
        {
            base.OnAdding(model, ref queryable);
            model.CreateTime = DateTime.Now;
        }

        protected override void OnUpdating(EmployeInfo model, ref IObQueryable<EmployeInfo, Employe> queryable)
        {
            base.OnUpdating(model, ref queryable);
            queryable = queryable.Join(o => o);
        }

        public async Task<ResultInfo<IList<EmployeInfo>>> GetTotals()
        {
            var list = await GetListAsync(o =>
                o.Join(j => j.Department)
                    .GroupBy(g => new
                    {
                        g.DepartmentId,
                        g.Department.Id,
                        g.Department.Name
                    }).Select(s => new
                    {
                        s.DepartmentId,
                        Department = new
                        {
                            s.Department.Id,
                            s.Department.Name
                        },
                        Age = s.Avg(a => a.Age)
                    }));
            return new ResultInfo<IList<EmployeInfo>>
            {
                Data = list
            };
        }
    }
}
