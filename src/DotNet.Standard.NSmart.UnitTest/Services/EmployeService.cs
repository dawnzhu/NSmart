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
    public class EmployeService : BaseService<EmployeInfo>, IEmployeService
    {
        protected override void GetList(ref IObQueryable<EmployeInfo> queryable, IDictionary<string, object> requestParams, IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts)
        {
            base.GetList(ref queryable, requestParams, requestGroupParams, requestSorts);
            queryable = MethodBase.GetCurrentMethod().CreateQueryable(queryable, requestParams, requestGroupParams, requestSorts);
            queryable.Join(o => new { o.Department, o.Department.Director });
        }

        protected override void OnAdding(EmployeInfo model, ref IObQueryable<EmployeInfo> queryable)
        {
            base.OnAdding(model, ref queryable);
            model.CreateTime = DateTime.Now;
        }

        protected override void OnUpdating(EmployeInfo model, ref IObQueryable<EmployeInfo> queryable)
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
                        s.FirstOrDefault().DepartmentId,
                        Department = new
                        {
                            s.FirstOrDefault().Department.Id,
                            s.FirstOrDefault().Department.Name
                        },
                        Age = s.Average(a => a.Age),
                        Dimission = s.Custom("dbo.Abc", a => new object[]{a.Max(d => d.Dimission), 1})
                    }));
            return new ResultInfo<IList<EmployeInfo>>
            {
                Data = list
            };
        }
    }
}
