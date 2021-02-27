using System;
using System.Collections.Generic;
using System.Reflection;
using DotNet.Standard.NParsing.Factory;
using DotNet.Standard.NParsing.Interface;
using DotNet.Standard.NSmart.UnitTest.Models;
using DotNet.Standard.NSmart.Utilities;

namespace DotNet.Standard.NSmart.UnitTest
{
    public class TestService : DoServiceBase<EmployeInfo>
    {
        public TestService() //: base(new Employe().Of())
        {
            if (!DoParam.Initialized)
            {
                DoParam.Initialize();
            }
        }

        protected override void OnAdding(EmployeInfo model, ref IObQueryable<EmployeInfo> queryable)
        {
            base.OnAdding(model, ref queryable);
            model.CreateTime = DateTime.Now;
        }

        public void Add()
        {
            Add(new EmployeInfo());
        }

        protected override void OnUpdating(EmployeInfo model, ref IObQueryable<EmployeInfo> queryable)
        {
            base.OnUpdating(model, ref queryable);
            model.CreateTime = DateTime.Now;
        }

        public void Update()
        {
            //BaseDals.First().Join().Update(new EmployeInfo(), o => o.Age == 21);
            var model = new EmployeInfo().Of();
            model.Age = 33;
            Update(model, o => o.Where(k => k.Id == 1));
        }

        protected override void GetList(ref IObQueryable<EmployeInfo> queryable, IDictionary<string, object> requestParams, IDictionary<string, object> requestGroupParams, IDictionary<string, string> requestSorts)
        {
            base.GetList(ref queryable, requestParams, requestGroupParams, requestSorts);
            queryable = MethodBase.GetCurrentMethod().CreateQueryable(queryable, requestParams, requestGroupParams, requestSorts);
            /*queryable.ObParameter = MethodBase.GetCurrentMethod().CreateParameter<EmployeInfo>(queryable.ObParameter, requestParams);
            queryable.ObGroupParameter = MethodBase.GetCurrentMethod().CreateGroupParameter<EmployeInfo>(queryable.ObGroupParameter, requestGroupParams);
            queryable.ObSort = MethodBase.GetCurrentMethod().CreateSort<EmployeInfo>(queryable.ObSort, requestSorts);*/
        }

        public async void QueryList()
        {
            var dal = ObHelper.Create<EmployeInfo>("database=NSmart.Demo01;server=.;uid=sa;pwd=1;Pooling=true;Connection Timeout=300;", "DotNet.Standard.NParsing.SQLServer");
            //var ba = dal.Where(o => o.Id == 1 && o.Age == 3);
            //var ba2 = dal.Where(o => o.Id == 1);
            //ba.And(o => o.Id != 0);
            //ba.OrderBy(o => o.Id);
            var e = new EmployeInfo
            {
                
            };
            var a = GetModel(q => q.Where(o => o.Id == e.DepartmentId));
            var b = a;
        }
    }
}
