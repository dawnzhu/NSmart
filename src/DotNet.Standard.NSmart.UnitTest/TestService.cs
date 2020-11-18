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

        public void QueryList()
        {

        }
    }
}
