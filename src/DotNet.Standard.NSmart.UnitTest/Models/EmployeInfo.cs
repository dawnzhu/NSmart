using System;
using System.Collections;
using System.Collections.Generic;
using DotNet.Standard.NParsing.ComponentModel;
using DotNet.Standard.NParsing.Factory;
using DotNet.Standard.NParsing.Interface;
using DotNet.Standard.NSmart.ComponentModel;

namespace DotNet.Standard.NSmart.UnitTest.Models
{
    [ObModel(Name = "Employes")]
    public class EmployeBaseInfo : BaseInfo
    {
        /// <summary>
        /// 员工编号
        /// </summary>
        [ObConstraint(ObConstraint.PrimaryKey)]
        [ObProperty(Name = "ID", Length = 4, Nullable = false)]
        public override int Id
        {
            get => base.Id;
            set => base.Id = value;
        }

        /// <summary>
        /// 员工名称
        /// </summary>	
        [ObProperty(Name = "Name", Length = 50, Nullable = false)]
        public virtual string Name { get; set; }
    }

    public class EmployeBase : BaseTerm
    {
        public EmployeBase() : base(typeof(EmployeBaseInfo))
        { }

        public EmployeBase(ObTermBase parent, string rename) : base(typeof(EmployeBaseInfo), parent, rename)
        { }

        protected EmployeBase(Type modelType) : base(modelType)
        {
        }

        protected EmployeBase(Type modelType, string rename) : base(modelType, rename)
        {
        }

        protected EmployeBase(Type modelType, ObTermBase parent, string rename) : base(modelType, parent, rename)
        {
        }

        /// <summary>
        /// 员工名称
        /// </summary>		
        public virtual ObProperty Name { get; }
    }

    /// <summary>
    /// Employes实体类
    /// </summary>
    [DoModel(Name = "Employes", DoType = DoType.Id)]
    public class EmployeInfo : EmployeBaseInfo
    {
        /// <summary>
        /// 性别
        /// </summary>	
        [ObProperty(Name = "Gender", Length = 4, Nullable = false)]
        public virtual int Gender { get; set; }

        /// <summary>
        /// 创建时间
        /// </summary>	
        [ObProperty(Name = "CreateTime", Length = 8, Precision = 3, Nullable = false)]
        public virtual DateTime CreateTime { get; set; }

        /// <summary>
        /// Dimission
        /// </summary>	
        [ObProperty(Name = "Dimission", Length = 1, Nullable = false)]
        public virtual bool Dimission { get; set; }

        /// <summary>
        /// 部门编号
        /// </summary>
        [ObConstraint(ObConstraint.ForeignKey, Refclass = typeof(DepartmentInfo), Refproperty = "Id")]
        [ObProperty(Name = "DepartmentID", Length = 4, Nullable = true)]
        public virtual int DepartmentId { get; set; }

        /// <summary>
        /// 年龄
        /// </summary>	
        [ObProperty(Name = "Age", Length = 4, Nullable = false)]
        public virtual int Age { get; set; }

        public DepartmentInfo Department { get; set; }

        //public List<DepartmentInfo> Departments { get; set; }

    }

    /// <summary>
    /// 员工条件类
    /// </summary>	
    public class Employe : EmployeBase
    {
        public Employe() : base(typeof(EmployeInfo))
        { }

        public Employe(ObTermBase parent, string rename) : base(typeof(EmployeInfo), parent, rename)
        { }

        /// <summary>
        /// 性别
        /// </summary>		
        public virtual ObProperty Gender { get; }

        /// <summary>
        /// 创建时间
        /// </summary>		
        public virtual ObProperty CreateTime { get; }

        /// <summary>
        /// Dimission
        /// </summary>		
        public virtual ObProperty Dimission { get; }

        /// <summary>
        /// 部门编号
        /// </summary>		
        public virtual ObProperty DepartmentId { get; }

        /// <summary>
        /// 年龄
        /// </summary>		
        public virtual ObProperty Age { get; }

        public virtual Department Department { get; }

    }
}
