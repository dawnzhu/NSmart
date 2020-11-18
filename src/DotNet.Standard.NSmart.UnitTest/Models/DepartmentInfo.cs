using DotNet.Standard.NParsing.ComponentModel;
using DotNet.Standard.NParsing.Factory;
using DotNet.Standard.NParsing.Interface;

namespace DotNet.Standard.NSmart.UnitTest.Models
{
    /// <summary>
    /// 部门实体类
    /// </summary>
    [ObModel(Name = "Departments")]
    public class DepartmentInfo : BaseInfo
    {
        /// <summary>
        /// 部门编号
        /// </summary>	
        [ObConstraint(ObConstraint.PrimaryKey)]
        [ObProperty(Name = "ID", Length = 4, Nullable = false)]
        public override int Id
        {
            get => base.Id;
            set => base.Id = value;
        }

        /// <summary>
        /// 门部名称
        /// </summary>	
        [ObProperty(Name = "Name", Length = 50, Nullable = false)]
        public virtual string Name { get; set; }

        /// <summary>
        /// 主管编号
        /// </summary>
        [ObConstraint(ObConstraint.ForeignKey, Refclass = typeof(EmployeBaseInfo), Refproperty = "Id")]
        [ObProperty(Name = "DirectorID", Length = 4, Nullable = true)]
        public virtual int DirectorId { get; set; }

        public EmployeBaseInfo Director { get; set; }
    }

    /// <summary>
    /// 部门条件类
    /// </summary>	
    public class Department : BaseTerm
    {
        public Department() : base(typeof(DepartmentInfo))
        { }

        public Department(ObTermBase parent, string rename) : base(typeof(DepartmentInfo), parent, rename)
        { }

        /// <summary>
        /// 部门名称
        /// </summary>		
        public virtual ObProperty Name { get; }

        /// <summary>
        /// 主管编号
        /// </summary>		
        public virtual ObProperty DirectorId { get; }

        public virtual EmployeBase Director { get; }
    }
}
