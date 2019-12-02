using System;
using DotNet.Standard.NParsing.Factory;
using Newtonsoft.Json.Converters;

namespace DotNet.Standard.NSmart
{
    public class DoModelConverter : CustomCreationConverter<DoModelBase>
    {
        public override DoModelBase Create(Type objectType)
        {
            var obj = ObModel.Create(objectType);
            /*var obj = Activator.CreateInstance(objectType);
            obj = typeof(ObModel).GetMethod("Of", BindingFlags.Static | BindingFlags.Public)
                ?.MakeGenericMethod(obj.GetType()).Invoke(null, new[] { obj });*/
            return (DoModelBase)obj;
        }
    }
}
