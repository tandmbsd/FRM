using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_CapnhapDinhMucChiTietHDDTM
{
    public class Plugin_CapnhapDinhMucChiTietHDDTM : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];
            Entity bienbangiamhuy = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
            string tinhtrang = ((OptionSetValue)bienbangiamhuy["new_trangthai"]).Value.ToString();
            string loaibienban = ((OptionSetValue)bienbangiamhuy["new_loai"]).Value.ToString();            

            if (tinhtrang == "100000002" && loaibienban == "100000000")
            {
                decimal dientichgiamhuy = bienbangiamhuy.GetAttributeValue<decimal>("new_dientichgiamhuy");
                Entity chinhsachdautu = service.Retrieve("new_chinhsachdautu",(Entity))
            }
        }
    }
}
