using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_BienBanMiaChay
{
    public class Plugin_BienBanMiaChay : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];

            Entity BBMiachay = service.Retrieve("new_bienbanmiachay", target.Id, new ColumnSet(true));


            if (target.Contains("new_trangthai")){
                if (((OptionSetValue)target["new_trangthai"]).Value == 100000002)
                {
                    Entity chitietHD = service.Retrieve("new_thuadatcanhtac", ((EntityReference)target["new_thuadatcanhtac"]).Id, new ColumnSet(new string[] { "new_thuadatcanhtacid", "new_tongdientichmiachay" }));

                    chitietHD["new_miachay"] = true;
                    chitietHD["new_tongdientichmiachay"] = (chitietHD.Contains("new_tongdientichmiachay") ? (decimal)chitietHD["new_tongdientichmiachay"] : 0 )+ (decimal)BBMiachay["new_dientichchay"];
                    service.Update(chitietHD);
                }
            }
        }
    }
}
