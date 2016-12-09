using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_DanhGiaNangSuat
{
    public class Plugin_DanhGiaNangSuat : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];

            Entity DGNS = service.Retrieve("new_danhgianangsuat", target.Id, new ColumnSet(new string[] {"new_nangsuatdukien", "new_thoigianthuhoachdukien", "new_sanluongdukien"}));

            if (target.Contains("new_trangthai")){
                if (((OptionSetValue)target["new_trangthai"]).Value == 100000002)
                {
                    Entity chitietHD = new Entity("new_thuadatcanhtac");
                    chitietHD.Id = ((EntityReference)target["new_thuadatcanhtac"]).Id;
                    chitietHD["new_nangsuat"] = DGNS.Contains("new_nangsuatdukien") ? DGNS["new_nangsuatdukien"] : null;
                    chitietHD["new_ngaythuhoachdukien"] = DGNS.Contains("new_thoigianthuhoachdukien") ? DGNS["new_thoigianthuhoachdukien"] : null;
                    chitietHD["new_sanluonguoc"] = DGNS.Contains("new_sanluongdukien") ? DGNS["new_sanluongdukien"] : null;
                    service.Update(chitietHD);
                }
            }
        }
    }
}
