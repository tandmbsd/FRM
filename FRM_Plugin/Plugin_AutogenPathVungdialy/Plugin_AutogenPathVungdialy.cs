using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_AutogenPathVungdialy
{
    public class Plugin_AutogenPathVungdialy : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            Entity target = (Entity)context.InputParameters["Target"];
            if (context.Depth < 2)
            {
                if (target.LogicalName.Trim().ToLower() == "new_vungdialy_hanhchinh")
                {
                    Entity fullTarget = service.Retrieve("new_vungdialy_hanhchinh", target.Id, new ColumnSet(true));
                    string path = (fullTarget.Contains("new_quocgia") ? ((EntityReference)fullTarget["new_quocgia"]).Name + "\\" : "") +
                                    (fullTarget.Contains("new_tinh") ? ((EntityReference)fullTarget["new_tinh"]).Name + "\\" : "") +
                                    (fullTarget.Contains("new_quanhuyen") ? ((EntityReference)fullTarget["new_quanhuyen"]).Name + "\\" : "") +
                                    (fullTarget.Contains("new_xaphuong") ? ((EntityReference)fullTarget["new_xaphuong"]).Name + "\\" : "") +
                                    (fullTarget.Contains("new_apthon") ? ((EntityReference)fullTarget["new_apthon"]).Name + "\\" : "");
                    Entity update = new Entity("new_vungdialy_hanhchinh");
                    update.Id = target.Id;
                    update["new_path"] = path;
                    service.Update(update);
                }
                else if (target.LogicalName.Trim().ToLower() == "new_diachi")
                {
                    Entity fullTarget = service.Retrieve("new_diachi", target.Id, new ColumnSet(true));
                    string path = (fullTarget.Contains("new_quocgia") ? ((EntityReference)fullTarget["new_quocgia"]).Name + "\\" : "") +
                                    (fullTarget.Contains("new_tinhthanh") ? ((EntityReference)fullTarget["new_tinhthanh"]).Name + "\\" : "") +
                                    (fullTarget.Contains("new_quanhuyen") ? ((EntityReference)fullTarget["new_quanhuyen"]).Name + "\\" : "") +
                                    (fullTarget.Contains("new_phuongxa") ? ((EntityReference)fullTarget["new_phuongxa"]).Name + "\\" : "") +
                                    (fullTarget.Contains("new_apthon") ? ((EntityReference)fullTarget["new_apthon"]).Name + "\\" : "");
                    Entity update = new Entity("new_diachi");
                    update.Id = target.Id;
                    update["new_path"] = path;
                    service.Update(update);
                }
            }  
        }
    }
}
