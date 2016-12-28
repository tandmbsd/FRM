using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_PDNThuNo
{
    public class Plugin_PDNThuNo : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000)
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity pdnthuno = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_loaithuno", "new_tongtienthu", "new_khachhang", "new_khachhangdoanhnghiep" }));
                decimal loaithuno = pdnthuno.Contains("new_loaithuno") ? ((OptionSetValue)pdnthuno["new_loaithuno"]).Value : 0;

                if ( loaithuno == 100000001)
                {                   
                    if (pdnthuno.Contains("new_khachhang"))
                    {                        
                        Entity contact = service.Retrieve("contact", ((EntityReference)pdnthuno["new_khachhang"]).Id, new ColumnSet(new string[] { "new_co", "new_no" }));
                        if (contact.Contains("new_no"))
                        {                            
                            decimal k = ((Money)contact["new_no"]).Value - (pdnthuno.Contains("new_tongtienthu") ? ((Money)pdnthuno["new_tongtienthu"]).Value : 0);
                            contact["new_no"] = new Money(k);
                            service.Update(contact);
                        }
                    }
                }
            }
        }
    }
}
