using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Update_CTHDDTM_TinhTrangNT
{
    public class Update_CTHDDTM_TinhTrangNT : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("new_trangthainghiemthu"))
            {
                int trangthainghiemthu = ((OptionSetValue)target["new_trangthainghiemthu"]).Value;
                Entity newCTHD = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "statuscode" }));

                if (trangthainghiemthu == 100000000) // nhap
                {
                    newCTHD["statuscode"] = new OptionSetValue(1);
                }
                else if (trangthainghiemthu == 100000001) // Ký
                {
                    newCTHD["statuscode"] = new OptionSetValue(100000000);
                }
                else if (trangthainghiemthu == 100000002) // nt lan 1 
                {
                    newCTHD["statuscode"] = new OptionSetValue(100000001);
                }
                else if (trangthainghiemthu == 100000005) // nt lan 4 
                {
                    newCTHD["statuscode"] = new OptionSetValue(100000002);
                }
                service.Update(newCTHD);
            }
        }
    }
}
