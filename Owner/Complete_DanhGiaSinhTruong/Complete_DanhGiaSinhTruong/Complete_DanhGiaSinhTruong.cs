using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Complete_DanhGiaSinhTruong
{
    public class Complete_DanhGiaSinhTruong : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            traceService.Trace("1");
            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statecode") && ((OptionSetValue)target["statecode"]).Value == 1)
            {
                traceService.Trace("2");
                Entity danhgiasinhtruong = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_nangsuatdukien", "new_sanluongdukien", "new_chitiethddtmia" }));
                traceService.Trace("3");

                Entity thuadatcanhtac = service.Retrieve("new_thuadatcanhtac",
                    ((EntityReference)danhgiasinhtruong["new_chitiethddtmia"]).Id, new ColumnSet(new string[] { "new_sanluonguoc", "new_nangsuatuoc" }));

                traceService.Trace("4");
                decimal sanluonguoc = 0;
                decimal nangsuatuoc = 0;

                sanluonguoc = (decimal)danhgiasinhtruong["new_sanluongdukien"];
                nangsuatuoc = (decimal)danhgiasinhtruong["new_nangsuatdukien"];

                thuadatcanhtac["new_sanluonguoc"] = sanluonguoc;
                traceService.Trace("5");

                thuadatcanhtac["new_nangsuatuoc"] = nangsuatuoc;
                traceService.Trace("6");

                service.Update(thuadatcanhtac);
            }
        }
    }
}
