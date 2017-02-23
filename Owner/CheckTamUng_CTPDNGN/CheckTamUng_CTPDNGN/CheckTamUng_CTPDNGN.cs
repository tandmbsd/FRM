using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;

namespace CheckTamUng_CTPDNGN
{
    public class CheckTamUng_CTPDNGN : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("new_phantramtamung"))
            {
                Entity CTPDNGN = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet("new_chinhsachdautu", "new_phantramtamung", "new_tamungcs"));

                if (CTPDNGN.Contains("new_chinhsachdautu"))
                {
                    Entity chinhsachdautu = service.Retrieve("new_chinhsachdautu",
                        ((EntityReference)CTPDNGN["new_chinhsachdautu"]).Id, new ColumnSet(new string[] { "new_dinhmuctamung" }));

                    decimal tamung = (decimal)CTPDNGN["new_phantramtamung"];
                    decimal tamungchinhsach = chinhsachdautu.Contains("new_dinhmuctamung") ? (decimal)chinhsachdautu["new_dinhmuctamung"] : 0;

                    if (tamung > tamungchinhsach)
                    {
                        throw new Exception("Tạm ứng nhập vào không được lớn hơn tạm ứng chính sách");
                    }
                }
            }
        }
    }
}
