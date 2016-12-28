using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Action_Taisanthehchap
{
    public class Action_Taisanthehchap : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            EntityReference target = (EntityReference)context.InputParameters["Target"];

            if (target.LogicalName == "new_bangketienkhuyenkhich")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity hopdongthechap = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                if (hopdongthechap == null)
                {
                    throw new Exception("Hợp đồng thế chấp không tồn tại !!");
                }
            }
        }
    }
}
