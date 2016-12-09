using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace MappingLeadtoContact
{
    public class MappingLeadtoContact : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            Entity target = (Entity)context.InputParameters["Target"];
            if (target.LogicalName == "contact")
                if (target.Attributes.Contains("originatingleadid"))
                {
                    factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                    service = factory.CreateOrganizationService(context.UserId);

                    Entity lead = service.Retrieve("lead", ((EntityReference)target["originatingleadid"]).Id , new ColumnSet("leadid"));

                    QueryExpression q = new QueryExpression("new_lichsucaytrong");
                    q.ColumnSet = new ColumnSet(true);
                    q.Criteria = new FilterExpression();
                    q.Criteria.AddCondition(new ConditionExpression("new_khachhangdubi", ConditionOperator.Equal, lead.Id));
                    EntityCollection entc = service.RetrieveMultiple(q);

                    foreach (Entity a in entc.Entities)
                    {
                        a["new_khachhang"] = new EntityReference("contact", target.Id);
                        service.Update(a);
                    }
                }
        }
    }
}
