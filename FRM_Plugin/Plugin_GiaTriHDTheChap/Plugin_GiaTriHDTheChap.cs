using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_GiaTriHDTheChap
{
    public class Plugin_GiaTriHDTheChap : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];
            if (target.LogicalName == "new_taisanthechap")
            {
                Entity TSTC = service.Retrieve("new_taisanthechap", target.Id, new ColumnSet(true));
                QueryExpression q = new QueryExpression("new_taisanthechap");
                q.ColumnSet = new ColumnSet(new string[] { "new_taisanthechapid", "new_giatridinhgiagiatrithechap" });
                q.Criteria = new FilterExpression();
                q.Criteria.AddCondition(new ConditionExpression("new_hopdongthechap", ConditionOperator.Equal, ((EntityReference)TSTC["new_hopdongthechap"]).Id));
                EntityCollection entc = service.RetrieveMultiple(q);

                Decimal giatrithucte = 0;
                
                foreach (Entity a in entc.Entities)
                {
                    giatrithucte += (a.Attributes.Contains("new_giatridinhgiagiatrithechap") ? ((Money)a["new_giatridinhgiagiatrithechap"]).Value : new decimal(0));
                }
                Entity rs = new Entity("new_hopdongthechap");
                rs.Id = ((EntityReference)TSTC["new_hopdongthechap"]).Id;
                rs["new_tonggiatrihd"] = new Money(giatrithucte);

                service.Update(rs);
            }
        }
    }
}
