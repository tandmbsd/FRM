using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;

namespace UpdateTSTCFromHDTheChap
{
    public class UpdateTSTCFromHDTheChap : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        ITracingService trace;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("statuscode"))
            {
                int tinhtrang = ((OptionSetValue)target["statuscode"]).Value;
                List<Entity> lstTaisanthechap = RetrieveMultiRecord(service, "new_taisanthechap",
                    new ColumnSet(new string[] { "statuscode" }), "new_hopdongthechap", target.Id);

                if (tinhtrang == 100000000) // da ky 
                {
                    foreach (Entity en in lstTaisanthechap)
                    {
                        Entity k = new Entity(en.LogicalName);
                        k.Id = en.Id;
                        k["statuscode"] = new OptionSetValue(100000000); //the chap
                        service.Update(k);
                    }
                }
                else if (tinhtrang == 100000001) // thanh ly
                {
                    foreach (Entity en in lstTaisanthechap)
                    {
                        Entity k = new Entity(en.LogicalName);
                        k.Id = en.Id;
                        k["statuscode"] = new OptionSetValue(100000001); // giai chap
                        service.Update(k);
                    }
                }
            }
        }

        List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>();
        }
    }
}
