using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace UpDateTinhTrangChiTietTuHD
{
    public class UpDateTinhTrangChiTietTuHD : IPlugin
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

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000003)
            {
                List<Entity> lstChitiet = RetrieveMultiRecord(service, "new_thuadatcanhtac", new ColumnSet(new string[] { "statuscode" }), "new_hopdongdautumia", target.Id);
                trace.Trace("1");
                foreach (Entity en in lstChitiet)
                {
                    if (en.Contains("statuscode") && ((OptionSetValue)en["statuscode"]).Value == 1)
                    {
                        Entity newCT = service.Retrieve(en.LogicalName, en.Id, new ColumnSet(new string[] { "statuscode" }));
                        newCT["statuscode"] = new OptionSetValue(100000000);
                        service.Update(newCT);
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
