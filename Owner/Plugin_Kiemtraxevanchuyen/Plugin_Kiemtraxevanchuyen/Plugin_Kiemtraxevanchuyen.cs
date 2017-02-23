using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Plugin_Kiemtraxevanchuyen
{
    public class Plugin_Kiemtraxevanchuyen : IPlugin
    {
        private IOrganizationService service = null;
        private IOrganizationServiceFactory factory = null;
        public ITracingService trace;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("new_xevanchuyen"))
            {
                int count = 0;
                Entity thexe = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_hopdongvanchuyen" }));
                Guid xevanchuyen = ((EntityReference)target["new_xevanchuyen"]).Id;

                if (!thexe.Contains("new_hopdongvanchuyen"))
                    throw new Exception("Thẻ xe không có hợp đồng vận chuyển");

                Entity hdvc = service.Retrieve("new_hopdongvanchuyen", ((EntityReference)thexe["new_hopdongvanchuyen"]).Id,
                    new ColumnSet(new string[] { "new_hopdongvanchuyenid" }));

                List<Entity> lstThexe = RetrieveMultiRecord(service, "new_hopdongvanchuyen_xevanchuyen",
                    new ColumnSet(new string[] { "new_xevanchuyen" }), "new_hopdongvanchuyen", hdvc.Id);

                foreach (Entity a in lstThexe)
                    if (a.Contains("new_xevanchuyen") && ((EntityReference)a["new_xevanchuyen"]).Id == xevanchuyen)
                        count++;

                if (count > 1)
                    throw new Exception("Xe vận chuyển đã tồn tại trong hợp đồng này");

                count = 0;

                QueryExpression q = new QueryExpression("new_hopdongvanchuyen_xevanchuyen");
                q.ColumnSet = new ColumnSet(new string[] { "new_xevanchuyen" });
                q.Criteria = new FilterExpression();
                q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));
                EntityCollection entc = service.RetrieveMultiple(q);

                foreach (Entity en in entc.Entities)                
                    if (en.Contains("new_xevanchuyen") && ((EntityReference)en["new_xevanchuyen"]).Id == xevanchuyen)                    
                        count++;

                if (count > 0)
                    throw new Exception("Xe vận chuyện đã thuộc hợp đồng vận chuyển khác");
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
