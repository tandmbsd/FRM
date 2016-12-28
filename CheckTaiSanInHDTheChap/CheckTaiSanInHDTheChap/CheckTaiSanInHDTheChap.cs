using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace CheckTaiSanInHDTheChap
{
    public class CheckTaiSanInHDTheChap : IPlugin
    {
        // moi nhat
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {

            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];
            int count = 0;

            if (context.MessageName == "Update" || context.MessageName == "Create")
            {
                if (target.Contains("new_taisan"))
                {
                    // Check tai san in hd the chap
                    Entity taisanthechap = service.Retrieve(target.LogicalName, target.Id,
                        new ColumnSet(new string[] { "new_taisan", "new_hopdongthechap", "new_name" }));

                    List<Entity> lstTaisanthechap = RetrieveMultiRecord(service, "new_taisanthechap",
                        new ColumnSet(new string[] { "new_taisanthechapid", "new_taisan", "new_name" }),
                        "new_hopdongthechap", ((EntityReference)taisanthechap["new_hopdongthechap"]).Id);

                    foreach (Entity en in lstTaisanthechap)
                    {
                        if (!en.Contains("new_taisan") || !taisanthechap.Contains("new_taisan"))
                            continue;

                        if (((EntityReference)en["new_taisan"]).Id == ((EntityReference)taisanthechap["new_taisan"]).Id)
                            count++;
                    }

                    if (count > 1)
                        throw new Exception("Tàn sản đã tồn tại trong hợp đồng thế chấp này");

                    // check tai san trong hd the chap khac

                    QueryExpression q = new QueryExpression("new_taisanthechap");
                    q.ColumnSet = new ColumnSet(new string[] { "new_taisan", "new_hopdongthechap" });

                    LinkEntity lnEntity = new LinkEntity("new_taisanthechap", "new_hopdongthechap", "new_hopdongthechap",
                        "new_hopdongthechapid", JoinOperator.Inner);
                    lnEntity.LinkCriteria = new FilterExpression();
                    lnEntity.LinkCriteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));

                    q.LinkEntities.Add(lnEntity);
                    EntityCollection entc = service.RetrieveMultiple(q);

                    count = 0;
                    
                    foreach (Entity en in entc.Entities)
                    {
                        if (!en.Contains("new_taisan"))
                            continue;

                        if (((EntityReference)en["new_taisan"]).Id == ((EntityReference)target["new_taisan"]).Id)
                        {
                            Entity hdtcExisted = service.Retrieve("new_hopdongthechap",
                                ((EntityReference)en["new_hopdongthechap"]).Id, new ColumnSet(new string[] { "new_mahopdong" }));

                            throw new Exception("Tài sản đã thuộc hợp đồng thế chấp có mã " + hdtcExisted["new_mahopdong"].ToString());
                        }
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
