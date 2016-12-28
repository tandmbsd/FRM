using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace PDKHomGiong_CreateChitiet
{
    public class PDKHomGiong_CreateChitiet : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            ITracingService trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("new_hopdongdautumia") && target.GetAttributeValue<EntityReference>("new_hopdongdautumia") != null)
            {
                EntityReference hddtmEnf = ((EntityReference)target["new_hopdongdautumia"]);
                EntityReferenceCollection DSthuadatcanhtac = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieudangkyhomgiong", "new_new_pdkhomgiong_new_chitiethddtmia",
                    new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_phieudangkyhomgiongid", target.Id);
                
                service.Disassociate("new_phieudangkyhomgiong", target.Id, new Relationship("new_new_pdkhomgiong_new_chitiethddtmia"), DSthuadatcanhtac);
                
                //List<Entity> lstChitiet = RetrieveMultiRecord(service, "new_thuadatcanhtac",
                //    new ColumnSet(new string[] { "new_loaigocmia" }), "new_hopdongdautumia", hddtmEnf.Id);

                QueryExpression q = new QueryExpression("new_thuadatcanhtac");
                q.ColumnSet = new ColumnSet(new string[] { "new_thuadatcanhtacid" });
                q.Criteria = new FilterExpression();
                q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, hddtmEnf.Id));
                q.Criteria.AddCondition(new ConditionExpression("new_loaigocmia", ConditionOperator.Equal, 100000000));
                EntityCollection lstChitiet = service.RetrieveMultiple(q);

                EntityReferenceCollection DSthuadatcanhtacNew = new EntityReferenceCollection();
                
                foreach (Entity e in lstChitiet.Entities)
                {
                    DSthuadatcanhtacNew.Add(e.ToEntityReference());
                }
                
                service.Associate("new_phieudangkyhomgiong", target.Id, new Relationship("new_new_pdkhomgiong_new_chitiethddtmia"), DSthuadatcanhtacNew);
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

        EntityReferenceCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
        {
            EntityReferenceCollection result = new EntityReferenceCollection();
            QueryExpression query = new QueryExpression(entity1);
            query.ColumnSet = column;
            LinkEntity linkEntity1 = new LinkEntity(entity1, relateName, entity1 + "id", entity1 + "id", JoinOperator.Inner);
            LinkEntity linkEntity2 = new LinkEntity(relateName, entity2, entity2 + "id", entity2 + "id", JoinOperator.Inner);

            linkEntity1.LinkEntities.Add(linkEntity2);
            query.LinkEntities.Add(linkEntity1);

            linkEntity2.LinkCriteria = new FilterExpression();
            linkEntity2.LinkCriteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection collRecords = service.RetrieveMultiple(query);

            foreach (Entity a in collRecords.Entities)
            {
                result.Add(new EntityReference(entity1, a.Id));
            }

            return result;
        }
    }
}
