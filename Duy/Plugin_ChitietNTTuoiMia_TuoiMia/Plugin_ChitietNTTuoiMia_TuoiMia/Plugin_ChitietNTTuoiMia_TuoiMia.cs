using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_ChitietNTTuoiMia_TuoiMia
{
    public class Plugin_ChitietNTTuoiMia_TuoiMia : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("new_thuadat") && context.Depth < 2)
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                
                Entity chitietnghiemthutuoimia = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                decimal sumdientich = 0;
                int count = 0;
                //throw new Exception("a");
                QueryExpression q = new QueryExpression("new_tuoimia");
                q.ColumnSet = new ColumnSet(true);
                q.Criteria = new FilterExpression();
                q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 1));
                LinkEntity linkEntity1 = new LinkEntity("new_tuoimia", "new_thuadatcanhtac", "new_thuacanhtac", "new_thuacanhtacid", JoinOperator.Inner);
                linkEntity1.LinkCriteria = new FilterExpression();
                linkEntity1.LinkCriteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, ((EntityReference)target["new_thuadat"]).Id));
                EntityCollection entc = service.RetrieveMultiple(q);
                
                List<Entity> lstTuoimia = entc.Entities.OrderBy(p => p.GetAttributeValue<DateTime>("actualstart")).ToList<Entity>();
                
                count = lstTuoimia.Count;
                chitietnghiemthutuoimia["new_solantuoi"] = count;
                
                if(lstTuoimia.Count == 1){
                    chitietnghiemthutuoimia["new_ngaybatdautuoi"] = lstTuoimia[0]["actualstart"];
                    chitietnghiemthutuoimia["new_ngaytuoi"] = lstTuoimia[0]["actualstart"];
                }
                else if(lstTuoimia.Count > 1){
                    chitietnghiemthutuoimia["new_ngaybatdautuoi"] = lstTuoimia[0]["actualstart"];
                    chitietnghiemthutuoimia["new_ngaytuoi"] = lstTuoimia[count - 1]["actualstart"];
                }
                
                foreach (Entity en in entc.Entities)
                {
                    sumdientich += en.Contains("new_dientichthuchien") ? (decimal)en["new_dientichthuchien"] : 0;                    
                    Entity chitietnghiemthutuoimia_tuoimia = new Entity("new_chitietnghiemthutuoimia_tuoimia");
                    chitietnghiemthutuoimia_tuoimia["new_chitietnghiemthutuoimia"] = new EntityReference(target.LogicalName, target.Id);
                    chitietnghiemthutuoimia_tuoimia["new_tuoimia"] = new EntityReference(en.LogicalName, en.Id);
                    service.Create(chitietnghiemthutuoimia_tuoimia);
                }
                service.Update(chitietnghiemthutuoimia);
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

        Entity RetrieveSingleRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>().FirstOrDefault();
        }

        EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
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

            return collRecords;
        }
    }
}
