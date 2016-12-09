using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_AutoCopyCSHThuaDatToHopDong
{
    public class Plugin_AutoCopyCSHThuaDatToHopDong : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            
            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("new_thuadat"))
            {
                Entity ChitietHD = service.Retrieve("new_thuadatcanhtac", target.Id, new ColumnSet(true));

                EntityReferenceCollection DSKhCu = RetrieveNNRecord(service, "contact", "new_thuadatcanhtac", "new_new_thuadatcanhtackh_khachhang", new ColumnSet(new string[] { "contactid" }), "new_thuadatcanhtacid", target.Id);
                service.Disassociate("new_thuadatcanhtac", target.Id, new Relationship("new_new_thuadatcanhtackh_khachhang"), DSKhCu);

                Entity Thuadat = service.Retrieve("new_thuadat", ((EntityReference)target["new_thuadat"]).Id, new ColumnSet(true));
                if (Thuadat.Contains("new_taisan"))
                {
                    Entity Taisan = service.Retrieve("new_taisan", ((EntityReference)Thuadat["new_taisan"]).Id, new ColumnSet(true));

                    EntityReferenceCollection DSKhDK = RetrieveNNRecord(service, "contact", "new_taisan", "new_new_taisankh_chutaisan", new ColumnSet(new string[] { "contactid" }), "new_taisanid", Taisan.Id);
                    service.Associate("new_thuadatcanhtac", target.Id, new Relationship("new_new_thuadatcanhtackh_khachhang"), DSKhDK);

                    Entity new_ctHD = new Entity("new_thuadatcanhtac");
                    if (Taisan.Contains("new_khachhang"))
                        new_ctHD["new_chusohuuchinhtd"] = Taisan["new_khachhang"];
                    else new_ctHD["new_chusohuuchinhtdkhdn"] = Taisan["new_khachhangdoanhnghiep"];
                    new_ctHD.Id = ChitietHD.Id;
                    service.Update(new_ctHD);
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
