using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_AutoCopyGiaoNhanVatTu
{
    public class Plugin_AutoCopyGiaoNhanVatTu : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            
            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("new_phieudangkyvattu"))
            {
                Entity PGNVattu = service.Retrieve("new_phieugiaonhanvattu", target.Id, new ColumnSet(true));
                List<Entity> DSchitietcu = RetrieveMultiRecord(service, "new_chitietgiaonhanvattu", new ColumnSet(true), "new_phieugiaonhanvattu", target.Id);
                foreach (Entity a in DSchitietcu)
                    service.Delete(a.LogicalName, a.Id);

                EntityReferenceCollection DSThuadat = RetrieveNNRecord(service, "new_thuadat", "new_phieugiaonhanvattu", "new_new_phieugiaonhanvattu_new_thuadat", new ColumnSet(new string[] { "new_thuadatid" }), "new_phieugiaonhanvattuid", target.Id);

                service.Disassociate("new_phieugiaonhanvattu", target.Id, new Relationship("new_new_phieugiaonhanvattu_new_thuadat"), DSThuadat);

                List<Entity> DSChitietdangkyVattu = RetrieveMultiRecord(service, "new_chitietdangkyvattu", new ColumnSet(true), "new_phieudangkyvattu", ((EntityReference)PGNVattu["new_phieudangkyvattu"]).Id);
                foreach (Entity a in DSChitietdangkyVattu)
                {
                    Entity rs = new Entity("new_chitietgiaonhanvattu");
                    rs["new_name"] = "Nhận vật tư " + ((EntityReference)a["new_vattu"]).Name;
                    rs["new_phieugiaonhanvattu"] = new EntityReference("new_phieugiaonhanvattu", target.Id);
                    rs["new_vattu"] = a.Attributes.Contains("new_vattu") ? a["new_vattu"] : null;
                    rs["new_dongia"] = a.Attributes.Contains("new_dongia") ? a["new_dongia"] : null;
                    rs["new_soluong"] = a.Attributes.Contains("new_soluong") ?  a["new_soluong"] : null;
                    service.Create(rs);
                }

                EntityReferenceCollection DSThuadatDK = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieudangkyvattu", "new_new_pdkvattu_new_chitiethddtmia", new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_phieudangkyvattuid", ((EntityReference)PGNVattu["new_phieudangkyvattu"]).Id);
                service.Associate("new_phieugiaonhanvattu", target.Id, new Relationship("new_new_pgnvattu_new_chitiethddtmia"), DSThuadatDK);
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
            LinkEntity linkEntity1 = new LinkEntity(entity1, relateName, entity1 + "id",entity1 + "id" , JoinOperator.Inner);
            LinkEntity linkEntity2 = new LinkEntity(relateName, entity2, entity2 + "id",entity2 + "id" , JoinOperator.Inner);

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
