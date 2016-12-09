using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_AutoCopyGiaoNhanThuoc
{
    public class Plugin_AutoCopyGiaoNhanThuoc : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            
            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("new_phieudangkythuoc"))
            {
                Entity PGNThuoc = service.Retrieve("new_phieugiaonhanthuoc", target.Id, new ColumnSet(true));
                List<Entity> DSchitietcu = RetrieveMultiRecord(service, "new_chitietgiaonhanthuoc", new ColumnSet(true), "new_phieugiaonhanthuoc", target.Id);
                foreach (Entity a in DSchitietcu)
                    service.Delete(a.LogicalName, a.Id);

                EntityReferenceCollection DSThuadat = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieugiaonhanthuoc", "new_new_pgnthuoc_new_chitiethddtmia", new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_phieugiaonhanthuocid", target.Id);

                service.Disassociate("new_phieugiaonhanthuoc", target.Id, new Relationship("new_new_pgnthuoc_new_chitiethddtmia"), DSThuadat);

                List<Entity> DSChitietdangkyThuoc = RetrieveMultiRecord(service, "new_chitietdangkythuoc", new ColumnSet(true), "new_phieudangkythuoc", ((EntityReference)PGNThuoc["new_phieudangkythuoc"]).Id);
                foreach (Entity a in DSChitietdangkyThuoc)
                {
                    Entity rs = new Entity("new_chitietgiaonhanthuoc");
                    rs["new_name"] = "Nhận thuốc " + ((EntityReference)a["new_thuoc"]).Name;
                    rs["new_phieugiaonhanthuoc"] = new EntityReference("new_phieugiaonhanthuoc", target.Id);
                    rs["new_thuoc"] = a.Attributes.Contains("new_thuoc") ? a["new_thuoc"] : null;
                    rs["new_dongia"] = a.Attributes.Contains("new_dongia") ? a["new_dongia"] : null;
                    rs["new_soluongdangky"] = a.Attributes.Contains("new_soluong") ? a["new_soluong"] : null;
                    service.Create(rs);
                }

                EntityReferenceCollection DSThuadatDK = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieudangkythuoc", "new_new_pdkthuoc_new_chitiethddtmia", new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_phieudangkythuocid", ((EntityReference)PGNThuoc["new_phieudangkythuoc"]).Id);
                service.Associate("new_phieugiaonhanthuoc", target.Id, new Relationship("new_new_pgnthuoc_new_chitiethddtmia"), DSThuadatDK);
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
