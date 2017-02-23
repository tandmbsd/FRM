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
    public class Plugin_AutoCopyPGNHomgiong : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("new_phieudangkyhomgiong") && target["new_phieudangkyhomgiong"] != null)
            {
                Entity PGNHomgiong = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_phieudangkyhomgiong" }));

                Entity PGNHomgiongNew = service.Retrieve(PGNHomgiong.LogicalName, PGNHomgiong.Id,
                   new ColumnSet(new string[] { "new_dinhmuc_khonghoanlai", "new_dinhmuc_hoanlai_vattu", "new_dinhmuc_hoanlai_tienmat" }));

                List<Entity> DSchitietcu = RetrieveMultiRecord(service, "new_chitietgiaonhanhomgiong", new ColumnSet(true), "new_phieugiaonhanhomgiong", target.Id);
                foreach (Entity a in DSchitietcu)
                    service.Delete(a.LogicalName, a.Id);

                Entity PDKHomGiong = service.Retrieve("new_phieudangkyhomgiong",
                    ((EntityReference)PGNHomgiong["new_phieudangkyhomgiong"]).Id,
                    new ColumnSet("new_dinhmuc_khonghoanlai", "new_dinhmuc_hoanlai_vattu", "new_dinhmuc_hoanlai_tienmat"));

                PGNHomgiongNew["new_dinhmuc_khonghoanlai"] = PDKHomGiong.Contains("new_dinhmuc_khonghoanlai") ? PDKHomGiong["new_dinhmuc_khonghoanlai"] : new Money(0);
                PGNHomgiongNew["new_dinhmuc_hoanlai_vattu"] = PDKHomGiong.Contains("new_dinhmuc_khonghoanlai") ? PDKHomGiong["new_dinhmuc_hoanlai_vattu"] : new Money(0);
                PGNHomgiongNew["new_dinhmuc_hoanlai_tienmat"] = PDKHomGiong.Contains("new_dinhmuc_hoanlai_tienmat") ? PDKHomGiong["new_dinhmuc_hoanlai_tienmat"] : new Money(0);
                service.Update(PGNHomgiongNew);

                EntityReferenceCollection DSthuadatcanhtac = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieugiaonhanhomgiong", "new_new_pgnhomgiong_new_chitiethddtmia", new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_phieugiaonhanhomgiongid", target.Id);

                service.Disassociate("new_phieugiaonhanhomgiong", target.Id, new Relationship("new_new_pgnhomgiong_new_chitiethddtmia"), DSthuadatcanhtac);

                List<Entity> DSChitietdangkyHomgiong = RetrieveMultiRecord(service, "new_chitietdangkyhomgiong", new ColumnSet(true), "new_phieudangkyhomgiong",
                    ((EntityReference)PGNHomgiong["new_phieudangkyhomgiong"]).Id);

                foreach (Entity a in DSChitietdangkyHomgiong)
                {
                    Entity rs = new Entity("new_chitietgiaonhanhomgiong");

                    rs["new_name"] = "Nhận hom giống " + (a.Attributes.Contains("new_giongmia") ? ((EntityReference)a["new_giongmia"]).Name : "");
                    rs["new_phieugiaonhanhomgiong"] = new EntityReference(target.LogicalName, target.Id);
                    rs["new_giongmia"] = a.Attributes.Contains("new_giongmia") ? a["new_giongmia"] : null;
                    rs["new_loaihom"] = a.Attributes.Contains("new_loaihom") ? a["new_loaihom"] : null;
                    rs["new_dongia"] = a.Attributes.Contains("new_dongia") ? a["new_dongia"] : null;
                    rs["new_soluong"] = a.Attributes.Contains("new_soluong") ? a["new_soluong"] : null;                    
                    rs["new_ngaynhan"] = DateTime.Now;
                    rs["new_sotiendthoanlai"] = a.Contains("new_sotienhl") ? a["new_sotienhl"] : new Money(0);
                    rs["new_sotiendtkhonghoanlai"] = a.Contains("new_sotienkhl") ? a["new_sotienkhl"] : new Money(0);
                    rs["new_tongthanhtien"] = new Money((a.Attributes.Contains("new_dongia") ? ((Money)a["new_dongia"]).Value : 0) * (a.Attributes.Contains("new_soluong") ? (decimal)a["new_soluong"] : 0));

                    service.Create(rs);
                }

                EntityReferenceCollection DSthuadatcanhtacDK = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieudangkyhomgiong", "new_new_pdkhomgiong_new_chitiethddtmia", new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_phieudangkyhomgiongid", ((EntityReference)PGNHomgiong["new_phieudangkyhomgiong"]).Id);
                service.Associate("new_phieugiaonhanhomgiong", target.Id, new Relationship("new_new_pgnhomgiong_new_chitiethddtmia"), DSthuadatcanhtacDK);
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
