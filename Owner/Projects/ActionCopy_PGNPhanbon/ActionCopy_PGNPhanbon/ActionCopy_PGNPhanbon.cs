using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace ActionCopy_PGNPhanbon
{
    public class ActionCopy_PGNPhanbon : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            EntityReference target = (EntityReference)context.InputParameters["Target"];
            if (target.LogicalName == "new_phieudangkyphanbon")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity phieudkphanbon = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                if (phieudkphanbon == null)
                {
                    throw new Exception("Phiếu đăng ký phân bón này không tồn tại !!!");
                }

                Entity hopdongdautumia = service.Retrieve("new_hopdongdautumia", ((EntityReference)phieudkphanbon["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_masohopdong" }));
                List<Entity> lstPGNHG = RetrieveMultiRecord(service, "new_phieugiaonhanphanbon", new ColumnSet(new string[] { "new_phieugiaonhanphanbonid" }), "new_hopdongdautumia", hopdongdautumia.Id);
                string mahopdong = hopdongdautumia.Contains("new_masohopdong") ? (string)hopdongdautumia["new_masohopdong"] : "";
                Entity pgnphanbon = new Entity("new_phieugiaonhanphanbon");

                string tenkhachhang = "";
                string p3 = "";
                int count = lstPGNHG.Count + 1;

                pgnphanbon["new_vudautu"] = phieudkphanbon["new_vudautu"];
                pgnphanbon["new_ngaylapphieu"] = phieudkphanbon["new_ngaylapphieu"];

                if (phieudkphanbon.Contains("new_khachhang"))
                {
                    pgnphanbon["new_khachhang"] = phieudkphanbon["new_khachhang"];

                    Entity kh = service.Retrieve("contact", ((EntityReference)phieudkphanbon["new_khachhang"]).Id, new ColumnSet(new string[] { "new_socmnd" }));
                    tenkhachhang = ((EntityReference)phieudkphanbon["new_khachhang"]).Name;
                    p3 = kh.Contains("new_socmnd") ? (string)kh["new_socmnd"] : "";

                }
                else if (phieudkphanbon.Contains("new_khachhangdoanhnghiep"))
                {
                    pgnphanbon["new_khachhangdoanhnghiep"] = phieudkphanbon["new_khachhangdoanhnghiep"];

                    Entity khdn = service.Retrieve("account", ((EntityReference)phieudkphanbon["new_khachhangdoanhnghiep"]).Id, new ColumnSet(new string[] { "new_sogpkd" }));
                    tenkhachhang = ((EntityReference)phieudkphanbon["new_khachhangdoanhnghiep"]).Name;
                    p3 = khdn.Contains("new_sogpkd") ? (string)khdn["new_sogpkd"] : "";
                }

                if (phieudkphanbon.Contains("new_doitaccungcapdichvukh"))
                {
                    pgnphanbon["new_nhacungcapkh"] = phieudkphanbon["new_doitaccungcapdichvukh"];
                }
                else if (phieudkphanbon.Contains("new_doitaccungcapdichvukhdn"))
                {
                    pgnphanbon["new_nhacungcapkhdn"] = phieudkphanbon["new_doitaccungcapdichvukhdn"];
                }

                pgnphanbon["new_lannhan"] = count;
                pgnphanbon["new_name"] = "PGNPB-" + tenkhachhang + "-" + p3 + "-" + mahopdong + "-" + "L" + count;
                pgnphanbon["new_phieudangkyphanbon"] = phieudkphanbon.ToEntityReference();
                pgnphanbon["new_hopdongdautumia"] = phieudkphanbon.Contains("new_hopdongdautumia") ? phieudkphanbon["new_hopdongdautumia"] : "";
                pgnphanbon["new_tram"] = phieudkphanbon.Contains("new_tram") ? phieudkphanbon["new_tram"] : "";
                pgnphanbon["new_canbonongvu"] = phieudkphanbon.Contains("new_canbonongvu") ? phieudkphanbon["new_canbonongvu"] : "";

                pgnphanbon["new_dinhmuc_khonghoanlai"] = phieudkphanbon.Contains("new_denghi_khonghoanlai") ? phieudkphanbon["new_denghi_khonghoanlai"] : new Money(0);
                pgnphanbon["new_dinhmuc_hoanlai_vattu"] = phieudkphanbon.Contains("new_denghi_hoanlai_vattu") ? phieudkphanbon["new_denghi_hoanlai_vattu"] : new Money(0);
                pgnphanbon["new_dinhmuc_hoanlai_tienmat"] = phieudkphanbon.Contains("new_denghi_hoanlai_tienmat") ? phieudkphanbon["new_denghi_hoanlai_tienmat"] : new Money(0);

                Guid idPGNPB = service.Create(pgnphanbon);
                
                Entity temp = service.Retrieve(pgnphanbon.LogicalName, idPGNPB, new ColumnSet(new string[] { "new_masophieu" }));
                string maphieu = temp.Contains("new_masophieu") ?  temp["new_masophieu"].ToString() : "";

                List<Entity> DSchitietcu = RetrieveMultiRecord(service, "new_chitietgiaonhanphanbon", new ColumnSet(true), "new_phieugiaonhanphanbon", idPGNPB);
                foreach (Entity a in DSchitietcu)
                    service.Delete(a.LogicalName, a.Id);

                List<Entity> DSCtPhanBon = RetrieveMultiRecord(service, "new_chitietdangkyphanbon", new ColumnSet(true),
                    "new_phieudangkyphanbon", phieudkphanbon.Id);
                
                decimal i = 0;
                foreach (Entity a in DSCtPhanBon)
                {
                    i++;
                    StringBuilder str = new StringBuilder();
                    str.Append(maphieu + "_" + i.ToString());

                    StringBuilder str1 = new StringBuilder();
                    str1.Append("CTGNPB-" + mahopdong + "-L" + count);
                    str1.Append("-CT" + i);

                    Entity rs = new Entity("new_chitietgiaonhanphanbon");

                    rs["new_ma"] = str.ToString();
                    rs["new_name"] = str1.ToString();
                    rs["new_current"] = i;
                    rs["new_phieugiaonhanphanbon"] = new EntityReference("new_phieugiaonhanphanbon", idPGNPB);
                    rs["new_donvitinh"] = a.Attributes.Contains("new_donvitinh") ? a["new_donvitinh"] : null;
                    rs["new_soluong"] = a.Attributes.Contains("new_soluong") ? a["new_soluong"] : null;
                    rs["new_dongia"] = a.Attributes.Contains("new_dongia") ? a["new_dongia"] : null;
                    rs["new_loaibon"] = a.Contains("new_loaibon") ? a["new_loaibon"] : null;
                    rs["new_phanbon"] = a.Contains("new_phanbon") ? a["new_phanbon"] : null;
                    rs["new_ngaynhan"] = DateTime.Now;
                    rs["new_sotienhl"] = a.Contains("new_sotienhl") ? a["new_sotienhl"] : new Money(0);
                    rs["new_sotienkhl"] = a.Contains("new_sotienkhl") ? a["new_sotienkhl"] : new Money(0);
                    rs["new_thanhtien1"] = new Money((a.Attributes.Contains("new_dongia") ? ((Money)a["new_dongia"]).Value : 0) * (a.Attributes.Contains("new_soluong") ? (decimal)a["new_soluong"] : 0));
                    service.Create(rs);
                }

                EntityCollection nnrecord = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieugiaonhanphanbon",
                    "new_new_pgnphanbon_new_chitiethddtmia", new ColumnSet(true), "new_phieugiaonhanphanbonid", idPGNPB);
                EntityReferenceCollection entcRef = new EntityReferenceCollection();

                foreach (Entity a in nnrecord.Entities)
                {
                    entcRef.Add(a.ToEntityReference());
                }

                service.Disassociate("new_phieugiaonhanphanbon", idPGNPB, new Relationship("new_new_pgnphanbon_new_chitiethddtmia"), entcRef);

                List<Entity> lstThuadatcanhtac = RetrieveMultiRecord(service, "new_thuadatcanhtac",
                    new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_hopdongdautumia", hopdongdautumia.Id);

                EntityReferenceCollection entcRef1 = new EntityReferenceCollection();

                foreach (Entity a in lstThuadatcanhtac)
                {
                    entcRef1.Add(a.ToEntityReference());
                }

                //Entity t = service.Retrieve(phieudkphanbon.LogicalName, phieudkphanbon.Id,
                //    new ColumnSet(new string[] { "statuscode" }));
                //t["statuscode"] = new OptionSetValue(100000002);
                //service.Update(t);

                service.Associate("new_phieugiaonhanphanbon", idPGNPB, new Relationship("new_new_pgnphanbon_new_chitiethddtmia"), entcRef1);

                context.OutputParameters["ReturnId"] = idPGNPB.ToString();
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

            foreach (Entity a in collRecords.Entities)
            {
                result.Add(new EntityReference(entity1, a.Id));
            }

            return collRecords;
        }

        EntityReferenceCollection RetrieveNNRecordEnF(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
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
