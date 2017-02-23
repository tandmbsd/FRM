using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace ActionCopy_PDKThuoc
{
    public class ActionCopy_PDKThuoc : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            EntityReference target = (EntityReference)context.InputParameters["Target"];
            if (target.LogicalName == "new_phieudangkythuoc")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity phieudkthuoc = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                if (phieudkthuoc == null)
                {
                    throw new Exception("Phiếu đăng ký thuốc này không tồn tại !!!");
                }

                Entity hopdongdautumia = service.Retrieve("new_hopdongdautumia", ((EntityReference)phieudkthuoc["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_masohopdong" }));
                List<Entity> lstPGNT = RetrieveMultiRecord(service, "new_phieugiaonhanthuoc", new ColumnSet(new string[] { "new_phieugiaonhanthuocid" }), "new_hopdongdautumia", hopdongdautumia.Id);
                string mahopdong = hopdongdautumia.Contains("new_masohopdong") ? (string)hopdongdautumia["new_masohopdong"] : "";
                Entity pgnthuoc = new Entity("new_phieugiaonhanthuoc");

                string tenkhachhang = "";
                string p3 = "";
                int count = lstPGNT.Count +1;

                if (phieudkthuoc.Contains("new_khachhang"))
                {
                    pgnthuoc["new_khachhang"] = phieudkthuoc["new_khachhang"];

                    Entity kh = service.Retrieve("contact", ((EntityReference)phieudkthuoc["new_khachhang"]).Id, new ColumnSet(new string[] { "new_socmnd" }));
                    tenkhachhang = ((EntityReference)phieudkthuoc["new_khachhang"]).Name;
                    p3 = kh.Contains("new_socmnd") ? (string)kh["new_socmnd"] : "";
                }
                else if (phieudkthuoc.Contains("new_khachhangdoanhnghiep"))
                {
                    pgnthuoc["new_khachhangdoanhnghiep"] = phieudkthuoc["new_khachhangdoanhnghiep"];

                    Entity khdn = service.Retrieve("account", ((EntityReference)phieudkthuoc["new_khachhangdoanhnghiep"]).Id, new ColumnSet(new string[] { "new_sogpkd" }));
                    tenkhachhang = ((EntityReference)phieudkthuoc["new_khachhangdoanhnghiep"]).Name;
                    p3 = khdn.Contains("new_sogpkd") ? (string)khdn["new_sogpkd"] : "";
                }

                if (phieudkthuoc.Contains("new_nhacungcapkh"))
                {
                    pgnthuoc["new_nhacungcapkh"] = phieudkthuoc["new_nhacungcapkh"];
                }
                else if (phieudkthuoc.Contains("new_nhacungcapkhdn"))
                {
                    pgnthuoc["new_nhacungcapkhdn"] = phieudkthuoc["new_nhacungcapkhdn"];
                }

                pgnthuoc["new_lannhan"] = count;
                pgnthuoc["new_name"] = "PGNT-" + tenkhachhang + "-" + p3 + "-" + mahopdong + "-" + "L" + count;
                pgnthuoc["new_hopdongdautumia"] = phieudkthuoc.Contains("new_hopdongdautumia") ? phieudkthuoc["new_hopdongdautumia"] : "";
                pgnthuoc["new_vudautu"] = phieudkthuoc.Contains("new_vudautu") ? phieudkthuoc["new_vudautu"] : "";
                pgnthuoc["new_tram"] = phieudkthuoc.Contains("new_tram") ? phieudkthuoc["new_tram"] : "";
                pgnthuoc["new_canbonongvu"] = phieudkthuoc.Contains("new_canbonongvu") ? phieudkthuoc["new_canbonongvu"] : "";
                pgnthuoc["new_ngaylapphieu"] = phieudkthuoc.Contains("new_ngaylapphieu") ? phieudkthuoc["new_ngaylapphieu"] : "";
                pgnthuoc["new_phieudangkythuoc"] = new EntityReference(phieudkthuoc.LogicalName, phieudkthuoc.Id);
                pgnthuoc["new_ngayduyet"] = phieudkthuoc.Contains("new_ngayduyet") ? phieudkthuoc["new_ngayduyet"] : "";
                pgnthuoc["new_dinhmuc_khonghoanlai"] = phieudkthuoc.Contains("new_denghi_khonghoanlai") ? phieudkthuoc["new_denghi_khonghoanlai"] : new Money(0);
                pgnthuoc["new_dinhmuc_hoanlai_vattu"] = phieudkthuoc.Contains("new_denghi_hoanlai_vattu") ? phieudkthuoc["new_denghi_hoanlai_vattu"] : new Money(0);
                pgnthuoc["new_dinhmuc_hoanlai_tienmat"] = phieudkthuoc.Contains("new_denghi_hoanlai_tienmat") ? phieudkthuoc["new_denghi_hoanlai_tienmat"] : new Money(0);
                pgnthuoc["new_laytupdk"] = true;
                Guid idPGNT = service.Create(pgnthuoc);

                List<Entity> DSchitietcu = RetrieveMultiRecord(service, "new_chitietgiaonhanthuoc", new ColumnSet(true), "new_phieugiaonhanthuoc", idPGNT);
                foreach (Entity a in DSchitietcu)
                    service.Delete(a.LogicalName, a.Id);

                List<Entity> DSChitietdangkyThuoc = RetrieveMultiRecord(service, "new_chitietdangkythuoc", new ColumnSet(true), "new_phieudangkythuoc", phieudkthuoc.Id);

                Entity temp = service.Retrieve(pgnthuoc.LogicalName, idPGNT, new ColumnSet(new string[] { "new_masophieu" }));
                string maphieu = temp["new_masophieu"].ToString();
                decimal i = 0;
                foreach (Entity a in DSChitietdangkyThuoc)
                {
                    i++;
                    StringBuilder str = new StringBuilder();
                    str.Append("CTGNTHUOC-" + mahopdong + "-L" + count);
                    str.Append("-CT" + i);

                    StringBuilder str1 = new StringBuilder();
                    str1.Append(maphieu + "_" + i.ToString());

                    Entity rs = new Entity("new_chitietgiaonhanthuoc");
                    rs["new_name"] = str.ToString();
                    rs["new_current"] = i;
                    rs["new_ma"] = str1.ToString();
                    rs["new_phieugiaonhanthuoc"] = new EntityReference("new_phieugiaonhanthuoc", idPGNT);
                    rs["new_thuoc"] = a.Attributes.Contains("new_thuoc") ? a["new_thuoc"] : null;
                    rs["new_dongia"] = a.Attributes.Contains("new_dongia") ? a["new_dongia"] : null;
                    rs["new_soluongdangky"] = a.Attributes.Contains("new_soluong") ? a["new_soluong"] : null;
                    rs["new_thanhtien1"] = new Money((a.Attributes.Contains("new_dongia") ? ((Money)a["new_dongia"]).Value : 0) * (a.Attributes.Contains("new_soluong") ? (decimal)a["new_soluong"] : 0));
                    rs["new_sotienhl"] = a.Contains("new_sotienhl") ? a["new_sotienhl"] : new Money(0);
                    rs["new_sotienkhl"] = a.Contains("new_sotienkhl") ? a["new_sotienkhl"] : new Money(0);
                    service.Create(rs);
                }

                List<Entity> lstThuadatcanhtac = RetrieveMultiRecord(service, "new_thuadatcanhtac",
                    new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_hopdongdautumia", hopdongdautumia.Id);

                EntityReferenceCollection nnrecord = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieugiaonhanthuoc",
                    "new_new_pgnthuoc_new_chitiethddtmia", new ColumnSet(true), "new_phieugiaonhanthuocid", idPGNT);

                service.Disassociate("new_phieugiaonhanthuoc", idPGNT, new Relationship("new_new_pgnthuoc_new_chitiethddtmia"), nnrecord);

                EntityReferenceCollection entcRef1 = new EntityReferenceCollection();

                foreach (Entity a in lstThuadatcanhtac)
                {
                    entcRef1.Add(a.ToEntityReference());
                }

                service.Associate("new_phieugiaonhanthuoc", idPGNT, new Relationship("new_new_pgnthuoc_new_chitiethddtmia"), entcRef1);

                context.OutputParameters["ReturnId"] = idPGNT.ToString();
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
