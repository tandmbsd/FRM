using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace ActionCopyPDKVatTu
{
    public class ActionCopyPDKVatTu : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            EntityReference target = (EntityReference)context.InputParameters["Target"];

            if (target.LogicalName == "new_phieudangkyvattu")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity phieudkvattu = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                if (phieudkvattu == null)
                {
                    throw new Exception("Phiếu đăng ký vật tư này không tồn tại !!!");
                }

                Entity hopdongdautumia = service.Retrieve("new_hopdongdautumia", ((EntityReference)phieudkvattu["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_masohopdong" }));
                List<Entity> lstPGNVatTu = RetrieveMultiRecord(service, "new_phieugiaonhanvattu", new ColumnSet(new string[] { "new_phieugiaonhanvattuid" }), "new_hopdongdautumia", hopdongdautumia.Id);
                string mahopdong = hopdongdautumia.Contains("new_masohopdong") ? (string)hopdongdautumia["new_masohopdong"] : "";

                string tenkhachhang = "";
                string p3 = "";
                int count = lstPGNVatTu.Count + 1;

                Entity pgnvattu = new Entity("new_phieugiaonhanvattu");

                if (phieudkvattu.Contains("new_khachhang"))
                {
                    pgnvattu["new_khachhang"] = phieudkvattu["new_khachhang"];

                    Entity kh = service.Retrieve("contact", ((EntityReference)phieudkvattu["new_khachhang"]).Id, new ColumnSet(new string[] { "new_socmnd" }));
                    tenkhachhang = ((EntityReference)phieudkvattu["new_khachhang"]).Name;
                    p3 = kh.Contains("new_socmnd") ? (string)kh["new_socmnd"] : "";
                }
                else if (phieudkvattu.Contains("new_khachhangdoanhnghiep"))
                {
                    pgnvattu["new_khachhangdoanhnghiep"] = phieudkvattu["new_khachhangdoanhnghiep"];

                    Entity khdn = service.Retrieve("account", ((EntityReference)phieudkvattu["new_khachhangdoanhnghiep"]).Id, new ColumnSet(new string[] { "new_sogpkd" }));
                    tenkhachhang = ((EntityReference)phieudkvattu["new_khachhangdoanhnghiep"]).Name;
                    p3 = khdn.Contains("new_sogpkd") ? (string)khdn["new_sogpkd"] : "";
                }

                if (phieudkvattu.Contains("new_nhacungcapkh"))
                {
                    pgnvattu["new_nhacungcapkh"] = phieudkvattu["new_nhacungcapkh"];
                }
                else if (phieudkvattu.Contains("new_nhacungcapkhdn"))
                {
                    pgnvattu["new_nhacungcapkhdn"] = phieudkvattu["new_nhacungcapkhdn"];
                }

                pgnvattu["new_lannhan"] = count;
                pgnvattu["new_name"] = "PGNVT-" + tenkhachhang + "-" + p3 + "-" + mahopdong + "-" + "L" + count;
                pgnvattu["new_hopdongdautumia"] = phieudkvattu.Contains("new_hopdongdautumia") ? phieudkvattu["new_hopdongdautumia"] : "";
                pgnvattu["new_vudautu"] = phieudkvattu.Contains("new_vudautu") ? phieudkvattu["new_vudautu"] : "";
                pgnvattu["new_tram"] = phieudkvattu.Contains("new_tram") ? phieudkvattu["new_tram"] : "";
                pgnvattu["new_canbonongvu"] = phieudkvattu.Contains("new_canbonongvu") ? phieudkvattu["new_canbonongvu"] : "";
                pgnvattu["new_ngaylapphieu"] = phieudkvattu.Contains("new_ngaylapphieu") ? phieudkvattu["new_ngaylapphieu"] : "";
                pgnvattu["new_phieudangkyvattu"] = new EntityReference(phieudkvattu.LogicalName, phieudkvattu.Id);
                pgnvattu["new_ngayduyet"] = phieudkvattu.Contains("new_ngayduyet") ? phieudkvattu["new_ngayduyet"] : "";
                pgnvattu["new_dinhmuc_khonghoanlai"] = phieudkvattu.Contains("new_denghi_khonghoanlai") ? phieudkvattu["new_denghi_khonghoanlai"] : new Money(0);
                pgnvattu["new_dinhmuc_hoanlai_vattu"] = phieudkvattu.Contains("new_denghi_hoanlai_vattu") ? phieudkvattu["new_denghi_hoanlai_vattu"] : new Money(0);
                pgnvattu["new_dinhmuc_hoanlai_tienmat"] = phieudkvattu.Contains("new_denghi_hoanlai_tienmat") ? phieudkvattu["new_denghi_hoanlai_tienmat"] : new Money(0);
                pgnvattu["new_laytupdk"] = true;

                Guid idPGNVT = service.Create(pgnvattu);

                List<Entity> DSchitietcu = RetrieveMultiRecord(service, "new_chitietgiaonhanvattu", new ColumnSet(true), "new_phieugiaonhanvattu", idPGNVT);
                foreach (Entity a in DSchitietcu)
                    service.Delete(a.LogicalName, a.Id);

                List<Entity> DSChitietdangkyVattu = RetrieveMultiRecord(service, "new_chitietdangkyvattu", new ColumnSet(true), "new_phieudangkyvattu", phieudkvattu.Id);

                Entity temp = service.Retrieve(pgnvattu.LogicalName, idPGNVT, new ColumnSet(new string[] { "new_masophieu" }));
                string maphieu = temp["new_masophieu"].ToString();
                decimal i = 0;

                foreach (Entity a in DSChitietdangkyVattu)
                {
                    i++;
                    Entity rs = new Entity("new_chitietgiaonhanvattu");
                    StringBuilder str = new StringBuilder();
                    str.Append("CTGNVATTU-" + mahopdong + "-L" + count);
                    str.Append("-CT" + i);

                    StringBuilder str1 = new StringBuilder();
                    str1.Append(maphieu + "_" + i.ToString());

                    rs["new_ma"] = str1.ToString();
                    rs["new_current"] = i;
                    rs["new_name"] = str.ToString();
                    rs["new_phieugiaonhanvattu"] = new EntityReference("new_phieugiaonhanvattu", idPGNVT);
                    rs["new_vattu"] = a.Attributes.Contains("new_vattu") ? a["new_vattu"] : null;
                    rs["new_dongia"] = a.Attributes.Contains("new_dongia") ? a["new_dongia"] : null;
                    rs["new_soluong"] = a.Attributes.Contains("new_soluong") ? a["new_soluong"] : null;
                    rs["new_sotienhl"] = a.Contains("new_sotienhl") ? a["new_sotienhl"] : new Money(0);
                    rs["new_sotienkhl"] = a.Contains("new_sotienkhl") ? a["new_sotienkhl"] : new Money(0);
                    rs["new_thanhtien1"] = new Money((a.Attributes.Contains("new_dongia") ? ((Money)a["new_dongia"]).Value : 0) * (a.Attributes.Contains("new_soluong") ? (decimal)a["new_soluong"] : 0));
                    service.Create(rs);
                }

                EntityCollection nnrecord = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieugiaonhanvattu",
                   "new_new_pgnvattu_new_chitiethddtmia", new ColumnSet(true), "new_phieugiaonhanvattuid", idPGNVT);
                EntityReferenceCollection entcRef = new EntityReferenceCollection();

                foreach (Entity a in nnrecord.Entities)
                {
                    entcRef.Add(a.ToEntityReference());
                }

                service.Disassociate("new_phieugiaonhanvattu", idPGNVT, new Relationship("new_new_pgnvattu_new_chitiethddtmia"), entcRef);

                List<Entity> lstThuadatcanhtac = RetrieveMultiRecord(service, "new_thuadatcanhtac",
                    new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_hopdongdautumia", hopdongdautumia.Id);

                EntityReferenceCollection entcRef1 = new EntityReferenceCollection();

                foreach (Entity a in lstThuadatcanhtac)
                {
                    entcRef1.Add(a.ToEntityReference());
                }

                //Entity t = service.Retrieve(phieudkvattu.LogicalName, phieudkvattu.Id,
                //    new ColumnSet(new string[] { "statuscode" }));
                //t["statuscode"] = new OptionSetValue(100000002);
                //service.Update(t);

                service.Associate("new_phieugiaonhanvattu", idPGNVT, new Relationship("new_new_pgnvattu_new_chitiethddtmia"), entcRef1);

                context.OutputParameters["ReturnId"] = idPGNVT.ToString();
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
    }
}
