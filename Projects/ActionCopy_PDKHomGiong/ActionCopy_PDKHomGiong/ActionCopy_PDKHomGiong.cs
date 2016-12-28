using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace ActionCopy_PDKHomGiong
{
    public class ActionCopy_PDKHomGiong : IPlugin
    {
        // moi nhat
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            EntityReference target = (EntityReference)context.InputParameters["Target"];

            if (target.LogicalName == "new_phieudangkyhomgiong")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity phieudkhomgiong = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                if (phieudkhomgiong == null)
                {
                    throw new Exception("Phiếu đăng ký hom giống này không tồn tại !!!");
                }

                Entity hopdongdautumia = service.Retrieve("new_hopdongdautumia", ((EntityReference)phieudkhomgiong["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_masohopdong" }));
                List<Entity> lstPGNHG = RetrieveMultiRecord(service, "new_phieugiaonhanphanbon", new ColumnSet(new string[] { "new_phieugiaonhanphanbonid" }), "new_hopdongdautumia", hopdongdautumia.Id);
                string mahopdong = hopdongdautumia.Contains("new_masohopdong") ? (string)hopdongdautumia["new_masohopdong"] : "";
                Entity pgnhomgiong = new Entity("new_phieugiaonhanhomgiong");
                string tenkhachhang = "";
                string p3 = "";
                int count = lstPGNHG.Count;

                pgnhomgiong["new_vudautu"] = phieudkhomgiong.Contains("new_vudautu") ? phieudkhomgiong["new_vudautu"] : "";
                pgnhomgiong["new_ngaylapphieu"] = phieudkhomgiong.Contains("new_ngaylapphieu") ? phieudkhomgiong["new_ngaylapphieu"] : "";

                if (phieudkhomgiong.Contains("new_khachhang"))
                {
                    pgnhomgiong["new_khachhang"] = phieudkhomgiong["new_khachhang"];

                    Entity kh = service.Retrieve("contact", ((EntityReference)phieudkhomgiong["new_khachhang"]).Id, new ColumnSet(new string[] { "new_socmnd" }));
                    tenkhachhang = ((EntityReference)phieudkhomgiong["new_khachhang"]).Name;
                    p3 = kh.Contains("new_socmnd") ? (string)kh["new_socmnd"] : "";
                }
                else if (phieudkhomgiong.Contains("new_khachhangdoanhnghiep"))
                {
                    pgnhomgiong["new_khachhangdoanhnghiep"] = phieudkhomgiong["new_khachhangdoanhnghiep"];

                    Entity khdn = service.Retrieve("account", ((EntityReference)phieudkhomgiong["new_khachhangdoanhnghiep"]).Id, new ColumnSet(new string[] { "new_sogpkd" }));
                    tenkhachhang = ((EntityReference)phieudkhomgiong["new_khachhangdoanhnghiep"]).Name;
                    p3 = khdn.Contains("new_sogpkd") ? (string)khdn["new_sogpkd"] : "";
                }

                pgnhomgiong["new_lannhan"] = count;
                pgnhomgiong["new_name"] = "PGNHG-" + tenkhachhang + "-" + p3 + "-" + mahopdong + "-L" + count;
                pgnhomgiong["new_hopdongdautumia"] = phieudkhomgiong["new_hopdongdautumia"];
                pgnhomgiong["new_tram"] = phieudkhomgiong.Contains("new_tram") ? phieudkhomgiong["new_tram"] : "";
                pgnhomgiong["new_canbonongvu"] = phieudkhomgiong.Contains("new_canbonongvu") ? phieudkhomgiong["new_canbonongvu"] : "";
                pgnhomgiong["new_phieudangkyhomgiong"] = new EntityReference(target.LogicalName, target.Id);

                pgnhomgiong["new_dinhmuc_khonghoanlai"] = phieudkhomgiong.Contains("new_denghi_khonghoanlai") ? phieudkhomgiong["new_denghi_khonghoanlai"] : new Money(0);
                pgnhomgiong["new_dinhmuc_hoanlai_vattu"] = phieudkhomgiong.Contains("new_denghi_hoanlai_vattu") ? phieudkhomgiong["new_denghi_hoanlai_vattu"] : new Money(0);
                pgnhomgiong["new_dinhmuc_hoanlai_tienmat"] = phieudkhomgiong.Contains("new_denghi_hoanlai_tienmat") ? phieudkhomgiong["new_denghi_hoanlai_tienmat"] : new Money(0);
                pgnhomgiong["new_laytupdk"] = true;
                pgnhomgiong["new_hopdongdautumia_doitac"] = phieudkhomgiong.Contains("new_hopdongdautumia_doitac") ? phieudkhomgiong["new_hopdongdautumia_doitac"] : "";
                pgnhomgiong["new_chitiethddtmia_doitac"] = phieudkhomgiong.Contains("new_chitiethddtmia_doitac") ? phieudkhomgiong["new_chitiethddtmia_doitac"] : "";
                pgnhomgiong["new_doitacgiaohom"] = phieudkhomgiong.Contains("new_doitacgiaogiong")
                    ? phieudkhomgiong["new_doitacgiaogiong"] : null;
                pgnhomgiong["new_doitacgiaohomkhdn"] = phieudkhomgiong.Contains("new_doitacgiaogiongkhdn")
                    ? phieudkhomgiong["new_doitacgiaogiongkhdn"]
                    : null;
                pgnhomgiong["new_loaigiaonhanhom"] = new OptionSetValue(100000001);

                Guid idPGNHG = service.Create(pgnhomgiong);

                List<Entity> DSchitietcu = RetrieveMultiRecord(service, "new_chitietgiaonhanhomgiong", new ColumnSet(true), "new_phieugiaonhanhomgiong", idPGNHG);
                foreach (Entity a in DSchitietcu)
                    service.Delete(a.LogicalName, a.Id);

                Entity temp = service.Retrieve(pgnhomgiong.LogicalName, idPGNHG, new ColumnSet(new string[] { "new_masophieu" }));
                string maphieu = temp["new_masophieu"].ToString();
                List<Entity> DSCtHomGiong = RetrieveMultiRecord(service, "new_chitietdangkyhomgiong", new ColumnSet(true), "new_phieudangkyhomgiong", phieudkhomgiong.Id);

                decimal i = 0;
                foreach (Entity a in DSCtHomGiong)
                {
                    i++;
                    Entity rs = new Entity("new_chitietgiaonhanhomgiong");
                    StringBuilder str = new StringBuilder();
                    str.Append("CTGNHG-" + mahopdong + "-L" + count);
                    str.Append("-CT" + i);

                    StringBuilder str1 = new StringBuilder();
                    str1.Append(maphieu + "_" + i.ToString());

                    rs["new_name"] = str.ToString();
                    rs["new_ma"] = str1.ToString();
                    rs["new_current"] = i;
                    rs["new_phieugiaonhanhomgiong"] = new EntityReference("new_phieugiaonhanhomgiong", idPGNHG);
                    rs["new_giongmia"] = a.Attributes.Contains("new_giongmia") ? a["new_giongmia"] : null;
                    rs["new_loaihom"] = a.Attributes.Contains("new_loaihom") ? a["new_loaihom"] : null;
                    rs["new_dongia"] = a.Attributes.Contains("new_dongia") ? a["new_dongia"] : new Money(0);
                    rs["new_soluong"] = a.Attributes.Contains("new_soluong") ? a["new_soluong"] : 0;
                    rs["new_ngaynhan"] = DateTime.Now;
                    rs["new_sotiendthoanlai"] = a.Contains("new_sotienhl") ? a["new_sotienhl"] : new Money(0);
                    rs["new_sotiendtkhonghoanlai"] = a.Contains("new_sotienkhl") ? a["new_sotienkhl"] : new Money(0);
                    rs["new_tongthanhtien"] = new Money((a.Attributes.Contains("new_dongia") ? ((Money)a["new_dongia"]).Value : 0) * (a.Attributes.Contains("new_soluong") ? (decimal)a["new_soluong"] : 0));

                    service.Create(rs);

                }

                EntityCollection nnrecord = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieugiaonhanhomgiong",
                    "new_new_pgnhomgiong_new_chitiethddtmia", new ColumnSet(true), "new_phieugiaonhanhomgiongid", idPGNHG);
                EntityReferenceCollection entcRef = new EntityReferenceCollection();

                foreach (Entity a in nnrecord.Entities)
                {
                    entcRef.Add(a.ToEntityReference());
                }

                service.Disassociate("new_phieugiaonhanhomgiong", idPGNHG, new Relationship("new_new_pgnhomgiong_new_chitiethddtmia"), entcRef);

                List<Entity> lstThuadatcanhtac = RetrieveMultiRecord(service, "new_thuadatcanhtac",
                    new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_hopdongdautumia", hopdongdautumia.Id);

                EntityReferenceCollection entcRef1 = new EntityReferenceCollection();

                foreach (Entity a in lstThuadatcanhtac)
                {
                    entcRef1.Add(a.ToEntityReference());
                }

                service.Associate("new_phieugiaonhanhomgiong", idPGNHG, new Relationship("new_new_pgnhomgiong_new_chitiethddtmia"), entcRef1);
                context.OutputParameters["ReturnId"] = idPGNHG.ToString();
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
