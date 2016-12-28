using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace PbanBo_PhieuGiaoNhanHomGiong
{
    public class PbanBo_PhieuGiaoNhanHomGiong : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000) // da duyet
            {
                Entity phieugiaonhan = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_hopdongdautumia", "new_khachhang",
                        "new_khachhangdoanhnghiep", "new_vudautu","new_phieudangkyhomgiong" }));

                List<Entity> lstChitietPGNHM = RetrieveMultiRecord(service, "new_chitietgiaonhanhomgiong",
                    new ColumnSet(new string[] { "new_sotiendtkhonghoanlai", "new_sotiendtkhonghoanlai" }),
                    "new_phieugiaonhanhomgiong", target.Id);

                EntityCollection entcChitiet = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieugiaonhanhomgiong",
                    "new_new_pgnhomgiong_new_chitiethddtmia",
                    new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_phieugiaonhanhomgiongid", target.Id);

                if (!phieugiaonhan.Contains("new_phieudangkyhomgiong"))
                {
                    throw new Exception("Phiếu giao nhận hôm giống không có phiếu đăng ký");
                }

                List<Entity> lstThoai = RetrieveMultiRecord(service, "new_thuadat_pdkhomgiong",
                    new ColumnSet(true), "new_phieudangky", ((EntityReference)phieugiaonhan["new_phieudangkyhomgiong"]).Id);

                if (entcChitiet.Entities.Count == 0)
                {
                    return;
                }

                Entity KH = null;

                if (!phieugiaonhan.Contains("new_hopdongdautumia"))
                {
                    throw new Exception("Phiếu giao nhận không có hợp đồng đầu tư mía");
                }

                Entity hddtmia = service.Retrieve("new_hopdongdautumia",
                    ((EntityReference)phieugiaonhan["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_masohopdong" }));

                if (phieugiaonhan.Contains("new_khachhang"))
                {
                    KH = service.Retrieve("contact", ((EntityReference)phieugiaonhan["new_khachhang"]).Id, new ColumnSet(new string[] { "fullname" }));
                }
                else if (phieugiaonhan.Contains("new_khachhangdoanhnghiep"))
                {
                    KH = service.Retrieve("account", ((EntityReference)phieugiaonhan["new_khachhangdoanhnghiep"]).Id, new ColumnSet(new string[] { "name" }));
                }

                if (KH == null)
                {
                    throw new Exception("Phiếu giao nhận hôm giống không có khách hàng");
                }

                Dictionary<Guid, DinhMuc> dtDinhMuc = new Dictionary<Guid, DinhMuc>();
                foreach (Entity en in lstThoai)
                {
                    Entity chitiet = service.Retrieve("new_thuadatcanhtac",
                                ((EntityReference)en["new_chitiethopdong"]).Id, new ColumnSet(new string[] { "new_thuadatcanhtacid" }));
                    decimal hl = (en.Contains("new_dmhlvt") ? ((Money)en["new_dmhlvt"]).Value : new decimal(0)) + (en.Contains("new_dmhltm") ? ((Money)en["new_dmhltm"]).Value : new decimal(0));
                    decimal khl = en.Contains("new_dm0hl") ? ((Money)en["new_dm0hl"]).Value : new decimal(0);

                    if (!dtDinhMuc.ContainsKey(chitiet.Id))
                        dtDinhMuc.Add(chitiet.Id, new DinhMuc(hl, khl));
                    else
                        dtDinhMuc[chitiet.Id] = new DinhMuc(hl, khl);
                }

                Dictionary<Guid, List<Tylethuhoivon>> dtTyleThuhoi = new Dictionary<Guid, List<Tylethuhoivon>>();

                foreach (Entity chitiet in entcChitiet.Entities) // vong lap cac chi tiet hddt mía
                {
                    List<Entity> lstTylethuhoi = RetrieveMultiRecord(service, "new_tylethuhoivondukien",
                        new ColumnSet(true), "new_chitiethddtmia", chitiet.Id);

                    foreach (Entity tylethuhoivon in lstTylethuhoi)
                    {
                        decimal tiendaphanbo = tylethuhoivon.Contains("new_tiendaphanbo") ? ((Money)tylethuhoivon["new_tiendaphanbo"]).Value : new decimal(0);
                        Tylethuhoivon item = new Tylethuhoivon();
                        item.daphanbo = tiendaphanbo;
                        item.vuthuhoi = (EntityReference)tylethuhoivon["new_vudautu"];
                        item.sotien = tylethuhoivon.Contains("new_sotienthuhoi") ? ((Money)tylethuhoivon["new_sotienthuhoi"]).Value : new decimal(0);
                        item.tylethuhoiid = tylethuhoivon.Id;

                        if (!dtTyleThuhoi.ContainsKey(chitiet.Id))
                            dtTyleThuhoi.Add(chitiet.Id, new List<Tylethuhoivon>());

                        dtTyleThuhoi[chitiet.Id].Add(item);
                    }
                }

                foreach (Entity giaingan in lstChitietPGNHM) // vong lap giai ngan
                {
                    decimal phanbokhonghoanlai = giaingan.Contains("new_sotiendtkhonghoanlai") ? ((Money)giaingan["new_sotiendtkhonghoanlai"]).Value : new decimal(0);
                    decimal phanbohoanlai = giaingan.Contains("new_sotiendthoanlai") ? ((Money)giaingan["new_sotiendthoanlai"]).Value : new decimal(0);

                    Entity vudautu = service.Retrieve("new_vudautu",
                            ((EntityReference)phieugiaonhan["new_vudautu"]).Id, new ColumnSet(new string[] { "new_vudautuid" }));

                    foreach (Guid key in dtDinhMuc.Keys)
                    {
                        DinhMuc a = dtDinhMuc[key];
                        decimal sotien = phanbokhonghoanlai * a.dinhMucKHL / DinhMuc.tongdinhmucKHL;

                        CreatePBDT(hddtmia, KH, key, vudautu.ToEntityReference(), sotien);
                    }

                    Dictionary<Guid, decimal> dtTungthua = new Dictionary<Guid, decimal>();

                    foreach (Guid key in dtDinhMuc.Keys)
                    {
                        DinhMuc a = dtDinhMuc[key];
                        decimal sotien = phanbohoanlai * a.dinhMucHL / DinhMuc.tongdinhmucHL;

                        if (!dtTungthua.ContainsKey(key))
                        {
                            dtTungthua.Add(key, sotien);
                        }
                    }

                    foreach (Guid key in dtTungthua.Keys)
                    {
                        List<Tylethuhoivon> lstTylethuhoivon = dtTyleThuhoi[key];
                        decimal dinhmuc = dtTungthua[key];

                        foreach (Tylethuhoivon a in lstTylethuhoivon)
                        {
                            Entity tilethuhoivon = service.Retrieve("new_tylethuhoivondukien", a.tylethuhoiid,
                                    new ColumnSet(new string[] { "new_sotienthuhoi", "new_tiendaphanbo" }));

                            if (dinhmuc < a.sotien - a.daphanbo)
                            {
                                CreatePBDT(hddtmia, KH, key, a.vuthuhoi, dinhmuc);
                                tilethuhoivon["new_tiendaphanbo"] = new Money(((Money)tilethuhoivon["new_tiendaphanbo"]).Value + dinhmuc);
                                service.Update(tilethuhoivon);
                                break;
                            }
                            else if (dinhmuc > a.sotien - a.daphanbo)
                            {
                                CreatePBDT(hddtmia, KH, key, a.vuthuhoi, a.sotien - a.daphanbo);
                                tilethuhoivon["new_tiendaphanbo"] = new Money(((Money)tilethuhoivon["new_tiendaphanbo"]).Value + (a.sotien - a.daphanbo));
                                dinhmuc = dinhmuc - (a.sotien + a.daphanbo);
                                service.Update(tilethuhoivon);

                            }
                        }
                    }
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

        public void CreatePBDT(Entity hddtmia, Entity KH, Guid tdct, EntityReference vudautu, decimal sotien)
        {
            if (sotien > 0)
            {
                StringBuilder Name = new StringBuilder();
                Name.Append("PBDT");

                if (hddtmia.Contains("new_masohopdong"))
                {
                    Name.Append("-" + hddtmia["new_masohopdong"].ToString());
                }

                if (KH.Contains("fullname"))
                {
                    Name.Append("-" + KH["fullname"]);
                }
                else if (KH.Contains("name"))
                {
                    Name.Append("-" + KH["name"]);
                }

                #region phan bo KHL
                Entity phanbodautuKHL = new Entity("new_phanbodautu");
                //phanbodautu["new_etltransaction"] =
                phanbodautuKHL["new_name"] = Name.ToString();

                if (KH.LogicalName == "contact")
                    phanbodautuKHL["new_khachhang"] = KH.ToEntityReference();
                else if (KH.LogicalName == "account")
                    phanbodautuKHL["new_khachhangdoanhnghiep"] = KH.ToEntityReference();

                phanbodautuKHL["new_loaihopdong"] = new OptionSetValue(100000000);
                phanbodautuKHL["new_hopdongdautumia"] = hddtmia.ToEntityReference();
                phanbodautuKHL["new_thuacanhtac"] = new EntityReference("new_thuadatcanhtac", tdct);
                phanbodautuKHL["new_vudautu"] = vudautu;
                phanbodautuKHL["new_sotien"] = new Money(sotien);
                service.Create(phanbodautuKHL);
                #endregion
            }
        }
    }
}
