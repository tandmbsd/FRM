using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace PDNThanhToan_LoadDSTheoLoaiThanhToan
{
    public class PDNThanhToan_LoadDSTheoLoaiThanhToan : IPlugin
    {
        // moi nhat
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            Entity target = (Entity)context.InputParameters["Target"];
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity pdnthanhtoan = service.Retrieve(target.LogicalName, target.Id,
                new ColumnSet(new string[] { "new_hopdongcungcapdichvu", "new_hopdongdautuhatang",
                    "new_hopdongdautumia", "new_khachhang", "new_khachhangdoanhnghiep","statuscode","new_loaithanhtoan","new_tongtien" }));

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000)
            {
                Entity fullEntity = (Entity)context.PostEntityImages["PostImg"];
                Entity Vudautu = service.Retrieve("new_vudautu", ((EntityReference)fullEntity["new_vudautu"]).Id, new ColumnSet(new string[] { "new_mavudautu" }));
                int loaithanhtoan = ((OptionSetValue)pdnthanhtoan["new_loaithanhtoan"]).Value;

                Entity HDMia = service.Retrieve("new_hopdongdautumia", ((EntityReference)fullEntity["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_masohopdong" }));
                Entity KH = null;
                if (fullEntity.Contains("new_khachhang"))
                    KH = service.Retrieve("contact", ((EntityReference)fullEntity["new_khachhang"]).Id, new ColumnSet(new string[] { "new_makhachhang", "new_socmnd" }));
                else
                    KH = service.Retrieve("account", ((EntityReference)fullEntity["new_khachhangdoanhnghiep"]).Id, new ColumnSet(new string[] { "new_makhachhang", "new_masothue" }));

                List<Entity> lstChitietPTT = RetrieveMultiRecord(service, "new_chitietphieudenghithanhtoan",
                    new ColumnSet(true), "new_phieudenghithanhtoan", pdnthanhtoan.Id);

                decimal tongtien = ((Money)pdnthanhtoan["new_tongtien"]).Value;

                if (loaithanhtoan == 100000000) // hd cung cap dich vu
                {

                }
                else if (loaithanhtoan == 100000001) // hd dau tu ha tang
                {
                    // STA
                    Entity etl_STA = new Entity("new_etltransaction");
                    etl_STA["new_name"] = fullEntity["new_masophieu"].ToString() + "_STA";
                    etl_STA["new_vouchernumber"] = "DTND";
                    etl_STA["new_transactiontype"] = new OptionSetValue(3);
                    etl_STA["new_customertype"] = new OptionSetValue(fullEntity.Contains("new_khachhang") ? 1 : 2);
                    etl_STA["new_season"] = Vudautu["new_mavudautu"].ToString();
                    etl_STA["new_vudattu"] = fullEntity["new_vudautu"];
                    etl_STA["new_sochungtu"] = fullEntity["new_masophieu"].ToString();
                    etl_STA["new_lannhan"] = "";
                    etl_STA["new_contractnumber"] = HDMia["new_masohopdong"].ToString();
                    etl_STA["new_tradingpartner"] = (KH.LogicalName.ToLower().Trim() == "contact" ?
                        ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : ""))
                        :
                        ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""))
                        );
                    etl_STA["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                    etl_STA["new_suppliersite"] = "TAY NINH";
                    etl_STA["new_invoicedate"] = DateTime.Now;
                    etl_STA["new_descriptionheader"] = "Giao nhận hôm giống";
                    etl_STA["new_terms"] = "Tra Ngay";
                    etl_STA["new_taxtype"] = "";
                    etl_STA["new_invoiceamount"] = (Money)fullEntity["new_tongsotienkhl"];

                    service.Create(etl_STA);
                }
                else if (loaithanhtoan == 100000003) // hom giong mia cua nong dan
                {

                }
            }

            else if (pdnthanhtoan.Contains("statuscode") && ((OptionSetValue)pdnthanhtoan["statuscode"]).Value == 100000001)
            {
                if (target.Contains("new_loaithanhtoan") && ((OptionSetValue)target["new_loaithanhtoan"]).Value == 100000000)
                {
                    QueryExpression q = new QueryExpression("new_nghiemthudichvu");
                    q.ColumnSet = new ColumnSet(new string[] { "new_tongtien" });
                    q.Criteria = new FilterExpression();
                    q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000)); // da duyet
                    q.Criteria.AddCondition(new ConditionExpression("new_dathanhtoan", ConditionOperator.Equal, false)); // chua thanh toan                
                    q.Criteria.AddCondition(new ConditionExpression("new_hopdongcungungdichvu", ConditionOperator.Equal,
                        ((EntityReference)pdnthanhtoan["new_hopdongcungcapdichvu"]).Id));

                    EntityCollection entc = service.RetrieveMultiple(q);

                    if (entc.Entities.Count > 0)
                    {
                        DeleteDSCu(target, 100000000);

                        foreach (Entity en in entc.Entities)
                        {
                            Entity t = new Entity("new_chitietphieudenghithanhtoan");
                            t["new_phieudenghithanhtoan"] = new EntityReference(target.LogicalName, target.Id);
                            t["new_nghiemthudichvu"] = en.ToEntityReference();
                            t["new_tongtien"] = en.Contains("new_tongtien") ? en["new_tongtien"] : new Money(0);
                            t["new_loaithanhtoan"] = new OptionSetValue(100000000);

                            service.Create(t);
                        }
                    }
                }

                else if (target.Contains("new_loaithanhtoan") && ((OptionSetValue)target["new_loaithanhtoan"]).Value == 100000001)
                {
                    traceService.Trace("b");
                    QueryExpression q = new QueryExpression("new_nghiemthucongtrinh");
                    q.ColumnSet = new ColumnSet(new string[] { "new_tongtien" });
                    q.Criteria = new FilterExpression();
                    q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));
                    q.Criteria.AddCondition(new ConditionExpression("new_dathanhtoan", ConditionOperator.Equal, false));
                    q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautuhatang", ConditionOperator.Equal, ((EntityReference)pdnthanhtoan["new_hopdongdautuhatang"]).Id));
                    EntityCollection entc = service.RetrieveMultiple(q);

                    if (entc.Entities.Count > 0)
                    {
                        DeleteDSCu(target, 100000001);
                        foreach (Entity en in entc.Entities)
                        {
                            Entity t = new Entity("new_chitietphieudenghithanhtoan");
                            t["new_phieudenghithanhtoan"] = new EntityReference(target.LogicalName, target.Id);
                            t["new_nghiemthucongtrinh"] = en.ToEntityReference();
                            t["new_tongtien"] = en.Contains("new_tongtien") ? en["new_tongtien"] : new Money(0);
                            t["new_loaithanhtoan"] = new OptionSetValue(100000001);

                            service.Create(t);
                        }
                    }
                }

                else if (target.Contains("new_loaithanhtoan") && ((OptionSetValue)target["new_loaithanhtoan"]).Value == 100000002) //pdn tam ung
                {
                    traceService.Trace("c");
                    Entity pdntamung = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                    QueryExpression q = new QueryExpression("new_phieutamung");
                    q.ColumnSet = new ColumnSet(new string[] { "new_sotienung" });
                    q.Criteria = new FilterExpression();
                    q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));

                    if (pdntamung.Contains("new_khachhang"))
                    {
                        q.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, ((EntityReference)pdnthanhtoan["new_khachhang"]).Id));
                    }
                    else if (pdntamung.Contains("new_khachhangdoanhnghiep"))
                    {
                        q.Criteria.AddCondition(new ConditionExpression("new_khachhangdoanhnghiep", ConditionOperator.Equal, ((EntityReference)pdnthanhtoan["new_khachhangdoanhnghiep"]).Id));
                    }

                    EntityCollection entc = service.RetrieveMultiple(q);

                    if (entc.Entities.Count > 0)
                    {
                        DeleteDSCu(target, 100000002);
                        foreach (Entity en in entc.Entities)
                        {
                            Entity t = new Entity("new_chitietphieudenghithanhtoan");
                            t["new_phieudenghithanhtoan"] = new EntityReference(target.LogicalName, target.Id);
                            t["new_phieudenghitamung"] = en.ToEntityReference();
                            t["new_tongtien"] = en.Contains("new_sotienung") ? en["new_sotienung"] : new Money(0);
                            t["new_loaithanhtoan"] = new OptionSetValue(100000002);

                            service.Create(t);
                        }
                    }
                }

                else if (target.Contains("new_loaithanhtoan") && ((OptionSetValue)target["new_loaithanhtoan"]).Value == 100000003)
                {
                    traceService.Trace("d");

                    QueryExpression q = new QueryExpression("new_phieugiaonhanhomgiong");
                    q.ColumnSet = new ColumnSet(new string[] { "new_tongtien", "new_denghi_khonghoanlai" });
                    q.Criteria = new FilterExpression();
                    q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000)); // da duyet
                    q.Criteria.AddCondition(new ConditionExpression("new_dathanhtoan", ConditionOperator.Equal, false)); // chua thanh toan
                    q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, ((EntityReference)target["new_hopdongdautumia"]).Id));
                    q.Criteria.AddCondition(new ConditionExpression("new_loaigiaonhanhom", ConditionOperator.Equal, 100000001)); // nong dan - nong dan

                    if (pdnthanhtoan.Contains("new_khachhang"))
                    {
                        q.Criteria.AddCondition(new ConditionExpression("new_doitacgiaohom", ConditionOperator.Equal, ((EntityReference)pdnthanhtoan["new_khachhang"]).Id));
                    }
                    else if (pdnthanhtoan.Contains("new_khachhangdoanhnghiep"))
                    {
                        q.Criteria.AddCondition(new ConditionExpression("new_doitacgiaohomkhdn", ConditionOperator.Equal, ((EntityReference)pdnthanhtoan["new_khachhangdoanhnghiep"]).Id));
                    }

                    EntityCollection entc = service.RetrieveMultiple(q);

                    if (entc.Entities.Count > 0)
                    {
                        DeleteDSCu(target, 100000003);
                        foreach (Entity en in entc.Entities)
                        {
                            Entity t = new Entity("new_chitietphieudenghithanhtoan");
                            t["new_phieudenghithanhtoan"] = new EntityReference(target.LogicalName, target.Id);
                            t["new_phieugiaonhanhomgiong"] = en.ToEntityReference();
                            t["new_tongtien"] = en.Contains("new_tongtien") ? en["new_tongtien"] : new Money(0);
                            t["new_hotro"] = en.Contains("new_denghi_khonghoanlai") ? en["new_denghi_khonghoanlai"] : new Money(0);
                            t["new_loaithanhtoan"] = new OptionSetValue(100000003);

                            service.Create(t);
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

        void DeleteDSCu(Entity target, int loaithanhtoan)
        {
            List<Entity> chitietpdnthanhtoancu = RetrieveMultiRecord(service, "new_chitietphieudenghithanhtoan", new ColumnSet(new string[] { "new_phieudenghithanhtoan" }),
                "new_phieudenghithanhtoan", target.Id);

            QueryExpression q = new QueryExpression("new_chitietphieudenghithanhtoan");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_phieudenghithanhtoan", ConditionOperator.Equal, target.Id));

            if (loaithanhtoan == 100000000) //hd cung cap dich vu
            {
                q.Criteria.AddCondition(new ConditionExpression("new_loaithanhtoan", ConditionOperator.Equal, loaithanhtoan));
            }
            else if (loaithanhtoan == 100000001)//hd dau tu ha tang
            {
                q.Criteria.AddCondition(new ConditionExpression("new_loaithanhtoan", ConditionOperator.Equal, loaithanhtoan));
            }
            else if (loaithanhtoan == 100000002)//pdn tam ung
            {
                q.Criteria.AddCondition(new ConditionExpression("new_loaithanhtoan", ConditionOperator.Equal, loaithanhtoan));
            }
            else if (loaithanhtoan == 100000003)//hom giong mua cua nong dan
            {
                q.Criteria.AddCondition(new ConditionExpression("new_loaithanhtoan", ConditionOperator.Equal, loaithanhtoan));
            }

            EntityCollection entc = service.RetrieveMultiple(q);
            foreach (Entity en in entc.Entities)
            {
                service.Delete(en.LogicalName, en.Id);

            }
        }
    }
}
