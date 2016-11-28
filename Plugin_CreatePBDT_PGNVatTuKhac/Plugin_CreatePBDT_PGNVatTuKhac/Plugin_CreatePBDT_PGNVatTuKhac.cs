using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Messaging;
using PbanBo_PhieuGiaoNhanHomGiong;


namespace Plugin_CreatePBDT_PGNVatTuKhac
{
    public class Plugin_CreatePBDT_PGNVatTuKhac : IPlugin
    {
        private IOrganizationService service = null;
        private IOrganizationServiceFactory factory = null;
        public ITracingService trace;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000) // da duyệt
            {
                Entity fullEntity = (Entity)context.PostEntityImages["PostImg"];
                Entity Vudautu = service.Retrieve("new_vudautu", ((EntityReference)fullEntity["new_vudautu"]).Id, new ColumnSet(new string[] { "new_mavudautu" }));
                Entity HDMia = service.Retrieve("new_hopdongdautumia", ((EntityReference)fullEntity["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_masohopdong" }));
                Entity KH = null;
                if (fullEntity.Contains("new_khachhang"))
                    KH = service.Retrieve("contact", ((EntityReference)fullEntity["new_khachhang"]).Id, new ColumnSet(new string[] { "new_makhachhang", "new_socmnd", "new_phuongthucthanhtoan" }));
                else
                    KH = service.Retrieve("account", ((EntityReference)fullEntity["new_khachhangdoanhnghiep"]).Id,
                        new ColumnSet(new string[] { "new_makhachhang", "new_masothue", "new_phuongthucthanhtoan" }));

                if (!fullEntity.Contains("new_lannhan"))
                    throw new Exception("Phiếu giao nhận không có lần nhận");

                if (!fullEntity.Contains("new_ngaynhanvattu"))
                    throw new Exception("Phiếu giao nhận không có ngày nhận");

                if (!fullEntity.Contains("new_masophieu"))
                    throw new Exception("Phiếu giao nhận không có mã số phiếu");

                if (!fullEntity.Contains("new_ngayduyet"))
                    throw new Exception("Phiếu giao nhận không có ngày duyệt");

                QueryExpression q1 = new QueryExpression("new_chitietgiaonhanvattu");
                q1.ColumnSet = new ColumnSet(true);
                q1.Criteria.AddCondition(new ConditionExpression("new_phieugiaonhanvattu", ConditionOperator.Equal, target.Id));
                q1.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
                EntityCollection dsChiTietGN = service.RetrieveMultiple(q1);

                //Ghi nợ hoàn lại.
                if (fullEntity.Contains("new_tongsotienhl") && ((Money)fullEntity["new_tongsotienhl"]).Value > 0)
                {
                    //gen ETL transaction
                    #region begin

                    //Tạo Credit Nông dân
                    Entity etl_ND = new Entity("new_etltransaction");
                    etl_ND["new_name"] = fullEntity["new_masophieu"].ToString() + "_CRE";
                    etl_ND["new_vouchernumber"] = "DTND";
                    etl_ND["new_transactiontype"] = "3.2.2.a";
                    etl_ND["new_customertype"] = new OptionSetValue(fullEntity.Contains("new_khachhang") ? 1 : 2);
                    etl_ND["new_season"] = Vudautu["new_mavudautu"].ToString();
                    //etl_ND["new_vudautu"] = fullEntity["new_vudautu"];
                    etl_ND["new_sochungtu"] = fullEntity["new_masophieu"].ToString();
                    etl_ND["new_lannhan"] = fullEntity["new_lannhan"];
                    etl_ND["new_contractnumber"] = HDMia["new_masohopdong"].ToString();
                    etl_ND["new_tradingpartner"] = (KH.LogicalName.ToLower().Trim() == "contact" ?
                        ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : ""))
                        :
                        ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""))
                        );
                    etl_ND["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                    etl_ND["new_suppliersite"] = "TAY NINH";
                    etl_ND["new_invoicedate"] = fullEntity["new_ngaynhanvattu"];
                    etl_ND["new_descriptionheader"] = "Ghi nợ nhận vật tư";
                    etl_ND["new_terms"] = "Tra Ngay";
                    etl_ND["new_taxtype"] = "";
                    etl_ND["new_invoiceamount"] = new Money(((Money)fullEntity["new_tongsotienhl"]).Value * (-1));
                    etl_ND["new_gldate"] = fullEntity["new_ngayduyet"];
                    etl_ND["new_invoicetype"] = "CRE";
                    etl_ND["new_paymenttype"] = "TM";

                    if (fullEntity.Contains("new_khachhang"))
                        etl_ND["new_khachhang"] = fullEntity["new_khachhang"];
                    else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                        etl_ND["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                    Guid etl_NDID = service.Create(etl_ND);
                    Send(etl_ND);
                    //gen phân bổ đầu tư
                    #region begin
                    GenPhanBoDauTu(target, etl_NDID);
                    #endregion

                    //Tạo tk 154 cho nông trường.

                    //Entity etl_NT = new Entity("new_etltransaction");
                    //etl_NT["new_name"] = fullEntity["new_masophieu"].ToString() + "_TK154";
                    //etl_NT["new_vouchernumber"] = "DTND";
                    ////etl_NT["new_transactiontype"] = new OptionSetValue(3);
                    //etl_NT["new_customertype"] = new OptionSetValue(7);
                    //etl_NT["new_season"] = Vudautu["new_mavudautu"].ToString();
                    //etl_NT["new_vudautu"] = fullEntity["new_vudautu"];
                    //etl_NT["new_sochungtu"] = fullEntity["new_masophieu"].ToString();
                    //etl_NT["new_lannhan"] = fullEntity["new_lannhan"];
                    //etl_NT["new_contractnumber"] = HDMia["new_masohopdong"].ToString();
                    //if (NongTruong != null)
                    //    etl_NT["new_tradingpartner"] = ((NongTruong.Contains("new_makhachhang") ? NongTruong["new_makhachhang"].ToString() : "") + "_" + (NongTruong.Contains("new_masothue") ? NongTruong["new_masothue"].ToString() : ""));
                    //else
                    //    etl_NT["new_tradingpartner"] = "";

                    //etl_NT["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                    //etl_NT["new_suppliersite"] = "TAY NINH";
                    //etl_NT["new_invoicedate"] = fullEntity["new_ngaynhan"];
                    //etl_NT["new_descriptionheader"] = "Hạch toán 154 hom nông trường giao nông dân";
                    //etl_NT["new_terms"] = "Tra Ngay";
                    //etl_NT["new_taxtype"] = "";
                    //etl_NT["new_invoiceamount"] = (Money)fullEntity["new_tongsotienhl"];

                    //service.Create(etl_NT);
                    //Send(etl_NT);
                    #endregion
                }

                if (fullEntity.Contains("new_tongsotienkhl") && ((Money)fullEntity["new_tongsotienkhl"]).Value > 0)
                {
                    #region begin
                    //tạo credit
                    Entity etl_ND = new Entity("new_etltransaction");
                    etl_ND["new_name"] = fullEntity["new_masophieu"].ToString() + "_CRE" + "_KHL";
                    etl_ND["new_vouchernumber"] = "DTND";
                    etl_ND["new_transactiontype"] = "3.2.2.a";
                    etl_ND["new_customertype"] = new OptionSetValue(fullEntity.Contains("new_khachhang") ? 1 : 2);
                    etl_ND["new_season"] = Vudautu["new_mavudautu"].ToString();
                    //etl_ND["new_vudautu"] = fullEntity["new_vudautu"];
                    etl_ND["new_sochungtu"] = fullEntity["new_masophieu"].ToString();
                    etl_ND["new_lannhan"] = fullEntity["new_lannhan"];
                    etl_ND["new_contractnumber"] = HDMia["new_masohopdong"].ToString();
                    etl_ND["new_tradingpartner"] = (KH.LogicalName.ToLower().Trim() == "contact" ?
                        ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : ""))
                        :
                        ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""))
                        );
                    etl_ND["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                    etl_ND["new_suppliersite"] = "TAY NINH";
                    etl_ND["new_invoicedate"] = fullEntity["new_ngaynhanvattu"];
                    etl_ND["new_descriptionheader"] = "Ghi nợ nhận vật tư";
                    etl_ND["new_terms"] = "Tra Ngay";
                    etl_ND["new_taxtype"] = "";
                    etl_ND["new_invoiceamount"] = new Money(((Money)fullEntity["new_tongsotienkhl"]).Value * (-1));
                    etl_ND["new_gldate"] = fullEntity["new_ngayduyet"];
                    etl_ND["new_invoicetype"] = "CRE";
                    etl_ND["new_paymenttype"] = "TM";

                    if (fullEntity.Contains("new_khachhang"))
                        etl_ND["new_khachhang"] = fullEntity["new_khachhang"];
                    else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                        etl_ND["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                    Guid etl_NDID = service.Create(etl_ND);
                    Send(etl_ND);

                    ////gen phân bổ đầu tư
                    //#region begin
                    ////GenPhanBoDauTu(target, etl_NDID);
                    //#endregion
                    ////Tạo tk 154 cho nông trường.

                    //Entity etl_NT = new Entity("new_etltransaction");
                    //etl_NT["new_name"] = fullEntity["new_masophieu"].ToString() + "_KHL_TK154";
                    //etl_ND["new_vouchernumber"] = "DTND";
                    //etl_NT["new_transactiontype"] = new OptionSetValue(3);
                    //etl_NT["new_customertype"] = new OptionSetValue(7);
                    //etl_NT["new_season"] = Vudautu["new_mavudautu"].ToString();
                    //etl_NT["new_vudautu"] = fullEntity["new_vudautu"];
                    //etl_NT["new_sochungtu"] = fullEntity["new_masophieu"].ToString();
                    ////etl_NT["new_lannhan"] = fullEntity["new_lannhan"];
                    //etl_NT["new_contractnumber"] = HDMia["new_masohopdong"].ToString();
                    //if (NongTruong != null)
                    //    etl_NT["new_tradingpartner"] = ((NongTruong.Contains("new_makhachhang") ? NongTruong["new_makhachhang"].ToString() : "") + "_" + (NongTruong.Contains("new_masothue") ? NongTruong["new_masothue"].ToString() : ""));
                    //else
                    //    etl_NT["new_tradingpartner"] = "";

                    //etl_NT["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                    //etl_NT["new_suppliersite"] = "TAY NINH";
                    //etl_NT["new_invoicedate"] = fullEntity["new_ngaynhan"];
                    //etl_NT["new_descriptionheader"] = "Hạch toán 154 hom nông trường giao nông dân";
                    //etl_NT["new_terms"] = "Tra Ngay";
                    //etl_NT["new_taxtype"] = "";
                    //etl_NT["new_invoiceamount"] = (Money)fullEntity["new_tongsotienhl"];

                    //service.Create(etl_NT);

                    // STA
                    Entity etl_STA = new Entity("new_etltransaction");
                    etl_STA["new_name"] = fullEntity["new_masophieu"].ToString() + "_STA";
                    etl_STA["new_vouchernumber"] = "GSND";
                    etl_STA["new_transactiontype"] = "3.1.3.a";
                    etl_STA["new_customertype"] = new OptionSetValue(fullEntity.Contains("new_khachhang") ? 1 : 2);
                    etl_STA["new_season"] = Vudautu["new_mavudautu"].ToString();
                    //etl_STA["new_vudautu"] = fullEntity["new_vudautu"];
                    etl_STA["new_sochungtu"] = fullEntity["new_masophieu"].ToString();
                    etl_STA["new_lannhan"] = fullEntity["new_lannhan"];
                    etl_STA["new_contractnumber"] = HDMia["new_masohopdong"].ToString();
                    etl_STA["new_tradingpartner"] = (KH.LogicalName.ToLower().Trim() == "contact" ?
                        ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : ""))
                        :
                        ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""))
                        );

                    etl_STA["new_suppliersite"] = "TAY NINH";
                    etl_STA["new_invoicedate"] = fullEntity["new_ngaynhanvattu"];
                    etl_STA["new_descriptionheader"] = "Giao nhận vật tư";
                    etl_STA["new_terms"] = "Tra Ngay";
                    etl_STA["new_taxtype"] = "";
                    etl_STA["new_invoiceamount"] = (Money)fullEntity["new_tongsotienkhl"];
                    etl_STA["new_gldate"] = fullEntity["new_ngayduyet"];
                    etl_STA["new_invoicetype"] = "STA";
                    etl_STA["new_paymenttype"] = "TM";

                    if (fullEntity.Contains("new_khachhang"))
                        etl_STA["new_khachhang"] = fullEntity["new_khachhang"];
                    else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                        etl_STA["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                    service.Create(etl_STA);
                    Send(etl_STA);
                    //Pay cấn trừ
                    #region Tạo transaction apply CRE
                    Entity apply_PGNhomgiong_CRE = new Entity("new_applytransaction");
                    //apply_PGNPhanbon["new_documentsequence"] = value++;
                    apply_PGNhomgiong_CRE["new_suppliersitecode"] = "Tây Ninh";

                    if (KH.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)KH["new_phuongthucthanhtoan"]).Value == 100000001)
                    {
                        List<Entity> taikhoannganhang = RetrieveMultiRecord(service, "new_taikhoannganhang",
                            new ColumnSet(new string[] { "new_sotaikhoan", "new_giaodichchinh" }),
                            KH.LogicalName == "contact" ? "new_khachhang" : "new_khachhangdoanhnghiep", KH.Id);

                        Entity taikhoanchinh = null;

                        foreach (Entity en in taikhoannganhang)
                        {
                            if ((bool)en["new_giaodichchinh"] == true)
                                taikhoanchinh = en;
                        }

                        apply_PGNhomgiong_CRE["new_supplierbankname"] = (taikhoanchinh == null ? "CTXL-VND-0" : taikhoanchinh["new_sotaikhoan"]);
                    }
                    else
                        apply_PGNhomgiong_CRE["new_supplierbankname"] = "CTXL-VND-0";

                    //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                    apply_PGNhomgiong_CRE["new_paymentamount"] = new Money(((Money)fullEntity["new_tongsotienkhl"]).Value * (-1));
                    //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                    apply_PGNhomgiong_CRE["new_paymentdate"] = fullEntity["new_ngayduyet"];
                    apply_PGNhomgiong_CRE["new_paymentdocumentname"] = "CANTRU_03";
                    apply_PGNhomgiong_CRE["new_vouchernumber"] = "CTND";
                    apply_PGNhomgiong_CRE["new_cashflow"] = "00.00";
                    apply_PGNhomgiong_CRE["new_paymentnum"] = 1;
                    apply_PGNhomgiong_CRE["new_documentnum"] = fullEntity["new_masophieu"].ToString();
                    apply_PGNhomgiong_CRE["new_documentsequence"] = fullEntity["new_lannhan"];

                    if (fullEntity.Contains("new_khachhang"))
                        apply_PGNhomgiong_CRE["new_khachhang"] = fullEntity["new_khachhang"];
                    else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                        apply_PGNhomgiong_CRE["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                    service.Create(apply_PGNhomgiong_CRE);
                    #endregion
                    Send(apply_PGNhomgiong_CRE);

                    #region Tạo transaction apply STA
                    Entity apply_PGNhomgiong_STA = new Entity("new_applytransaction");
                    //apply_PGNPhanbon["new_documentsequence"] = value++;
                    apply_PGNhomgiong_STA["new_suppliersitecode"] = "Tây Ninh";

                    if (KH.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)KH["new_phuongthucthanhtoan"]).Value == 100000001)
                    {
                        List<Entity> taikhoannganhang = RetrieveMultiRecord(service, "new_taikhoannganhang",
                            new ColumnSet(new string[] { "new_sotaikhoan", "new_giaodichchinh" }),
                            KH.LogicalName == "contact" ? "new_khachhang" : "new_khachhangdoanhnghiep", KH.Id);

                        Entity taikhoanchinh = null;

                        foreach (Entity en in taikhoannganhang)
                        {
                            if ((bool)en["new_giaodichchinh"] == true)
                                taikhoanchinh = en;
                        }

                        apply_PGNhomgiong_STA["new_supplierbankname"] = (taikhoanchinh == null ? "CTXL-VND-0" : taikhoanchinh["new_sotaikhoan"]);
                    }
                    else
                        apply_PGNhomgiong_STA["new_supplierbankname"] = "CTXL-VND-0";

                    
                    apply_PGNhomgiong_STA["new_paymentamount"] = fullEntity["new_tongsotienkhl"];                    
                    apply_PGNhomgiong_STA["new_paymentdate"] = fullEntity["new_ngayduyet"];
                    apply_PGNhomgiong_STA["new_paymentdocumentname"] = "CANTRU_03";
                    apply_PGNhomgiong_STA["new_vouchernumber"] = "CTND";
                    apply_PGNhomgiong_STA["new_cashflow"] = "00.00";
                    apply_PGNhomgiong_STA["new_paymentnum"] = 1;
                    apply_PGNhomgiong_STA["new_documentnum"] = fullEntity["new_masophieu"].ToString();

                    if (fullEntity.Contains("new_khachhang"))
                        apply_PGNhomgiong_STA["new_khachhang"] = fullEntity["new_khachhang"];
                    else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                        apply_PGNhomgiong_STA["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                    service.Create(apply_PGNhomgiong_STA);
                    Send(apply_PGNhomgiong_STA);
                    #endregion

                    #endregion
                }
            }
        }

        public decimal GetSequenceNumber()
        {
            try
            {
                QueryExpression qe = new QueryExpression("new_sequencenumber");
                qe.ColumnSet = new ColumnSet(new string[] { "new_value" });
                qe.Criteria.AddCondition(new ConditionExpression("new_name", ConditionOperator.Equal, "new_applytransaction"));
                qe.TopCount = 1;

                EntityCollection result = service.RetrieveMultiple(qe);
                if (result.Entities.Count > 0)
                {
                    decimal value = result[0].Contains("new_value") ? (decimal)result[0]["new_value"] : 0;
                    Entity a = new Entity("new_sequencenumber");
                    a.Id = result[0].Id;
                    a["new_value"] = value + 1;
                    service.Update(a);
                    return (value + 1);
                }
                else
                {
                    Entity a = new Entity("new_sequencenumber");
                    a["new_name"] = "new_applytransaction";
                    a["new_value"] = 1;
                    service.Create(a);
                    return 1;
                }
            }
            catch
            {
                return -1;
            }
        }

        private EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
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

        public void CreatePBDT(Entity hddtmia, Entity KH, Guid tdct, EntityReference vudautu,
            decimal sotien, Guid etlID, int type, Entity tram, Entity cbnv, DateTime ngaylapphieu,Entity giaingan)
        {
            bool colai = false;
            Entity thuadatcanhtac = service.Retrieve("new_thuadatcanhtac", tdct,
                new ColumnSet(new string[] { "new_laisuat", "new_name", "new_loailaisuat" }));

            if (!thuadatcanhtac.Contains("new_laisuat"))
                throw new Exception(thuadatcanhtac["new_name"].ToString() + " không có lãi suất");

            if (((OptionSetValue)thuadatcanhtac["new_loailaisuat"]).Value == 100000000 && !thuadatcanhtac.Contains("new_laisuat"))
                throw new Exception("Thửa đất canh tác không có lãi suất");

            if (((OptionSetValue)thuadatcanhtac["new_loailaisuat"]).Value == 100000000)
                colai = true;

            // type = 1 - hl , type = 2 - khl
            if (sotien > 0)
            {
                StringBuilder Name = new StringBuilder();
                Name.Append("PBDT");

                if (hddtmia.Contains("new_masohopdong"))
                    Name.Append("-" + hddtmia["new_masohopdong"].ToString());

                if (KH.Contains("fullname"))
                    Name.Append("-" + KH["fullname"]);

                else if (KH.Contains("name"))
                    Name.Append("-" + KH["name"]);

                #region phan bo KHL

                Entity phanbodautuKHL = new Entity("new_phanbodautu");
                //phanbodautu["new_etltransaction"] =
                phanbodautuKHL["new_name"] = Name.ToString();

                if (KH.LogicalName == "contact")
                    phanbodautuKHL["new_khachhang"] = KH.ToEntityReference();
                else if (KH.LogicalName == "account")
                    phanbodautuKHL["new_khachhangdoanhnghiep"] = KH.ToEntityReference();

                phanbodautuKHL["new_etltransaction"] = new EntityReference("new_etltransaction", etlID);

                if (type == 2)
                    phanbodautuKHL["new_loaidautu"] = new OptionSetValue(100000000); // credit

                else if (type == 1)
                    phanbodautuKHL["new_loaidautu"] = new OptionSetValue(100000002); // standard                

                phanbodautuKHL["new_loaihopdong"] = new OptionSetValue(100000000);
                phanbodautuKHL["new_hopdongdautumia"] = hddtmia.ToEntityReference();
                phanbodautuKHL["new_thuacanhtac"] = new EntityReference("new_thuadatcanhtac", tdct);
                phanbodautuKHL["new_vudautu"] = vudautu;
                phanbodautuKHL["new_sotien"] = new Money(sotien);
                phanbodautuKHL["new_conlai"] = new Money(sotien);
                phanbodautuKHL["new_tram"] = tram.ToEntityReference();
                phanbodautuKHL["new_vbnv"] = cbnv.ToEntityReference();
                phanbodautuKHL["new_ngayphatsinh"] = ngaylapphieu;
                phanbodautuKHL["new_chitietgiaonhanvattu"] = giaingan.ToEntityReference();

                if (colai == true)
                    phanbodautuKHL["new_laisuat"] = thuadatcanhtac["new_laisuat"];

                service.Create(phanbodautuKHL);

                #endregion phan bo KHL
            }
        }

        private List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>();
        }

        public void GenPhanBoDauTu(Entity target, Guid etlID)
        {
            int type = 0;
            Entity phieugiaonhan = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_hopdongdautumia", "new_khachhang",
                        "new_khachhangdoanhnghiep", "new_vudautu","new_phieudangkyvattu",
                        "new_ngaynhanvattu","new_tram","new_canbonongvu" }));

            if (!phieugiaonhan.Contains("new_ngaynhanvattu"))
                throw new Exception("Phiếu giao nhận không có ngày lập phiếu");

            if (!phieugiaonhan.Contains("new_phieudangkyvattu"))
                throw new Exception("Phiếu giao nhận không có phiếu đăng ký vật tư");

            Entity tram = null;
            Entity cbnv = null;
            DateTime ngaylapphieu = ((DateTime)phieugiaonhan["new_ngaynhanvattu"]);

            if (phieugiaonhan.Contains("new_tram"))
                tram = service.Retrieve("businessunit", ((EntityReference)phieugiaonhan["new_tram"]).Id,
                    new ColumnSet(new string[] { "businessunitid" }));

            if (phieugiaonhan.Contains("new_canbonongvu"))
                cbnv = service.Retrieve("new_kiemsoatvien", ((EntityReference)phieugiaonhan["new_canbonongvu"]).Id,
                    new ColumnSet(new string[] { "new_kiemsoatvienid" }));

            List<Entity> lstChitietPGNHM = RetrieveMultiRecord(service, "new_chitietgiaonhanvattu",
                    new ColumnSet(new string[] { "new_sotienhl", "new_sotienkhl" }),
                    "new_phieugiaonhanvattu", phieugiaonhan.Id);

            EntityCollection entcChitiet = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieugiaonhanvattu",
                "new_new_pgnvattu_new_chitiethddtmia",
                new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_phieugiaonhanvattuid", phieugiaonhan.Id);

            decimal tongdmhl = 0;
            decimal tongdmkhl = 0;

            List<Entity> lstThoai = RetrieveMultiRecord(service, "new_thuadat_pdkvattukhac",
                new ColumnSet(true), "new_phieudangky", ((EntityReference)phieugiaonhan["new_phieudangkyvattu"]).Id);

            if (entcChitiet.Entities.Count == 0)
                return;

            // error
            Entity KH = null;

            if (!phieugiaonhan.Contains("new_hopdongdautumia"))
                throw new Exception("Phiếu giao nhận không có hợp đồng đầu tư mía");

            Entity hddtmia = service.Retrieve("new_hopdongdautumia",
                ((EntityReference)phieugiaonhan["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_masohopdong" }));

            if (phieugiaonhan.Contains("new_khachhang"))
                KH = service.Retrieve("contact", ((EntityReference)phieugiaonhan["new_khachhang"]).Id, new ColumnSet(new string[] { "fullname" }));

            else if (phieugiaonhan.Contains("new_khachhangdoanhnghiep"))
                KH = service.Retrieve("account", ((EntityReference)phieugiaonhan["new_khachhangdoanhnghiep"]).Id, new ColumnSet(new string[] { "name" }));

            if (KH == null)
                throw new Exception("Phiếu giao nhận vật tư không có khách hàng");

            Dictionary<Guid, DinhMuc> dtDinhMuc = new Dictionary<Guid, DinhMuc>();
            foreach (Entity en in lstThoai)
            {
                Entity chitiet = service.Retrieve("new_thuadatcanhtac",
                            ((EntityReference)en["new_chitiethopdong"]).Id, new ColumnSet(new string[] { "new_thuadatcanhtacid" }));
                decimal hl = (en.Contains("new_dmhlvt") ? ((Money)en["new_dmhlvt"]).Value : new decimal(0)) + (en.Contains("new_dmhltm") ? ((Money)en["new_dmhltm"]).Value : new decimal(0));
                decimal khl = en.Contains("new_dm0hl") ? ((Money)en["new_dm0hl"]).Value : new decimal(0);

                tongdmhl += hl;
                tongdmkhl += khl;

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
                decimal phanbokhonghoanlai = giaingan.Contains("new_sotienkhl") ? ((Money)giaingan["new_sotienkhl"]).Value : new decimal(0);
                decimal phanbohoanlai = giaingan.Contains("new_sotienhl") ? ((Money)giaingan["new_sotienhl"]).Value : new decimal(0);

                Entity vudautu = service.Retrieve("new_vudautu",
                        ((EntityReference)phieugiaonhan["new_vudautu"]).Id, new ColumnSet(new string[] { "new_vudautuid" }));

                int i = 0;
                foreach (Guid key in dtDinhMuc.Keys)
                {
                    i++;
                    DinhMuc a = dtDinhMuc[key];
                    if (tongdmkhl == 0)
                        throw new Exception("Tổng định mức không hoàn lại phải khác 0");

                    decimal sotien = phanbokhonghoanlai * a.dinhMucKHL / tongdmkhl;

                    trace.Trace(sotien.ToString() + "-" + phanbokhonghoanlai.ToString()
                                            + "-" + a.dinhMucKHL + "-" + tongdmkhl.ToString() + "\n");

                    CreatePBDT(hddtmia, KH, key, vudautu.ToEntityReference(), sotien, etlID,
                        type = 1, tram, cbnv, ngaylapphieu,giaingan);
                }

                Dictionary<Guid, decimal> dtTungthua = new Dictionary<Guid, decimal>();

                foreach (Guid key in dtDinhMuc.Keys)
                {
                    DinhMuc a = dtDinhMuc[key];
                    if (tongdmhl == 0)
                        throw new Exception("Tổng định mức hoàn lại phải khác 0");

                    decimal sotien = phanbohoanlai * a.dinhMucHL / tongdmhl;

                    trace.Trace(sotien.ToString() + "-" + phanbohoanlai.ToString()
                                            + "-" + a.dinhMucHL + "-" + tongdmhl.ToString() + "\n");

                    if (!dtTungthua.ContainsKey(key))
                        dtTungthua.Add(key, sotien);
                }

                foreach (Guid key in dtTungthua.Keys)
                {
                    List<Tylethuhoivon> lstTylethuhoivon = dtTyleThuhoi[key];
                    decimal dinhmuc = dtTungthua[key];

                    foreach (Tylethuhoivon a in lstTylethuhoivon)
                    {
                        Entity tilethuhoivon = service.Retrieve("new_tylethuhoivondukien", a.tylethuhoiid,
                                new ColumnSet(new string[] { "new_sotienthuhoi", "new_tiendaphanbo" }));
                        decimal tiendaphanbo = tilethuhoivon.Contains("new_tiendaphanbo") ?
                             ((Money)tilethuhoivon["new_tiendaphanbo"]).Value : new decimal(0);

                        if (dinhmuc < a.sotien - a.daphanbo)
                        {
                            CreatePBDT(hddtmia, KH, key, a.vuthuhoi, dinhmuc, etlID, type = 2,
                                tram, cbnv, ngaylapphieu,giaingan);
                            tiendaphanbo = tiendaphanbo + dinhmuc;
                            tilethuhoivon["new_tiendaphanbo"] = new Money(tiendaphanbo);
                            //service.Update(tilethuhoivon);
                            break;
                        }
                        else if (dinhmuc > a.sotien - a.daphanbo)
                        {
                            CreatePBDT(hddtmia, KH, key, a.vuthuhoi, a.sotien - a.daphanbo,
                                etlID, type = 2, tram, cbnv, ngaylapphieu,giaingan);
                            tiendaphanbo = tiendaphanbo + (a.sotien - a.daphanbo);
                            dinhmuc = dinhmuc - (a.sotien + a.daphanbo);
                            tilethuhoivon["new_tiendaphanbo"] = new Money(tiendaphanbo);
                            //service.Update(tilethuhoivon);
                        }
                    }
                }
            }
        }

        public static string Serialize(object obj)
        {
            using (MemoryStream memoryStream = new MemoryStream())
            using (StreamReader reader = new StreamReader(memoryStream))
            {
                DataContractSerializer serializer = new DataContractSerializer(obj.GetType());
                serializer.WriteObject(memoryStream, obj);
                memoryStream.Position = 0;
                return reader.ReadToEnd();
            }
        }

        public static object Deserialize(string xml, Type toType)
        {
            using (Stream stream = new MemoryStream())
            {
                byte[] data = System.Text.Encoding.UTF8.GetBytes(xml);
                stream.Write(data, 0, data.Length);
                stream.Position = 0;
                DataContractSerializer deserializer = new DataContractSerializer(toType);
                return deserializer.ReadObject(stream);
            }
        }

        private void Send(Entity tmp)
        {
            MessageQueue mq;

            if (MessageQueue.Exists(@".\Private$\DynamicCRM2Oracle"))
                mq = new MessageQueue(@".\Private$\DynamicCRM2Oracle");
            else
                mq = MessageQueue.Create(@".\Private$\DynamicCRM2Oracle");

            Message m = new Message();
            m.Body = Serialize(tmp);
            m.Label = "cust";
            mq.Send(m);
        }
    }
}