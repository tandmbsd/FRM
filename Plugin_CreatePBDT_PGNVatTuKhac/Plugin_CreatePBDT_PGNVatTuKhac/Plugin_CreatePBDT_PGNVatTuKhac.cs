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
            bool flag = false;

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000) // da duyệt
            {
                Entity fullEntity = (Entity)context.PostEntityImages["PostImg"];
                Entity Vudautu = service.Retrieve("new_vudautu", ((EntityReference)fullEntity["new_vudautu"]).Id, new ColumnSet(new string[] { "new_mavudautu" }));
                var lsVuThuHoach = RetrieveMultiRecord(service, "new_vuthuhoach", new ColumnSet(true), "new_vudautu", ((EntityReference)fullEntity["new_vudautu"]).Id);
                var vuThuHoach = lsVuThuHoach.Count > 0 ? lsVuThuHoach[0] : null;
                string vuMua = "";
                if (vuThuHoach != null)
                {
                    vuMua = ((DateTime)vuThuHoach["new_tungay"]).ToString("yyyy") + "-" + ((DateTime)vuThuHoach["new_denngay"]).ToString("yyyy");
                }

                string test = "";

                Entity HDMia = service.Retrieve("new_hopdongdautumia", ((EntityReference)fullEntity["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_masohopdong" }));
                Entity KH = null;
                if (fullEntity.Contains("new_khachhang"))
                    KH = service.Retrieve("contact", ((EntityReference)fullEntity["new_khachhang"]).Id, new ColumnSet(true));
                else
                    KH = service.Retrieve("account", ((EntityReference)fullEntity["new_khachhangdoanhnghiep"]).Id, new ColumnSet(true));

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
                    etl_ND["new_name"] = fullEntity["new_masophieu"].ToString() + "_CRE_HL" + test;
                    etl_ND["new_vouchernumber"] = "DTND";
                    etl_ND["new_transactiontype"] = "3.2.2.a";
                    etl_ND["new_customertype"] = new OptionSetValue(fullEntity.Contains("new_khachhang") ? 1 : 2);
                    etl_ND["new_season"] = vuMua;//Vudautu["new_mavudautu"].ToString();
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
                    etl_ND["new_invoicedate"] = fullEntity["new_ngaylapphieu"];
                    etl_ND["new_descriptionheader"] = "Ghi nợ nhận vật tư khác_vụ_" + vuMua;
                    etl_ND["new_terms"] = "Tra Ngay";
                    etl_ND["new_taxtype"] = "";
                    etl_ND["new_invoiceamount"] = new Money(((Money)fullEntity["new_tongsotienhl"]).Value * (-1));
                    etl_ND["new_gldate"] = fullEntity["new_ngaynhanthuoc"];
                    etl_ND["new_invoicetype"] = "CRE";

                    if (fullEntity.Contains("new_khachhang"))
                        etl_ND["new_khachhang"] = fullEntity["new_khachhang"];
                    else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                        etl_ND["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                    Guid etl_NDID = service.Create(etl_ND);

                    etl_ND["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                    etl_ND["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                    etl_ND["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                    etl_ND["new_descriptionlines"] = fullEntity["new_name"].ToString();
                    etl_ND["tran_type"] = "CRE";

                    Send(etl_ND);
                    //gen phân bổ đầu tư
                    #region begin
                    GenPhanBoDauTuHL(target, etl_NDID);
                    #endregion

                    #endregion
                }

                if (fullEntity.Contains("new_tongsotienkhl") && ((Money)fullEntity["new_tongsotienkhl"]).Value > 0)
                {
                    #region begin
                    //tạo credit
                    Entity etl_ND = new Entity("new_etltransaction");
                    etl_ND["new_name"] = fullEntity["new_masophieu"].ToString() + "_CRE" + "_KHL" + test;
                    etl_ND["new_vouchernumber"] = "DTND";
                    etl_ND["new_transactiontype"] = "3.2.2.a";
                    etl_ND["new_customertype"] = new OptionSetValue(fullEntity.Contains("new_khachhang") ? 1 : 2);
                    etl_ND["new_season"] = vuMua;//Vudautu["new_mavudautu"].ToString();
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
                    etl_ND["new_invoicedate"] = fullEntity["new_ngaylapphieu"];
                    etl_ND["new_descriptionheader"] = "Ghi nợ nhận vật tư khác_vụ_" + vuMua;
                    etl_ND["new_terms"] = "Tra Ngay";
                    etl_ND["new_taxtype"] = "";
                    etl_ND["new_invoiceamount"] = new Money(((Money)fullEntity["new_tongsotienkhl"]).Value * (-1));
                    etl_ND["new_gldate"] = fullEntity["new_ngaynhanthuoc"];
                    etl_ND["new_invoicetype"] = "CRE";

                    if (fullEntity.Contains("new_khachhang"))
                        etl_ND["new_khachhang"] = fullEntity["new_khachhang"];
                    else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                        etl_ND["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                    Guid etl_NDID = service.Create(etl_ND);

                    etl_ND["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                    etl_ND["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                    etl_ND["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                    etl_ND["new_descriptionlines"] = fullEntity["new_name"].ToString();
                    etl_ND["tran_type"] = "CRE";

                    Send(etl_ND);

                    //gen phân bổ đầu tư
                    #region begin
                    GenPhanBoDauTuKHL(target, etl_NDID);
                    #endregion

                    // STA
                    Entity etl_STA = new Entity("new_etltransaction");
                    etl_STA["new_name"] = fullEntity["new_masophieu"].ToString() + "_STA_KHL" + test;
                    etl_STA["new_vouchernumber"] = "GSND";
                    etl_STA["new_transactiontype"] = "3.1.3.a";
                    etl_STA["new_customertype"] = new OptionSetValue(fullEntity.Contains("new_khachhang") ? 1 : 2);
                    etl_STA["new_season"] = vuMua;//Vudautu["new_mavudautu"].ToString();
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
                    etl_STA["new_invoicedate"] = fullEntity["new_ngaynhanthuoc"];
                    etl_STA["new_descriptionheader"] = "Giao nhận vật tư khác_vụ_" + vuMua;
                    etl_STA["new_terms"] = "Tra Ngay";
                    etl_STA["new_taxtype"] = "";
                    etl_STA["new_invoiceamount"] = (Money)fullEntity["new_tongsotienkhl"];
                    etl_STA["new_gldate"] = fullEntity["new_ngayduyet"];
                    etl_STA["new_invoicetype"] = "STA";

                    if (fullEntity.Contains("new_khachhang"))
                        etl_STA["new_khachhang"] = fullEntity["new_khachhang"];
                    else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                        etl_STA["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                    var etl_STAID = service.Create(etl_STA);

                    etl_STA["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                    etl_STA["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                    etl_STA["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                    etl_STA["new_descriptionlines"] = fullEntity["new_name"].ToString();
                    etl_STA["tran_type"] = "STA";

                    Send(etl_STA);
                    //Pay cấn trừ
                    #region Tạo transaction apply CRE
                    Entity apply_PGNhomgiong_CRE = new Entity("new_applytransaction");
                    //apply_PGNPhanbon["new_documentsequence"] = value++;
                    apply_PGNhomgiong_CRE["new_suppliersitecode"] = "Tây Ninh";

                    //if (KH.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)KH["new_phuongthucthanhtoan"]).Value == 100000001)
                    //{
                    //    List<Entity> taikhoannganhang = RetrieveMultiRecord(service, "new_taikhoannganhang",
                    //        new ColumnSet(new string[] { "new_sotaikhoan", "new_giaodichchinh" }),
                    //        KH.LogicalName == "contact" ? "new_khachhang" : "new_khachhangdoanhnghiep", KH.Id);

                    //    Entity taikhoanchinh = null;

                    //    foreach (Entity en in taikhoannganhang)
                    //    {
                    //        if ((bool)en["new_giaodichchinh"] == true)
                    //            taikhoanchinh = en;
                    //    }

                    //    apply_PGNhomgiong_CRE["new_bankcccountnum"] = (taikhoanchinh == null ? "CTXL-VND-0" : taikhoanchinh["new_sotaikhoan"]);
                    //}
                    //else
                    //{
                    apply_PGNhomgiong_CRE["new_bankcccountnum"] = "CTXL-VND-0";
                    //}

                    Entity etl_entityCRE = service.Retrieve("new_etltransaction", etl_NDID, new ColumnSet(new string[] { "new_name" }));
                    if (etl_entityCRE != null && etl_entityCRE.Contains("new_name"))
                    {
                        apply_PGNhomgiong_CRE["new_name"] = (string)etl_entityCRE["new_name"];
                    }

                    //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                    apply_PGNhomgiong_CRE["new_paymentamount"] = new Money(((Money)fullEntity["new_tongsotienkhl"]).Value * (-1));
                    //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                    apply_PGNhomgiong_CRE["new_paymentdate"] = fullEntity["new_ngaynhanthuoc"];
                    apply_PGNhomgiong_CRE["new_paymentdocumentname"] = "CANTRU_03";
                    apply_PGNhomgiong_CRE["new_vouchernumber"] = "CTND";
                    apply_PGNhomgiong_CRE["new_cashflow"] = "00.00";
                    apply_PGNhomgiong_CRE["new_referencenumber"] = fullEntity["new_masophieu"].ToString();
                    apply_PGNhomgiong_CRE["new_paymentnum"] = 1;
                    apply_PGNhomgiong_CRE["new_documentnum"] = fullEntity["new_masophieu"].ToString();

                    if (fullEntity.Contains("new_khachhang"))
                        apply_PGNhomgiong_CRE["new_khachhang"] = fullEntity["new_khachhang"];
                    else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                        apply_PGNhomgiong_CRE["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                    apply_PGNhomgiong_CRE.Id = service.Create(apply_PGNhomgiong_CRE);

                    apply_PGNhomgiong_CRE["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                    apply_PGNhomgiong_CRE["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                    apply_PGNhomgiong_CRE["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                    apply_PGNhomgiong_CRE["new_type"] = "TYPE4";

                    #endregion
                    Send(apply_PGNhomgiong_CRE);
                    #region Tạo transaction apply STA
                    Entity apply_PGNhomgiong_STA = new Entity("new_applytransaction");
                    //apply_PGNPhanbon["new_documentsequence"] = value++;
                    apply_PGNhomgiong_STA["new_suppliersitecode"] = "Tây Ninh";

                    //if (KH.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)KH["new_phuongthucthanhtoan"]).Value == 100000001)
                    //{
                    //    List<Entity> taikhoannganhang = RetrieveMultiRecord(service, "new_taikhoannganhang",
                    //        new ColumnSet(new string[] { "new_sotaikhoan", "new_giaodichchinh" }),
                    //        KH.LogicalName == "contact" ? "new_khachhang" : "new_khachhangdoanhnghiep", KH.Id);

                    //    Entity taikhoanchinh = null;

                    //    foreach (Entity en in taikhoannganhang)
                    //    {
                    //        if ((bool)en["new_giaodichchinh"] == true)
                    //            taikhoanchinh = en;
                    //    }

                    //    //apply_PGNhomgiong_STA["new_supplierbankname"] = (taikhoanchinh == null ? "" : taikhoanchinh["new_sotaikhoan"]);
                    //    apply_PGNhomgiong_STA["new_bankcccountnum"] = (taikhoanchinh == null ? "CTXL-VND-0" : taikhoanchinh["new_sotaikhoan"]);
                    //}
                    //else
                    //{
                    apply_PGNhomgiong_STA["new_bankcccountnum"] = "CTXL-VND-0";
                    //}

                    Entity etl_entitySTA = service.Retrieve("new_etltransaction", etl_STAID, new ColumnSet(new string[] { "new_name" }));
                    if (etl_entitySTA != null && etl_entitySTA.Contains("new_name"))
                    {
                        apply_PGNhomgiong_STA["new_name"] = (string)etl_entitySTA["new_name"];
                    }
                    //apply_PGNhomgiong_STA["new_name"] = "new_phieugiaonhanhomgiong";
                    apply_PGNhomgiong_STA["new_paymentamount"] = fullEntity["new_tongsotienkhl"];
                    //apply_PGNhomgiong_STA["new_suppliernumber"] = KH["new_makhachhang"];
                    apply_PGNhomgiong_STA["new_paymentdate"] = fullEntity["new_ngaynhanthuoc"];
                    apply_PGNhomgiong_STA["new_paymentdocumentname"] = "CANTRU_03";
                    apply_PGNhomgiong_STA["new_vouchernumber"] = "CTND";
                    apply_PGNhomgiong_STA["new_cashflow"] = "00.00";
                    apply_PGNhomgiong_STA["new_referencenumber"] = fullEntity["new_masophieu"].ToString();
                    apply_PGNhomgiong_STA["new_paymentnum"] = 1;
                    apply_PGNhomgiong_STA["new_documentnum"] = fullEntity["new_masophieu"].ToString();

                    if (fullEntity.Contains("new_khachhang"))
                        apply_PGNhomgiong_STA["new_khachhang"] = fullEntity["new_khachhang"];
                    else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                        apply_PGNhomgiong_STA["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                    apply_PGNhomgiong_STA.Id = service.Create(apply_PGNhomgiong_STA);

                    apply_PGNhomgiong_STA["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                    apply_PGNhomgiong_STA["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                    apply_PGNhomgiong_STA["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                    apply_PGNhomgiong_STA["new_type"] = "TYPE4";

                    Send(apply_PGNhomgiong_STA);

                    #endregion

                    #endregion
                }

                Send(null);
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

        public void CreatePBDT(Entity hddtmia, Entity KH, Guid tdct, EntityReference vudautu, EntityReference vuthanhtoan,
            decimal sotien, Guid etlID, int type, Entity tram, Entity cbnv, DateTime ngaygiaonhan,
            Entity pgnvt, string sophieu)
        {
            bool colai = false;
            Entity thuadatcanhtac = service.Retrieve("new_thuadatcanhtac", tdct,
                new ColumnSet(new string[] { "new_laisuat", "new_name", "new_loailaisuat", "new_dachikhonghoanlai_vattukhac", "new_dachihoanlai_vattukhac" }));

            int loailaisuat = ((OptionSetValue)thuadatcanhtac["new_loailaisuat"]).Value;

            if (!thuadatcanhtac.Contains("new_laisuat"))
                throw new Exception(thuadatcanhtac["new_name"].ToString() + " không có lãi suất");

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

                decimal dachihoanlai = thuadatcanhtac.Contains("new_dachihoanlai_vattukhac") ? ((Money)thuadatcanhtac["new_dachihoanlai_vattukhac"]).Value : new decimal(0);
                decimal dachikhonghoanlai = thuadatcanhtac.Contains("new_dachikhonghoanlai_vattukhac") ? ((Money)thuadatcanhtac["new_dachikhonghoanlai_vattukhac"]).Value : new decimal(0);

                if (type == 2)//HL
                {
                    phanbodautuKHL["new_loaidautu"] = new OptionSetValue(100000000); // credit
                    thuadatcanhtac["new_dachihoanlai_vattukhac"] = new Money(dachihoanlai + sotien);
                }
                else if (type == 1)//KHL
                {
                    phanbodautuKHL["new_loaidautu"] = new OptionSetValue(100000002); // standard
                    thuadatcanhtac["new_dachikhonghoanlai_vattukhac"] = new Money(dachikhonghoanlai + sotien);
                }

                service.Update(thuadatcanhtac);

                phanbodautuKHL["new_loaihopdong"] = new OptionSetValue(100000000);
                phanbodautuKHL["new_hopdongdautumia"] = hddtmia.ToEntityReference();
                phanbodautuKHL["new_thuacanhtac"] = new EntityReference("new_thuadatcanhtac", tdct);
                phanbodautuKHL["new_vudautu"] = vudautu;
                phanbodautuKHL["new_vuthanhtoan"] = vuthanhtoan;
                phanbodautuKHL["new_sotien"] = new Money(sotien);
                phanbodautuKHL["new_conlai"] = new Money(sotien);
                phanbodautuKHL["new_tram"] = tram.ToEntityReference();
                phanbodautuKHL["new_vbnv"] = cbnv.ToEntityReference();
                phanbodautuKHL["new_ngayphatsinh"] = ngaygiaonhan;
                phanbodautuKHL["new_phieugiaonhanvattu"] = pgnvt.ToEntityReference();
                phanbodautuKHL["new_loailaisuat"] = new OptionSetValue(loailaisuat);
                phanbodautuKHL["new_mucdichdautu"] = new OptionSetValue(100000000);
                phanbodautuKHL["new_sophieu"] = sophieu;
                phanbodautuKHL["new_laisuat"] = Getlaisuat(vudautu, 100000000, ngaygiaonhan);

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

        public void GenPhanBoDauTuKHL(Entity target, Guid etlID)
        {
            int type = 0;
            Entity phieugiaonhan = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_hopdongdautumia", "new_khachhang",
                        "new_khachhangdoanhnghiep", "new_vudautu","new_phieudangkyvattu",
                        "new_ngaynhanvattu","new_tram","new_canbonongvu","new_masophieu" }));

            string sophieu = phieugiaonhan.Contains("new_masophieu") ? (string)phieugiaonhan["new_masophieu"] : "";

            if (!phieugiaonhan.Contains("new_ngaynhanvattu"))
                throw new Exception("Phiếu giao nhận không có ngày lập phiếu");

            if (!phieugiaonhan.Contains("new_phieudangkyvattu"))
                throw new Exception("Phiếu giao nhận không có phiếu đăng ký vật tư");

            Entity tram = null;
            Entity cbnv = null;

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

            Entity vudautu = service.Retrieve("new_vudautu",
                        ((EntityReference)phieugiaonhan["new_vudautu"]).Id, new ColumnSet(new string[] { "new_vudautuid" }));

            foreach (Entity giaingan in lstChitietPGNHM) // vong lap giai ngan
            {
                decimal phanbokhonghoanlai = giaingan.Contains("new_sotienkhl") ? ((Money)giaingan["new_sotienkhl"]).Value : new decimal(0);
                decimal phanbohoanlai = giaingan.Contains("new_sotienhl") ? ((Money)giaingan["new_sotienhl"]).Value : new decimal(0);
                DateTime ngaynhan = (DateTime)giaingan["new_ngaynhan"];

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

                    Entity chitiet = service.Retrieve("new_thuadatcanhtac", key,
                        new ColumnSet(new string[] { "new_chinhsachdautu", "new_name" }));

                    if (!chitiet.Contains("new_chinhsachdautu"))
                        throw new Exception(chitiet["new_name"].ToString() + " không có chính sách đầu tư");

                    Entity CSDT = service.Retrieve("new_chinhsachdautu", ((EntityReference)chitiet["new_chinhsachdautu"]).Id,
                        new ColumnSet(new string[] { "new_new_thoihanthuhoivon_khl", "new_machinhsach" }));

                    int sonamthuhoiKHL = CSDT.Contains("new_new_thoihanthuhoivon_khl") ? (int)CSDT["new_new_thoihanthuhoivon_khl"] : 0;
                    decimal sotienphanboKHL = 0;

                    if (sonamthuhoiKHL != 0)
                        sotienphanboKHL = sotien / sonamthuhoiKHL;
                    else
                        return;

                    List<Entity> lst = RetrieveVudautu().Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList<Entity>();
                    int curr = lst.FindIndex(p => p.Id == vudautu.Id);

                    for (int k = 0; k < sonamthuhoiKHL; k++)
                    {
                        Entity vudaututhuhoi = lst[curr++];
                        trace.Trace(vudaututhuhoi["new_mavudautu"].ToString());
                        CreatePBDT(hddtmia, KH, key, vudaututhuhoi.ToEntityReference(), vudaututhuhoi.ToEntityReference(), sotienphanboKHL,
                        etlID, type = 1, tram, cbnv, ngaynhan, phieugiaonhan, sophieu);

                        if (curr > lst.Count - 1)
                            throw new Exception("Phân bổ không hoàn lại thất bại. Vui lòng tạo thêm vụ đầu tư mới để phân bổ");
                    }
                }
            }
        }

        public void GenPhanBoDauTuHL(Entity target, Guid etlID)
        {
            int type = 0;
            Entity phieugiaonhan = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_hopdongdautumia", "new_khachhang",
                        "new_khachhangdoanhnghiep", "new_vudautu","new_phieudangkyvattu",
                        "new_ngaynhanvattu","new_tram","new_canbonongvu","new_masophieu" }));

            string sophieu = phieugiaonhan.Contains("new_masophieu") ? (string)phieugiaonhan["new_masophieu"] : "";

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
                    new ColumnSet(new string[] { "new_sotienhl", "new_sotienkhl", "new_ngaynhan" }),
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
                DateTime ngaynhan = (DateTime)giaingan["new_ngaynhan"];

                Entity vudautu = service.Retrieve("new_vudautu",
                        ((EntityReference)phieugiaonhan["new_vudautu"]).Id, new ColumnSet(new string[] { "new_vudautuid" }));

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
                        decimal sotienphanbo = a.sotien - a.daphanbo;

                        if (dinhmuc < sotienphanbo)
                        {
                            CreatePBDT(hddtmia, KH, key, vudautu.ToEntityReference(), a.vuthuhoi, dinhmuc,
                        etlID, type = 2, tram, cbnv, ngaynhan, phieugiaonhan, sophieu);
                            tiendaphanbo = tiendaphanbo + dinhmuc;

                            break;
                        }
                        else if (dinhmuc > a.sotien - a.daphanbo)
                        {
                            CreatePBDT(hddtmia, KH, key, vudautu.ToEntityReference(), a.vuthuhoi, sotienphanbo,
                        etlID, type = 2, tram, cbnv, ngaynhan, phieugiaonhan, sophieu);
                            tiendaphanbo = tiendaphanbo + sotienphanbo;
                            dinhmuc = dinhmuc - sotienphanbo;
                        }

                        tilethuhoivon["new_tiendaphanbo"] = new Money(tiendaphanbo);
                        //service.Update(tilethuhoivon);
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

        EntityCollection RetrieveVudautu()
        {
            QueryExpression q = new QueryExpression("new_vudautu");
            q.ColumnSet = new ColumnSet(true);

            EntityCollection entc = service.RetrieveMultiple(q);

            return entc;
        }

        private decimal Getlaisuat(EntityReference vudautu, int mucdichdautu, DateTime ngaygiaonhan)
        {
            QueryExpression qbangLai = new QueryExpression("new_banglaisuatthaydoi");
            qbangLai.ColumnSet = new ColumnSet(new string[] { "new_name", "new_ngayapdung", "new_phantramlaisuat" });
            qbangLai.Criteria = new FilterExpression(LogicalOperator.And);
            //qbangLai.Criteria.AddCondition(new ConditionExpression("new_chinhsachdautu", ConditionOperator.Equal,
            //    ((EntityReference)thuacanhtac["new_chinhsachdautu"]).Id));
            qbangLai.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            qbangLai.Criteria.AddCondition(new ConditionExpression("new_vudautuapdung", ConditionOperator.Equal, vudautu.Id));
            qbangLai.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, mucdichdautu));
            qbangLai.AddOrder("new_ngayapdung", OrderType.Ascending);
            EntityCollection bls = service.RetrieveMultiple(qbangLai);
            //Entity kq = null;
            decimal result = 0;
            int n = bls.Entities.Count;
            trace.Trace(n.ToString());
            for (int i = 0; i < n; i++)
            {
                Entity q = bls[i];

                DateTime dt = (DateTime)q["new_ngayapdung"];
                if (n == 1 && CompareDate(ngaygiaonhan, dt) == 0)
                {
                    trace.Trace("A");
                    result = (decimal)q["new_phantramlaisuat"];
                    break;
                }
                else if (n > 1 && CompareDate(ngaygiaonhan, dt) < 0)
                {
                    trace.Trace("B");
                    result = (decimal)bls[i - 1]["new_phantramlaisuat"];
                    break;
                }
                else if (i == n - 1)
                {
                    trace.Trace("C");
                    result = (decimal)bls[(i > 0 ? i : 1) - 1]["new_phantramlaisuat"];
                    break;
                }
            }

            return result;
        }

        private decimal CompareDate(DateTime date1, DateTime date2) // begin,end
        {
            string currentTimerZone = TimeZoneInfo.Local.Id;
            DateTime d1 = TimeZoneInfo.ConvertTimeBySystemTimeZoneId(date1, currentTimerZone);
            DateTime d2 = TimeZoneInfo.ConvertTimeBySystemTimeZoneId(date2, currentTimerZone);
            decimal temp = (decimal)d1.Date.Subtract(d2.Date).TotalDays;
            return temp;
        }
    }
}