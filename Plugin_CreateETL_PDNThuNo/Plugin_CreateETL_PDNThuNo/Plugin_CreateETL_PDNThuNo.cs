using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Messaging;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Plugin_CreateETL_PDNThuNo
{
    public class Plugin_CreateETL_PDNThuNo : IPlugin
    {
        IOrganizationService service;
        IOrganizationServiceFactory factory = null;
        IPluginExecutionContext context;
        public void Execute(IServiceProvider serviceProvider)
        {
            context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000) // da duyet
            {
                Entity phieuDNThuNo = (Entity)context.PostEntityImages["PostImg"];

                if (!phieuDNThuNo.Contains("new_loaithuno"))
                {
                    throw new Exception("Phiếu thu nợ chưa có loại thu nợ!");
                }
                Entity khNongDan = null;
                if (phieuDNThuNo.Contains("new_khachhang"))
                    khNongDan = service.Retrieve("contact", ((EntityReference)phieuDNThuNo["new_khachhang"]).Id, new ColumnSet(true));
                else
                    khNongDan = service.Retrieve("account", ((EntityReference)phieuDNThuNo["new_khachhangdoanhnghiep"]).Id, new ColumnSet(true));

                var vudautu = service.Retrieve("new_vudautu", ((EntityReference)phieuDNThuNo["new_vudautu"]).Id, new ColumnSet(new string[] { "new_mavudautu" }));
                var lsVuThuHoach = RetrieveMultiRecord(service, "new_vuthuhoach", new ColumnSet(true), "new_vudautu", ((EntityReference)phieuDNThuNo["new_vudautu"]).Id);
                var vuThuHoach = lsVuThuHoach.Count > 0 ? lsVuThuHoach[0] : null;
                var vuMua = "";
                if (vuThuHoach != null)
                {
                    vuMua = ((DateTime)vuThuHoach["new_tungay"]).ToString("yyyy") + "-" + ((DateTime)vuThuHoach["new_denngay"]).ToString("yyyy");
                }

                if (((OptionSetValue)phieuDNThuNo["new_loaithuno"]).Value == 100000001)// can tru
                {
                    #region Can Tru
                    //var queryPhieuTinhLai = new QueryExpression("new_phieutinhlai");
                    //queryPhieuTinhLai.Criteria = new FilterExpression();
                    //queryPhieuTinhLai.Criteria.AddCondition("new_phieudenghithuno", ConditionOperator.Equal, phieuDNThuNo.Id);
                    //queryPhieuTinhLai.ColumnSet = new ColumnSet(true);

                    //var dsPhieuTinhLai = service.RetrieveMultiple(queryPhieuTinhLai);

                    //if (dsPhieuTinhLai != null && dsPhieuTinhLai.Entities.Count > 0)
                    //{
                    //    #region Phat sint etl STA Theo phieu tinh lai
                    //    //Entity apply_PhaiTraSTA = new Entity("new_applytransaction");

                    //    //Entity etl_entity = service.Retrieve("new_etltransaction", phaiTraSTAID, new ColumnSet(new string[] { "new_name" }));
                    //    //if (etl_entity != null && etl_entity.Contains("new_name"))
                    //    //{
                    //    //    apply_PhaiTraSTA["new_name"] = (string)etl_entity["new_name"];
                    //    //}

                    //    //apply_PhaiTraSTA["new_suppliersitecode"] = "Tây Ninh";

                    //    //if (kHPhaiTra.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)kHPhaiTra["new_phuongthucthanhtoan"]).Value == 100000001)
                    //    //{
                    //    //    List<Entity> taikhoannganhang = RetrieveMultiRecord(service, "new_taikhoannganhang",
                    //    //        new ColumnSet(new string[] { "new_sotaikhoan", "new_giaodichchinh" }),
                    //    //        kHPhaiTra.LogicalName == "contact" ? "new_khachhang" : "new_khachhangdoanhnghiep", kHPhaiTra.Id);

                    //    //    Entity taikhoanchinh = null;

                    //    //    foreach (Entity en in taikhoannganhang)
                    //    //    {
                    //    //        if ((bool)en["new_giaodichchinh"] == true)
                    //    //            taikhoanchinh = en;
                    //    //    }

                    //    //    apply_PhaiTraSTA["new_supplierbankname"] = (taikhoanchinh == null ? "CTXL-VND-0" : taikhoanchinh["new_sotaikhoan"]);
                    //    //}
                    //    //else
                    //    //    apply_PhaiTraSTA["new_supplierbankname"] = "CTXL-VND-0";

                    //    ////apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                    //    //apply_PhaiTraSTA["new_referencenumber"] = i.ToString();
                    //    //apply_PhaiTraSTA["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienvay"]).Value);
                    //    ////apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                    //    //apply_PhaiTraSTA["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                    //    //apply_PhaiTraSTA["new_paymentdocumentname"] = "CANTRU_03";
                    //    //apply_PhaiTraSTA["new_vouchernumber"] = "CTND";
                    //    //apply_PhaiTraSTA["new_cashflow"] = "00.00";
                    //    //apply_PhaiTraSTA["new_paymentnum"] = "1";
                    //    //apply_PhaiTraSTA["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();
                    //    //apply_PhaiTraSTA["new_documentsequence"] = "";// phieuDNThuNo["new_lannhan"];

                    //    //if (phieuDNThuNo.Contains("new_khachhang"))
                    //    //    apply_PhaiTraSTA["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                    //    //else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                    //    //    apply_PhaiTraSTA["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                    //    //apply_PhaiTraSTA.Id = service.Create(apply_PhaiTraSTA);

                    //    //apply_PhaiTraSTA["new_makhachhang"] = kHPhaiTra.Contains("new_makhachhang") ? kHPhaiTra["new_makhachhang"].ToString() : "";
                    //    //apply_PhaiTraSTA["name"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("fullname") ? kHPhaiTra["fullname"].ToString() : "") : (kHPhaiTra.Contains("name") ? kHPhaiTra["name"].ToString() : ""));
                    //    //apply_PhaiTraSTA["new_socmnd"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("new_socmnd") ? kHPhaiTra["new_socmnd"].ToString() : "") : (kHPhaiTra.Contains("new_masothue") ? kHPhaiTra["new_masothue"].ToString() : ""));
                    //    //Send(apply_PhaiTraSTA);
                    //    #endregion

                    //    int i = 0;
                    //    foreach (var phieuTinhLai in dsPhieuTinhLai.Entities)
                    //    {
                    //        ++i;

                    //        #region Can tru, Phat sinh CRE hoac PRE
                    //        // Can tru
                    //        Entity phanBoDauTu = service.Retrieve("new_phanbodautu", ((EntityReference)phieuTinhLai["new_phanbodautu"]).Id, new ColumnSet(true));
                    //        if (phanBoDauTu.Contains("new_etltransaction"))
                    //        {
                    //            if (phieuDNThuNo.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)phieuDNThuNo["new_phuongthucthanhtoan"]).Value == 100000001)
                    //            {
                    //                Entity etlTransaction = service.Retrieve("new_etltransaction", ((EntityReference)phanBoDauTu["new_etltransaction"]).Id, new ColumnSet(true));
                    //                if (etlTransaction.Contains("new_invoicetype") && (etlTransaction["new_invoicetype"].ToString() == "PRE" || etlTransaction["new_invoicetype"].ToString() == "CRE"))
                    //                {
                    //                    Entity apply_PhaiTraCanTruPRE = new Entity("new_applytransaction");

                    //                    //Entity etl_entity = service.Retrieve("new_etltransaction", phaiTraSTAID, new ColumnSet(new string[] { "new_name" }));
                    //                    if (etlTransaction != null && etlTransaction.Contains("new_name"))
                    //                    {
                    //                        apply_PhaiTraCanTruPRE["new_name"] = (string)etlTransaction["new_name"];
                    //                    }

                    //                    apply_PhaiTraCanTruPRE["new_suppliersitecode"] = "Tây Ninh";

                    //                    if (khNongDan.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)khNongDan["new_phuongthucthanhtoan"]).Value == 100000001)
                    //                    {
                    //                        List<Entity> taikhoannganhang = RetrieveMultiRecord(service, "new_taikhoannganhang",
                    //                            new ColumnSet(new string[] { "new_sotaikhoan", "new_giaodichchinh" }),
                    //                            khNongDan.LogicalName == "contact" ? "new_khachhang" : "new_khachhangdoanhnghiep", khNongDan.Id);

                    //                        Entity taikhoanchinh = null;

                    //                        foreach (Entity en in taikhoannganhang)
                    //                        {
                    //                            if ((bool)en["new_giaodichchinh"] == true)
                    //                                taikhoanchinh = en;
                    //                        }

                    //                        apply_PhaiTraCanTruPRE["new_supplierbankname"] = (taikhoanchinh == null ? "CTXL-VND-0" : taikhoanchinh["new_sotaikhoan"]);
                    //                    }
                    //                    else
                    //                        apply_PhaiTraCanTruPRE["new_supplierbankname"] = "CTXL-VND-0";

                    //                    //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                    //                    apply_PhaiTraCanTruPRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienvay"]).Value * (-1));

                    //                    //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                    //                    apply_PhaiTraCanTruPRE["new_referencenumber"] = i.ToString();
                    //                    apply_PhaiTraCanTruPRE["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                    //                    apply_PhaiTraCanTruPRE["new_paymentdocumentname"] = "CANTRU_04";
                    //                    apply_PhaiTraCanTruPRE["new_vouchernumber"] = "CTND";
                    //                    apply_PhaiTraCanTruPRE["new_cashflow"] = "00.00";
                    //                    apply_PhaiTraCanTruPRE["new_paymentnum"] = "1";
                    //                    apply_PhaiTraCanTruPRE["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();
                    //                    apply_PhaiTraCanTruPRE["new_documentsequence"] = phieuDNThuNo["new_lannhan"];

                    //                    if (phieuDNThuNo.Contains("new_khachhang"))
                    //                        apply_PhaiTraCanTruPRE["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                    //                    else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                    //                        apply_PhaiTraCanTruPRE["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                    //                    apply_PhaiTraCanTruPRE.Id = service.Create(apply_PhaiTraCanTruPRE);

                    //                    apply_PhaiTraCanTruPRE["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                    //                    apply_PhaiTraCanTruPRE["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                    //                    apply_PhaiTraCanTruPRE["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                    //                    Send(apply_PhaiTraCanTruPRE);
                    //                }
                    //            }
                    //        }
                    //        #endregion
                    //        // lai
                    //        #region Lai
                    //        if (phieuTinhLai.Contains("new_tienlai"))
                    //        {
                    //            #region phat sinh Etl CRE
                    //            Entity etl_ND = new Entity("new_etltransaction");
                    //            etl_ND["new_name"] = phieuDNThuNo["new_masophieu"].ToString() + "_" + i.ToString() + "_CRE";
                    //            etl_ND["new_vouchernumber"] = "DTND";
                    //            etl_ND["new_transactiontype"] = "5.4.2.b";
                    //            etl_ND["new_customertype"] = new OptionSetValue(1);
                    //            etl_ND["new_season"] = vuThuHoach != null ? vuThuHoach["new_name"].ToString() : "";//vudautu["new_mavudautu"].ToString();
                    //            //etl_ND["new_vudautu"] = fullEntity["new_vudautu"];
                    //            etl_ND["new_sochungtu"] = phieuDNThuNo["new_masophieu"].ToString();
                    //            //etl_ND["new_lannhan"] = phieuNghiemThu["new_lannhan"];
                    //            etl_ND["new_contractnumber"] = phanBoDauTu["new_maphieuphanbo"].ToString();// lay hop dong mia tren phieu nghiem thu
                    //            etl_ND["new_tradingpartner"] = (khNongDan.LogicalName.ToLower().Trim() == "contact" ? // phieu nghiem thu
                    //                ((khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "") + "_" + (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : ""))
                    //                :
                    //                ((khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "") + "_" + (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""))
                    //                );
                    //            etl_ND["new_suppliernumber"] = khNongDan["new_makhachhang"].ToString();
                    //            etl_ND["new_suppliersite"] = "TAY NINH";
                    //            etl_ND["new_invoicedate"] = phieuDNThuNo["new_ngaythu"];
                    //            etl_ND["new_descriptionheader"] = "Cấn trừ tiền mía_vụ_" + vuThuHoach != null ? vuThuHoach["new_name"].ToString() : "";//vudautu["new_mavudautu"].ToString();
                    //            etl_ND["new_terms"] = "Tra Ngay";
                    //            etl_ND["new_taxtype"] = "";
                    //            // tong tien chi tiet thanh toan
                    //            etl_ND["new_invoiceamount"] = new Money(((Money)phieuTinhLai["new_tienlai"]).Value * (-1));
                    //            etl_ND["new_gldate"] = phieuDNThuNo["new_ngayduyet"]; // ngay duyet phieu nghiem thu
                    //            etl_ND["new_invoicetype"] = "CRE";
                    //            if (phieuDNThuNo.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)phieuDNThuNo["new_phuongthucthanhtoan"]).Value == 100000001)
                    //            {
                    //                etl_ND["new_paymenttype"] = "CK";
                    //            }
                    //            else
                    //            {
                    //                etl_ND["new_paymenttype"] = "TM";
                    //            }

                    //            if (phieuDNThuNo.Contains("new_khachhang"))
                    //            {
                    //                etl_ND["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                    //            }
                    //            else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                    //            {
                    //                etl_ND["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];
                    //            }

                    //            var etl_LaiID = service.Create(etl_ND);

                    //            etl_ND["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                    //            etl_ND["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                    //            etl_ND["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                    //            //etl_ND["new_descriptionlines"] = phieuDNThuNo["new_name"].ToString();
                    //            etl_ND["tran_type"] = "CRE";

                    //            Send(etl_ND);
                    //            #endregion

                    //            #region Phat sinh Apply 
                    //            if (phieuDNThuNo.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)phieuDNThuNo["new_phuongthucthanhtoan"]).Value == 100000001)
                    //            {
                    //                // phat sinh apply CRE Lai
                    //                Entity apply_PhaiTraCanTruCRE = new Entity("new_applytransaction");

                    //                Entity etl_Lai = service.Retrieve("new_etltransaction", etl_LaiID, new ColumnSet(new string[] { "new_name" }));
                    //                if (etl_Lai != null && etl_Lai.Contains("new_name"))
                    //                {
                    //                    apply_PhaiTraCanTruCRE["new_name"] = (string)etl_Lai["new_name"];
                    //                }

                    //                apply_PhaiTraCanTruCRE["new_suppliersitecode"] = "Tây Ninh";

                    //                if (khNongDan.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)khNongDan["new_phuongthucthanhtoan"]).Value == 100000001)
                    //                {
                    //                    List<Entity> taikhoannganhang = RetrieveMultiRecord(service, "new_taikhoannganhang",
                    //                        new ColumnSet(new string[] { "new_sotaikhoan", "new_giaodichchinh" }),
                    //                        khNongDan.LogicalName == "contact" ? "new_khachhang" : "new_khachhangdoanhnghiep", khNongDan.Id);

                    //                    Entity taikhoanchinh = null;

                    //                    foreach (Entity en in taikhoannganhang)
                    //                    {
                    //                        if ((bool)en["new_giaodichchinh"] == true)
                    //                            taikhoanchinh = en;
                    //                    }

                    //                    apply_PhaiTraCanTruCRE["new_supplierbankname"] = (taikhoanchinh == null ? "CTXL-VND-0" : taikhoanchinh["new_sotaikhoan"]);
                    //                }
                    //                else
                    //                    apply_PhaiTraCanTruCRE["new_supplierbankname"] = "CTXL-VND-0";

                    //                //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                    //                apply_PhaiTraCanTruCRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienvay"]).Value * (-1));

                    //                //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                    //                apply_PhaiTraCanTruCRE["new_referencenumber"] = i.ToString();
                    //                apply_PhaiTraCanTruCRE["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                    //                apply_PhaiTraCanTruCRE["new_paymentdocumentname"] = "CANTRU_04";
                    //                apply_PhaiTraCanTruCRE["new_vouchernumber"] = "CTND";
                    //                apply_PhaiTraCanTruCRE["new_cashflow"] = "00.00";
                    //                apply_PhaiTraCanTruCRE["new_paymentnum"] = "1";
                    //                apply_PhaiTraCanTruCRE["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();
                    //                apply_PhaiTraCanTruCRE["new_documentsequence"] = phieuDNThuNo["new_lannhan"];

                    //                if (phieuDNThuNo.Contains("new_khachhang"))
                    //                    apply_PhaiTraCanTruCRE["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                    //                else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                    //                    apply_PhaiTraCanTruCRE["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                    //                apply_PhaiTraCanTruCRE.Id = service.Create(apply_PhaiTraCanTruCRE);

                    //                apply_PhaiTraCanTruCRE["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                    //                apply_PhaiTraCanTruCRE["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                    //                apply_PhaiTraCanTruCRE["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                    //                Send(apply_PhaiTraCanTruCRE);
                    //            }
                    //            #endregion
                    //        }
                    //        #endregion
                    //    }
                    //}
                    #endregion
                }
                else if (((OptionSetValue)phieuDNThuNo["new_loaithuno"]).Value == 100000000)//Tien mat
                {
                    //if (!phieuDNThuNo.Contains("new_bank_acc"))
                    //{
                    //    throw new Exception("Phiếu đề nghị thu nợ chưa có Bank Account Name");
                    //}
                    //else
                    if (!phieuDNThuNo.Contains("new_cash_flow"))
                    {
                        throw new Exception("Phiếu đề nghị thu nợ chưa có Cash Flow");
                    }
                    else if (!phieuDNThuNo.Contains("new_voucher_num"))
                    {
                        throw new Exception("Phiếu đề nghị thu nợ chưa có Voucher Number");
                    }

                    var queryPhieuTinhLai = new QueryExpression("new_phieutinhlai");
                    queryPhieuTinhLai.Criteria = new FilterExpression();
                    queryPhieuTinhLai.Criteria.AddCondition("new_phieudenghithuno", ConditionOperator.Equal, phieuDNThuNo.Id);
                    queryPhieuTinhLai.ColumnSet = new ColumnSet(true);

                    var dsPhieuTinhLai = service.RetrieveMultiple(queryPhieuTinhLai);

                    if (dsPhieuTinhLai != null && dsPhieuTinhLai.Entities.Count > 0)
                    {
                        int i = 0;
                        foreach (var phieuTinhLai in dsPhieuTinhLai.Entities)
                        {
                            ++i;
                            Entity bankAccount = null;
                            if (phieuDNThuNo.Contains("new_bank_acc"))
                            {
                                bankAccount = service.Retrieve("new_bank_acc", ((EntityReference)phieuDNThuNo["new_bank_acc"]).Id, new ColumnSet(new string[] { "new_name" }));
                            }
                            var vouchernumber = service.Retrieve("new_voucher_num", ((EntityReference)phieuDNThuNo["new_voucher_num"]).Id, new ColumnSet(new string[] { "new_name" }));
                            var cashFlow = service.Retrieve("new_cashflow", ((EntityReference)phieuDNThuNo["new_cash_flow"]).Id, new ColumnSet(new string[] { "new_name" }));

                            #region Phat sinh Mix cho PRE và Pay Refund cho CRE
                            // Can tru
                            Entity phanBoDauTu = service.Retrieve("new_phanbodautu", ((EntityReference)phieuTinhLai["new_phanbodautu"]).Id, new ColumnSet(true));
                            if (phanBoDauTu.Contains("new_etltransaction"))
                            {
                                #region Lai
                                if (phieuTinhLai.Contains("new_tienlai") && ((Money)phieuTinhLai["new_tienlai"]).Value > 0)
                                {
                                    #region phat sinh Etl CRE
                                    Entity etl_ND = new Entity("new_etltransaction");
                                    etl_ND["new_name"] = phieuDNThuNo["new_masophieu"].ToString() + "_LAI_CRE_" + i.ToString();
                                    etl_ND["new_vouchernumber"] = "DTND";
                                    etl_ND["new_transactiontype"] = "5.4.2.b";
                                    etl_ND["new_customertype"] = new OptionSetValue(1);
                                    etl_ND["new_season"] = vuMua;//vudautu["new_mavudautu"].ToString();
                                                                 //etl_ND["new_vudautu"] = fullEntity["new_vudautu"];
                                    etl_ND["new_sochungtu"] = phieuDNThuNo["new_masophieu"].ToString();
                                    //etl_ND["new_lannhan"] = phieuNghiemThu["new_lannhan"];
                                    etl_ND["new_contractnumber"] = phanBoDauTu["new_maphieuphanbo"].ToString();// lay hop dong mia tren phieu nghiem thu
                                    etl_ND["new_tradingpartner"] = (khNongDan.LogicalName.ToLower().Trim() == "contact" ? // phieu nghiem thu
                                        ((khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "") + "_" + (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : ""))
                                        :
                                        ((khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "") + "_" + (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""))
                                        );
                                    etl_ND["new_suppliernumber"] = khNongDan["new_makhachhang"].ToString();
                                    etl_ND["new_suppliersite"] = "TAY NINH";
                                    etl_ND["new_invoicedate"] = phieuDNThuNo["new_ngaylapphieu"];
                                    etl_ND["new_descriptionheader"] = "Tiền lãi_vụ_" + vuMua;//vudautu["new_mavudautu"].ToString();
                                    etl_ND["new_terms"] = "Tra Ngay";
                                    etl_ND["new_taxtype"] = "";
                                    // tong tien chi tiet thanh toan
                                    etl_ND["new_invoiceamount"] = new Money(((Money)phieuTinhLai["new_tienlai"]).Value * (-1));
                                    etl_ND["new_gldate"] = phieuDNThuNo["new_ngaythu"]; // ngay duyet phieu nghiem thu
                                    etl_ND["new_invoicetype"] = "CRE";

                                    if (phieuDNThuNo.Contains("new_khachhang"))
                                    {
                                        etl_ND["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                    }
                                    else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                    {
                                        etl_ND["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];
                                    }

                                    var etl_LaiID = service.Create(etl_ND);

                                    etl_ND["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                                    etl_ND["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                                    etl_ND["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                                    //etl_ND["new_descriptionlines"] = phieuDNThuNo["new_name"].ToString();
                                    etl_ND["tran_type"] = "CRE";

                                    Send(etl_ND);
                                    #endregion

                                    #region Phat sinh Apply 
                                    // phat sinh apply CRE Lai
                                    Entity apply_PayRefundCRE = new Entity("new_applytransaction");

                                    apply_PayRefundCRE["new_name"] = etl_ND["new_name"];

                                    apply_PayRefundCRE["new_suppliersitecode"] = "Tây Ninh";

                                    apply_PayRefundCRE["new_bankcccountnum"] = bankAccount != null ? bankAccount["new_name"] : "";

                                    apply_PayRefundCRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienlai"]).Value * (-1));

                                    apply_PayRefundCRE["new_referencenumber"] = phieuDNThuNo["new_masophieu"].ToString() + "_" + apply_PayRefundCRE["new_name"];
                                    apply_PayRefundCRE["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                    apply_PayRefundCRE["new_paymentdocumentname"] = "CANTRU_04";
                                    apply_PayRefundCRE["new_vouchernumber"] = vouchernumber["new_name"];
                                    apply_PayRefundCRE["new_cashflow"] = cashFlow["new_name"];
                                    apply_PayRefundCRE["new_paymentnum"] = "1";
                                    apply_PayRefundCRE["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();

                                    if (phieuDNThuNo.Contains("new_khachhang"))
                                        apply_PayRefundCRE["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                    else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                        apply_PayRefundCRE["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                    apply_PayRefundCRE.Id = service.Create(apply_PayRefundCRE);

                                    apply_PayRefundCRE["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                                    apply_PayRefundCRE["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                                    apply_PayRefundCRE["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                                    apply_PayRefundCRE["new_type"] = "TYPE6";
                                    Send(apply_PayRefundCRE);

                                    #endregion
                                }
                                #endregion

                                Entity etlTransaction = service.Retrieve("new_etltransaction", ((EntityReference)phanBoDauTu["new_etltransaction"]).Id, new ColumnSet(true));
                                var sotien = ((Money)etlTransaction["new_invoiceamount"]).Value > 0 ? ((Money)etlTransaction["new_invoiceamount"]).Value * (-1) : ((Money)etlTransaction["new_invoiceamount"]).Value;
                                if (etlTransaction.Contains("new_invoicetype") && (etlTransaction["new_invoicetype"].ToString() == "PRE"))
                                {
                                    #region Phat sinh MIX
                                    Entity etlMix = new Entity("new_etltransaction");
                                    etlMix.Id = Guid.Empty;
                                    etlMix["new_name"] = phieuDNThuNo["new_masophieu"].ToString() + "_MIX_" + i.ToString();
                                    etlMix["new_transactiontype"] = "5.4.4.b";
                                    etlMix["new_descriptionheader"] = "Phiếu đề nghị thu nợ tiền mặt theo NN";
                                    etlMix["new_vouchernumber"] = "GSND";
                                    etlMix["new_terms"] = "Tra Ngay";
                                    etlMix["new_invoiceamount"] = new Money(sotien);
                                    etlMix["new_invoicetype"] = "MIX";

                                    etlMix["new_customertype"] = etlTransaction["new_customertype"];
                                    etlMix["new_season"] = vuMua;
                                    etlMix["new_sochungtu"] = etlTransaction["new_sochungtu"];
                                    etlMix["new_contractnumber"] = etlTransaction["new_contractnumber"];// lay hop dong mia tren phieu nghiem thu
                                    etlMix["new_tradingpartner"] = etlTransaction["new_tradingpartner"];
                                    etlMix["new_suppliernumber"] = etlTransaction["new_suppliernumber"];
                                    etlMix["new_suppliersite"] = "TAY NINH";
                                    etlMix["new_invoicedate"] = phieuDNThuNo["new_ngaylapphieu"];// etlTransaction["new_invoicedate"];
                                    etlMix["new_taxtype"] = "";
                                    etlMix["new_gldate"] = phieuDNThuNo["new_ngaythu"]; // ngay duyet phieu nghiem thu

                                    if (phieuDNThuNo.Contains("new_khachhang"))
                                    {
                                        etlMix["new_khachhang"] = etlTransaction["new_khachhang"];
                                    }
                                    else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                    {
                                        etlMix["new_khachhangdoanhnghiep"] = etlTransaction["new_khachhangdoanhnghiep"];
                                    }

                                    Guid etlMixId = service.Create(etlMix);

                                    etlMix["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                                    etlMix["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                                    etlMix["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                                    //etl_ND["new_descriptionlines"] = phieuDNThuNo["new_name"].ToString();
                                    etlMix["tran_type"] = "MIX";
                                    Send(etlMix);
                                    #endregion

                                    #region App PayRefund
                                    Entity apply_PayRefundMix = new Entity("new_applytransaction");
                                    apply_PayRefundMix["new_name"] = (string)etlMix["new_name"];
                                    apply_PayRefundMix["new_suppliersitecode"] = "Tây Ninh";
                                    //apply_PayRefundMix["new_bankcccountnum"] = bankAccount["new_name"];

                                    apply_PayRefundMix["new_paymentamount"] = new Money(sotien * (-1));

                                    apply_PayRefundMix["new_referencenumber"] = phieuDNThuNo["new_masophieu"] + "_" + apply_PayRefundMix["new_name"];
                                    apply_PayRefundMix["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                    apply_PayRefundMix["new_paymentdocumentname"] = "CANTRU_04";

                                    apply_PayRefundMix["new_vouchernumber"] = vouchernumber["new_name"];
                                    apply_PayRefundMix["new_cashflow"] = cashFlow["new_name"];
                                    apply_PayRefundMix["new_paymentnum"] = "1";
                                    apply_PayRefundMix["new_prepay_num"] = etlTransaction["new_name"];
                                    apply_PayRefundMix["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();

                                    if (phieuDNThuNo.Contains("new_khachhang"))
                                        apply_PayRefundMix["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                    else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                        apply_PayRefundMix["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                    apply_PayRefundMix.Id = service.Create(apply_PayRefundMix);

                                    apply_PayRefundMix["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                                    apply_PayRefundMix["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                                    apply_PayRefundMix["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                                    apply_PayRefundMix["new_type"] = "TYPE3";
                                    Send(apply_PayRefundMix);
                                    #endregion
                                }
                                else if (etlTransaction.Contains("new_invoicetype") && etlTransaction["new_invoicetype"].ToString() == "CRE")
                                {
                                    #region Apply PayRefund CRE
                                    Entity apply_PayRefundCRE = new Entity("new_applytransaction");

                                    //Entity etl_entity = service.Retrieve("new_etltransaction", phaiTraSTAID, new ColumnSet(new string[] { "new_name" }));

                                    apply_PayRefundCRE["new_name"] = etlTransaction["new_name"];
                                    apply_PayRefundCRE["new_suppliersitecode"] = "Tây Ninh";

                                    apply_PayRefundCRE["new_bankcccountnum"] = bankAccount["new_name"];

                                    //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                                    apply_PayRefundCRE["new_paymentamount"] = new Money(sotien);

                                    //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                                    apply_PayRefundCRE["new_referencenumber"] = phieuDNThuNo["new_masophieu"] + "_" + apply_PayRefundCRE["new_name"];
                                    apply_PayRefundCRE["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                    apply_PayRefundCRE["new_paymentdocumentname"] = "CANTRU_04";

                                    apply_PayRefundCRE["new_vouchernumber"] = vouchernumber["new_name"];
                                    apply_PayRefundCRE["new_cashflow"] = cashFlow["new_name"];
                                    apply_PayRefundCRE["new_paymentnum"] = "1";
                                    apply_PayRefundCRE["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();

                                    if (phieuDNThuNo.Contains("new_khachhang"))
                                        apply_PayRefundCRE["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                    else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                        apply_PayRefundCRE["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                    apply_PayRefundCRE.Id = service.Create(apply_PayRefundCRE);

                                    apply_PayRefundCRE["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                                    apply_PayRefundCRE["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                                    apply_PayRefundCRE["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                                    apply_PayRefundCRE["new_type"] = "TYPE6";
                                    Send(apply_PayRefundCRE);
                                    #endregion
                                }
                            }
                            #endregion                            
                        }
                    }
                    Send(null);
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

        public void Send(Entity tmp)
        {
            MessageQueue mq;

            if (MessageQueue.Exists(@".\Private$\DynamicCRM2Oracle_" + context.OrganizationName))
                mq = new MessageQueue(@".\Private$\DynamicCRM2Oracle_" + context.OrganizationName);
            else
                mq = MessageQueue.Create(@".\Private$\DynamicCRM2Oracle_" + context.OrganizationName);

            Message m = new Message();

            if (tmp != null)
            {
                m.Body = Serialize(tmp);
                m.Label = "invo";
            }
            else
                m.Label = "brek";
            mq.Send(m);
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
    }
}
