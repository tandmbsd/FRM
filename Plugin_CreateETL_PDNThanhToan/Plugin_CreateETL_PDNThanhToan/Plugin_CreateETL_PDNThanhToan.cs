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

namespace Plugin_CreateETL_PDNThanhToan
{
    public class Plugin_CreateETL_PDNThanhToan : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        public void Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statuscode") && (((OptionSetValue)target["statuscode"]).Value == 100000000)) // da duyet
            {
                Entity fullEntity = (Entity)context.PostEntityImages["PostImg"];
                Entity Vudautu = service.Retrieve("new_vudautu", ((EntityReference)fullEntity["new_vudautu"]).Id, new ColumnSet(new string[] { "new_mavudautu" }));
                var lsVuThuHoach = RetrieveMultiRecord(service, "new_vuthuhoach", new ColumnSet(true), "new_vudautu", ((EntityReference)fullEntity["new_vudautu"]).Id);
                Entity vuThuHoach = lsVuThuHoach.Count > 0 ? lsVuThuHoach[0] : null;
                string vuMua = "";
                if (vuThuHoach != null)
                {
                    vuMua = ((DateTime)vuThuHoach["new_tungay"]).ToString("yyyy") + "-" + ((DateTime)vuThuHoach["new_denngay"]).ToString("yyyy");
                }
                Entity KH = null;
                if (fullEntity.Contains("new_khachhang"))
                    KH = service.Retrieve("contact", ((EntityReference)fullEntity["new_khachhang"]).Id,
                        new ColumnSet(true));
                else
                    KH = service.Retrieve("account", ((EntityReference)fullEntity["new_khachhangdoanhnghiep"]).Id,
                        new ColumnSet(true));
                QueryExpression queryChiTiet = new QueryExpression("new_chitietphieudenghithanhtoan");
                queryChiTiet.Criteria.AddCondition("new_phieudenghithanhtoan", ConditionOperator.Equal, fullEntity.Id);
                queryChiTiet.ColumnSet = new ColumnSet(true);
                var dsChiTiet = service.RetrieveMultiple(queryChiTiet);

                if (!fullEntity.Contains("new_ngayduyet"))
                {
                    throw new Exception("Phiếu thanh toán chưa có ngày duyệt!");
                }
                // loai thanh toan dich vu
                if (fullEntity.Contains("new_loaithanhtoan") && ((OptionSetValue)fullEntity["new_loaithanhtoan"]).Value == 100000000)
                {
                    Entity HDDichVu = service.Retrieve("new_hopdongcungungdichvu", ((EntityReference)fullEntity["new_hopdongcungcapdichvu"]).Id, new ColumnSet(true));
                    if (dsChiTiet != null && dsChiTiet.Entities.Count > 0)
                    {
                        foreach (var chiTiet in dsChiTiet.Entities)
                        {
                            //lay phieu nghiem thu
                            if (chiTiet.Contains("new_nghiemthudichvu") && chiTiet["new_nghiemthudichvu"] != null)
                            {
                                Entity phieuNghiemThu = service.Retrieve("new_nghiemthudichvu", ((EntityReference)chiTiet["new_nghiemthudichvu"]).Id, new ColumnSet(true));
                                Entity hDDauTuMiaCT = service.Retrieve("new_hopdongdautumia", ((EntityReference)phieuNghiemThu["new_hopdongdautumia"]).Id, new ColumnSet(true));
                                if (!phieuNghiemThu.Contains("new_ngayduyet"))
                                {
                                    throw new Exception("Nghiệm thu chưa có ngày duyệt!");
                                }
                                // kiem tra neu la khach hang dau tu mia thi phat sinh, nong truong khong phat sinh CRE
                                if (phieuNghiemThu != null && phieuNghiemThu.Contains("new_khachhangdautumia"))
                                {
                                    // phat sinh nhan no
                                    #region Nhan No
                                    Entity kHNhanNo = service.Retrieve("contact", ((EntityReference)phieuNghiemThu["new_khachhangdautumia"]).Id, new ColumnSet(true));
                                    Entity etl_ND = new Entity("new_etltransaction");
                                    etl_ND["new_name"] = phieuNghiemThu["new_manghiemthu"].ToString() + "_CRE";
                                    etl_ND["new_vouchernumber"] = "DTND";
                                    etl_ND["new_transactiontype"] = "3.3.3.a";
                                    etl_ND["new_customertype"] = new OptionSetValue(1);
                                    etl_ND["new_season"] = vuMua;//Vudautu["new_mavudautu"].ToString();
                                    //etl_ND["new_vudautu"] = fullEntity["new_vudautu"];
                                    etl_ND["new_sochungtu"] = phieuNghiemThu["new_manghiemthu"].ToString();
                                    //etl_ND["new_lannhan"] =  phieuNghiemThu["new_lannhan"];
                                    etl_ND["new_contractnumber"] = hDDauTuMiaCT["new_masohopdong"].ToString();// lay hop dong mia tren phieu nghiem thu
                                    etl_ND["new_tradingpartner"] = (kHNhanNo.LogicalName.ToLower().Trim() == "contact" ? // phieu nghiem thu
                                        ((kHNhanNo.Contains("new_makhachhang") ? kHNhanNo["new_makhachhang"].ToString() : "") + "_" + (kHNhanNo.Contains("new_socmnd") ? kHNhanNo["new_socmnd"].ToString() : ""))
                                        :
                                        ((kHNhanNo.Contains("new_makhachhang") ? kHNhanNo["new_makhachhang"].ToString() : "") + "_" + (kHNhanNo.Contains("new_masothue") ? kHNhanNo["new_masothue"].ToString() : ""))
                                        );
                                    etl_ND["new_suppliernumber"] = kHNhanNo["new_makhachhang"].ToString();
                                    etl_ND["new_suppliersite"] = "TAY NINH";
                                    etl_ND["new_invoicedate"] = phieuNghiemThu["actualstart"];// lay ngay nghiem thu (ngay thuc hien)

                                    etl_ND["new_descriptionheader"] = "Thanh toán dịch vụ_vụ_" + vuMua;

                                    etl_ND["new_terms"] = "Tra Ngay";
                                    etl_ND["new_taxtype"] = "";
                                    // tong tien chi tiet thanh toan
                                    etl_ND["new_invoiceamount"] = new Money(((Money)chiTiet["new_tongtien"]).Value * (-1));
                                    etl_ND["new_gldate"] = phieuNghiemThu["new_ngayduyet"]; // ngay duyet phieu nghiem thu
                                    etl_ND["new_invoicetype"] = "CRE";

                                    if (phieuNghiemThu.Contains("new_khachhang"))
                                    {
                                        etl_ND["new_khachhang"] = phieuNghiemThu["new_khachhang"];
                                    }
                                    else if (phieuNghiemThu.Contains("new_khachhangdoanhnghiep"))
                                    {
                                        etl_ND["new_khachhangdoanhnghiep"] = phieuNghiemThu["new_khachhangdoanhnghiep"];
                                    }

                                    service.Create(etl_ND);

                                    etl_ND["new_makhachhang"] = kHNhanNo.Contains("new_makhachhang") ? kHNhanNo["new_makhachhang"].ToString() : "";
                                    etl_ND["name"] = (kHNhanNo.LogicalName.ToLower() == "contact" ? (kHNhanNo.Contains("fullname") ? kHNhanNo["fullname"].ToString() : "") : (kHNhanNo.Contains("name") ? kHNhanNo["name"].ToString() : ""));
                                    etl_ND["new_socmnd"] = (kHNhanNo.LogicalName.ToLower() == "contact" ? (kHNhanNo.Contains("new_socmnd") ? kHNhanNo["new_socmnd"].ToString() : "") : (kHNhanNo.Contains("new_masothue") ? kHNhanNo["new_masothue"].ToString() : ""));
                                    etl_ND["new_descriptionlines"] = phieuNghiemThu["subject"].ToString();
                                    etl_ND["tran_type"] = "CRE";

                                    Send(etl_ND);
                                    #endregion
                                }
                            }
                        }
                    }

                    if (fullEntity.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)fullEntity["new_phuongthucthanhtoan"]).Value == 100000001)
                    {
                        // phat sinh Ben phai tra
                        #region Ben Phai Tra
                        // phat sinh transaction STA
                        Entity etl_PhaiTraSTA = new Entity("new_etltransaction");
                        etl_PhaiTraSTA["new_name"] = fullEntity["new_masophieu"].ToString() + "_STA";
                        etl_PhaiTraSTA["new_vouchernumber"] = "DTND";
                        etl_PhaiTraSTA["new_transactiontype"] = "3.3.3.a";
                        etl_PhaiTraSTA["new_customertype"] = new OptionSetValue(1);
                        etl_PhaiTraSTA["new_season"] = vuMua;//Vudautu["new_mavudautu"].ToString();
                                                             //etl_ND["new_vudautu"] = fullEntity["new_vudautu"];
                        etl_PhaiTraSTA["new_sochungtu"] = fullEntity["new_masophieu"].ToString();
                        //etl_PhaiTraSTA["new_lannhan"] = phieuNghiemThu["new_lannhan"];
                        etl_PhaiTraSTA["new_contractnumber"] = HDDichVu["new_sohopdong"].ToString();// lay hop dong mia tren phieu phan bo dau tu
                        etl_PhaiTraSTA["new_tradingpartner"] = (KH.LogicalName.ToLower().Trim() == "contact" ? // phieu nghiem thu
                            ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : ""))
                            :
                            ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""))
                            );
                        etl_PhaiTraSTA["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                        etl_PhaiTraSTA["new_suppliersite"] = "TAY NINH";
                        etl_PhaiTraSTA["new_invoicedate"] = fullEntity["new_ngaylapphieu"];// lay ngay nghiem thu (ngay thuc hien)
                        etl_PhaiTraSTA["new_descriptionheader"] = "Thanh toán dịch vụ_vụ_" + vuMua;
                        etl_PhaiTraSTA["new_terms"] = "Tra Ngay";
                        etl_PhaiTraSTA["new_taxtype"] = "";

                        // tong tien chi tiet thanh toan
                        etl_PhaiTraSTA["new_invoiceamount"] = new Money(((Money)fullEntity["new_tongtienthanhtoan"]).Value);// tong tien de nghi thanh toan
                        etl_PhaiTraSTA["new_gldate"] = fullEntity["new_ngayduyet"]; // ngay duyet phieu nghiem thu
                        etl_PhaiTraSTA["new_invoicetype"] = "STA";
                        //etl_PhaiTraSTA["new_paymenttype"] = "CK";

                        if (fullEntity.Contains("new_khachhang"))
                        {
                            etl_PhaiTraSTA["new_khachhang"] = fullEntity["new_khachhang"];
                        }
                        else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                            etl_PhaiTraSTA["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                        var phaiTraSTAID = service.Create(etl_PhaiTraSTA);

                        etl_PhaiTraSTA["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                        etl_PhaiTraSTA["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                        etl_PhaiTraSTA["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                        etl_PhaiTraSTA["new_descriptionlines"] = fullEntity["new_name"].ToString();
                        etl_PhaiTraSTA["tran_type"] = "STA";
                        etl_PhaiTraSTA["new_type"] = "TYPE4";

                        Send(etl_PhaiTraSTA);

                        Entity apply_PhaiTraSTA = new Entity("new_applytransaction");

                        Entity etl_PhaiTra = service.Retrieve("new_etltransaction", phaiTraSTAID, new ColumnSet(true));
                        if (etl_PhaiTra != null && etl_PhaiTra.Contains("new_name"))
                        {
                            apply_PhaiTraSTA["new_name"] = (string)etl_PhaiTra["new_name"];
                        }

                        apply_PhaiTraSTA["new_suppliersitecode"] = "Tây Ninh";

                        apply_PhaiTraSTA["new_bankcccountnum"] = "CTXL-VND-0";

                        //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                        apply_PhaiTraSTA["new_paymentamount"] = new Money(((Money)fullEntity["new_tongtienthanhtoan"]).Value);

                        //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                        apply_PhaiTraSTA["new_referencenumber"] = HDDichVu["new_sohopdong"].ToString();
                        apply_PhaiTraSTA["new_paymentdate"] = fullEntity["new_ngayduyet"];
                        apply_PhaiTraSTA["new_paymentdocumentname"] = "CANTRU_03";
                        apply_PhaiTraSTA["new_vouchernumber"] = "CTND";
                        apply_PhaiTraSTA["new_cashflow"] = "00.00";
                        apply_PhaiTraSTA["new_paymentnum"] = 1;
                        apply_PhaiTraSTA["new_documentnum"] = HDDichVu["new_sohopdong"].ToString();
                        //apply_PhaiTraSTA["new_documentsequence"] = phieuNghiemThu["new_lannhan"];

                        if (fullEntity.Contains("new_khachhang"))
                            apply_PhaiTraSTA["new_khachhang"] = fullEntity["new_khachhang"];
                        else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                            apply_PhaiTraSTA["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                        apply_PhaiTraSTA.Id = service.Create(apply_PhaiTraSTA);

                        apply_PhaiTraSTA["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                        apply_PhaiTraSTA["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                        apply_PhaiTraSTA["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                        apply_PhaiTraSTA["new_type"] = "TYPE4";
                        Send(apply_PhaiTraSTA);
                        #endregion
                    }

                    #region Thu No
                    if (fullEntity.Contains("new_phieudenghithuno"))
                    {
                        Entity phieuDNThuNo = service.Retrieve("new_phieudenghithuno", ((EntityReference)fullEntity["new_phieudenghithuno"]).Id, new ColumnSet(true));

                        var queryPhieuTinhLai = new QueryExpression("new_phieutinhlai");
                        queryPhieuTinhLai.Criteria = new FilterExpression();
                        queryPhieuTinhLai.Criteria.AddCondition("new_phieudenghithuno", ConditionOperator.Equal, phieuDNThuNo.Id);
                        queryPhieuTinhLai.ColumnSet = new ColumnSet(true);

                        var dsPhieuTinhLai = service.RetrieveMultiple(queryPhieuTinhLai);

                        Entity kHPhaiTra = null;
                        if (phieuDNThuNo.Contains("new_khachhang"))
                            kHPhaiTra = service.Retrieve("contact", ((EntityReference)phieuDNThuNo["new_khachhang"]).Id,
                                new ColumnSet(true));
                        else
                            kHPhaiTra = service.Retrieve("account", ((EntityReference)phieuDNThuNo["new_khachhangdoanhnghiep"]).Id, new ColumnSet(true));

                        if (dsPhieuTinhLai != null && dsPhieuTinhLai.Entities.Count > 0)
                        {
                            int i = 0;
                            foreach (var phieuTinhLai in dsPhieuTinhLai.Entities)
                            {
                                ++i;
                                // phat sinh Apply                                 

                                #region Can tru, Phat sinh CRE hoac PRE
                                // Can tru
                                Entity phanBoDauTu = service.Retrieve("new_phanbodautu", ((EntityReference)phieuTinhLai["new_phanbodautu"]).Id, new ColumnSet(true));
                                if (phanBoDauTu.Contains("new_etltransaction"))
                                {

                                    Entity etlTransaction = service.Retrieve("new_etltransaction", ((EntityReference)phanBoDauTu["new_etltransaction"]).Id, new ColumnSet(true));
                                    if (etlTransaction.Contains("new_invoicetype") && (etlTransaction["new_invoicetype"].ToString() == "PRE" || etlTransaction["new_invoicetype"].ToString() == "CRE"))
                                    {
                                        Entity apply_PhaiTraCanTruPRE = new Entity("new_applytransaction");

                                        //Entity etl_entity = service.Retrieve("new_etltransaction", phaiTraSTAID, new ColumnSet(new string[] { "new_name" }));
                                        if (etlTransaction != null && etlTransaction.Contains("new_name"))
                                        {
                                            apply_PhaiTraCanTruPRE["new_name"] = (string)etlTransaction["new_name"];
                                        }

                                        apply_PhaiTraCanTruPRE["new_suppliersitecode"] = "Tây Ninh";

                                        apply_PhaiTraCanTruPRE["new_bankcccountnum"] = "CTXL-VND-0";

                                        //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                                        apply_PhaiTraCanTruPRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienvay"]).Value * (-1));

                                        //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                                        apply_PhaiTraCanTruPRE["new_referencenumber"] = phieuDNThuNo["new_masophieu"].ToString();
                                        apply_PhaiTraCanTruPRE["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                        apply_PhaiTraCanTruPRE["new_paymentdocumentname"] = "CANTRU_03";
                                        apply_PhaiTraCanTruPRE["new_vouchernumber"] = "CTND";
                                        apply_PhaiTraCanTruPRE["new_cashflow"] = "00.00";
                                        apply_PhaiTraCanTruPRE["new_paymentnum"] = 1;
                                        apply_PhaiTraCanTruPRE["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();
                                        //apply_PhaiTraCanTruPRE["new_documentsequence"] = phieuDNThuNo["new_lannhan"];

                                        if (phieuDNThuNo.Contains("new_khachhang"))
                                            apply_PhaiTraCanTruPRE["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                        else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                            apply_PhaiTraCanTruPRE["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                        apply_PhaiTraCanTruPRE.Id = service.Create(apply_PhaiTraCanTruPRE);

                                        apply_PhaiTraCanTruPRE["new_makhachhang"] = kHPhaiTra.Contains("new_makhachhang") ? kHPhaiTra["new_makhachhang"].ToString() : "";
                                        apply_PhaiTraCanTruPRE["name"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("fullname") ? kHPhaiTra["fullname"].ToString() : "") : (KH.Contains("name") ? kHPhaiTra["name"].ToString() : ""));
                                        apply_PhaiTraCanTruPRE["new_socmnd"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("new_socmnd") ? kHPhaiTra["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? kHPhaiTra["new_masothue"].ToString() : ""));
                                        apply_PhaiTraCanTruPRE["new_type"] = etlTransaction["new_invoicetype"].ToString() == "PRE" ? "TYPE2" : "TYPE4";

                                        Send(apply_PhaiTraCanTruPRE);
                                    }

                                }
                                #endregion
                                // lai
                                #region Lai
                                if (phieuTinhLai.Contains("new_tienlai") && ((Money)phieuTinhLai["new_tienlai"]).Value > 0)
                                {
                                    #region phat sinh Etl CRE
                                    Entity etl_ND = new Entity("new_etltransaction");
                                    etl_ND["new_name"] = phieuDNThuNo["new_masophieu"].ToString() + "_" + i.ToString() + "_CRE";
                                    etl_ND["new_vouchernumber"] = "DTND";
                                    etl_ND["new_transactiontype"] = "5.4.2.b";
                                    etl_ND["new_customertype"] = new OptionSetValue(1);
                                    etl_ND["new_season"] = vuMua;//Vudautu["new_mavudautu"].ToString();
                                    //etl_ND["new_vudautu"] = fullEntity["new_vudautu"];
                                    etl_ND["new_sochungtu"] = phieuDNThuNo["new_masophieu"].ToString();
                                    //etl_ND["new_lannhan"] = phieuNghiemThu["new_lannhan"];
                                    etl_ND["new_contractnumber"] = phanBoDauTu["new_maphieuphanbo"].ToString();// lay hop dong mia tren phieu nghiem thu
                                    etl_ND["new_tradingpartner"] = (kHPhaiTra.LogicalName.ToLower().Trim() == "contact" ? // phieu nghiem thu
                                        ((kHPhaiTra.Contains("new_makhachhang") ? kHPhaiTra["new_makhachhang"].ToString() : "") + "_" + (kHPhaiTra.Contains("new_socmnd") ? kHPhaiTra["new_socmnd"].ToString() : ""))
                                        :
                                        ((kHPhaiTra.Contains("new_makhachhang") ? kHPhaiTra["new_makhachhang"].ToString() : "") + "_" + (kHPhaiTra.Contains("new_masothue") ? kHPhaiTra["new_masothue"].ToString() : ""))
                                        );
                                    etl_ND["new_suppliernumber"] = kHPhaiTra["new_makhachhang"].ToString();
                                    etl_ND["new_suppliersite"] = "TAY NINH";
                                    etl_ND["new_invoicedate"] = phieuDNThuNo["new_ngaythu"];// lay ngay nghiem thu (ngay thuc hien)
                                    etl_ND["new_descriptionheader"] = "Tiền lãi_vụ_" + vuMua;
                                    etl_ND["new_terms"] = "Tra Ngay";
                                    etl_ND["new_taxtype"] = "";
                                    // tong tien chi tiet thanh toan
                                    etl_ND["new_invoiceamount"] = new Money(((Money)phieuTinhLai["new_tienlai"]).Value * (-1));
                                    etl_ND["new_gldate"] = phieuDNThuNo["new_ngaythu"]; // ngay duyet phieu nghiem thu
                                    etl_ND["new_invoicetype"] = "CRE";
                                    //if (phieuDNThuNo.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)phieuDNThuNo["new_phuongthucthanhtoan"]).Value == 100000001)
                                    //{
                                    //    etl_ND["new_paymenttype"] = "CK";
                                    //}else
                                    //{
                                    //    etl_ND["new_paymenttype"] = "TM";
                                    //}

                                    if (phieuDNThuNo.Contains("new_khachhang"))
                                    {
                                        etl_ND["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                    }
                                    else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                    {
                                        etl_ND["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];
                                    }

                                    var etl_LaiID = service.Create(etl_ND);

                                    etl_ND["new_makhachhang"] = kHPhaiTra.Contains("new_makhachhang") ? kHPhaiTra["new_makhachhang"].ToString() : "";
                                    etl_ND["name"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("fullname") ? kHPhaiTra["fullname"].ToString() : "") : (kHPhaiTra.Contains("name") ? kHPhaiTra["name"].ToString() : ""));
                                    etl_ND["new_socmnd"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("new_socmnd") ? kHPhaiTra["new_socmnd"].ToString() : "") : (kHPhaiTra.Contains("new_masothue") ? kHPhaiTra["new_masothue"].ToString() : ""));
                                    etl_ND["new_descriptionlines"] = phieuDNThuNo["new_name"].ToString();
                                    etl_ND["tran_type"] = "CRE";

                                    Send(etl_ND);
                                    #endregion

                                    #region Phat sinh Apply 

                                    // phat sinh apply CRE Lai
                                    Entity apply_PhaiTraCanTruCRE = new Entity("new_applytransaction");

                                    Entity etl_Lai = service.Retrieve("new_etltransaction", etl_LaiID, new ColumnSet(new string[] { "new_name" }));
                                    if (etl_Lai != null && etl_Lai.Contains("new_name"))
                                    {
                                        apply_PhaiTraCanTruCRE["new_name"] = (string)etl_Lai["new_name"];
                                    }

                                    apply_PhaiTraCanTruCRE["new_suppliersitecode"] = "Tây Ninh";

                                    apply_PhaiTraCanTruCRE["new_bankcccountnum"] = "CTXL-VND-0";

                                    //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                                    apply_PhaiTraCanTruCRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienlai"]).Value * (-1));

                                    //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                                    apply_PhaiTraCanTruCRE["new_referencenumber"] = phieuDNThuNo["new_masophieu"].ToString();
                                    apply_PhaiTraCanTruCRE["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                    apply_PhaiTraCanTruCRE["new_paymentdocumentname"] = "CANTRU_03";
                                    apply_PhaiTraCanTruCRE["new_vouchernumber"] = "CTND";
                                    apply_PhaiTraCanTruCRE["new_cashflow"] = "00.00";
                                    apply_PhaiTraCanTruCRE["new_paymentnum"] = 1;
                                    apply_PhaiTraCanTruCRE["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();
                                    //apply_PhaiTraCanTruCRE["new_documentsequence"] = phieuDNThuNo["new_lannhan"];

                                    if (phieuDNThuNo.Contains("new_khachhang"))
                                        apply_PhaiTraCanTruCRE["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                    else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                        apply_PhaiTraCanTruCRE["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                    apply_PhaiTraCanTruCRE.Id = service.Create(apply_PhaiTraCanTruCRE);

                                    apply_PhaiTraCanTruCRE["new_makhachhang"] = kHPhaiTra.Contains("new_makhachhang") ? kHPhaiTra["new_makhachhang"].ToString() : "";
                                    apply_PhaiTraCanTruCRE["name"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("fullname") ? kHPhaiTra["fullname"].ToString() : "") : (kHPhaiTra.Contains("name") ? kHPhaiTra["name"].ToString() : ""));
                                    apply_PhaiTraCanTruCRE["new_socmnd"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("new_socmnd") ? kHPhaiTra["new_socmnd"].ToString() : "") : (kHPhaiTra.Contains("new_masothue") ? kHPhaiTra["new_masothue"].ToString() : ""));
                                    apply_PhaiTraCanTruCRE["new_type"] = "TYPE4";
                                    Send(apply_PhaiTraCanTruCRE);

                                    #endregion
                                }
                                #endregion
                            }
                        }
                    }
                    #endregion

                }
                else if (fullEntity.Contains("new_loaithanhtoan") && ((OptionSetValue)fullEntity["new_loaithanhtoan"]).Value == 100000003)// hom giong
                {
                    Entity HDMia = service.Retrieve("new_hopdongdautumia", ((EntityReference)fullEntity["new_hopdongdautumia"]).Id, new ColumnSet(true));
                    #region Ben Phai Tra Hom Giong
                    // phat sinh transaction STA
                    Entity etl_PhaiTraSTA = new Entity("new_etltransaction");
                    etl_PhaiTraSTA["new_name"] = fullEntity["new_masophieu"].ToString() + "_STA";
                    etl_PhaiTraSTA["new_vouchernumber"] = "DTND";
                    etl_PhaiTraSTA["new_transactiontype"] = "3.3.3.a";
                    etl_PhaiTraSTA["new_customertype"] = new OptionSetValue(1);
                    etl_PhaiTraSTA["new_season"] = vuMua;//Vudautu["new_mavudautu"].ToString();
                    //etl_ND["new_vudautu"] = fullEntity["new_vudautu"];
                    etl_PhaiTraSTA["new_sochungtu"] = fullEntity["new_masophieu"].ToString();
                    //etl_PhaiTraSTA["new_lannhan"] = phieuNghiemThu["new_lannhan"];
                    etl_PhaiTraSTA["new_contractnumber"] = HDMia["new_masohopdong"].ToString();// lay hop dong mia tren phieu phan bo dau tu
                    etl_PhaiTraSTA["new_tradingpartner"] = (KH.LogicalName.ToLower().Trim() == "contact" ? // phieu nghiem thu
                        ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : ""))
                        :
                        ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""))
                        );

                    etl_PhaiTraSTA["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                    etl_PhaiTraSTA["new_suppliersite"] = "TAY NINH";
                    etl_PhaiTraSTA["new_invoicedate"] = fullEntity["new_ngaylapphieu"];// lay ngay nghiem thu (ngay thuc hien)
                    etl_PhaiTraSTA["new_descriptionheader"] = "Thanh toán hom giống_vụ_" + vuMua;
                    etl_PhaiTraSTA["new_terms"] = "Tra Ngay";
                    etl_PhaiTraSTA["new_taxtype"] = "";
                    // tong tien chi tiet thanh toan
                    etl_PhaiTraSTA["new_invoiceamount"] = new Money(((Money)fullEntity["new_tongtienthanhtoan"]).Value);// tong tien de nghi thanh toan
                    etl_PhaiTraSTA["new_gldate"] = fullEntity["new_ngayduyet"]; // ngay duyet phieu nghiem thu
                    etl_PhaiTraSTA["new_invoicetype"] = "STA";
                    if (fullEntity.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)fullEntity["new_phuongthucthanhtoan"]).Value == 100000001)
                    {
                        etl_PhaiTraSTA["new_paymenttype"] = "CK";
                    }
                    else
                    {
                        etl_PhaiTraSTA["new_paymenttype"] = "TM";
                    }

                    if (fullEntity.Contains("new_khachhang"))
                    {
                        etl_PhaiTraSTA["new_khachhang"] = fullEntity["new_khachhang"];
                    }
                    else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                        etl_PhaiTraSTA["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                    var phaiTraSTAID = service.Create(etl_PhaiTraSTA);

                    etl_PhaiTraSTA["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                    etl_PhaiTraSTA["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                    etl_PhaiTraSTA["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                    etl_PhaiTraSTA["new_descriptionlines"] = fullEntity["new_name"].ToString();
                    etl_PhaiTraSTA["tran_type"] = "STA";

                    Send(etl_PhaiTraSTA);

                    if (fullEntity.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)fullEntity["new_phuongthucthanhtoan"]).Value == 100000001)
                    {
                        Entity apply_STA = new Entity("new_applytransaction");
                        //apply_PGNPhanbon["new_documentsequence"] = value++;
                        apply_STA["new_suppliersitecode"] = "Tây Ninh";

                        if (fullEntity.Contains("new_taikhoan"))
                        {
                            var taiKhoan = service.Retrieve("new_taikhoannganhang", ((EntityReference)fullEntity["new_taikhoan"]).Id, new ColumnSet(true));
                            apply_STA["new_bankcccountnum"] = taiKhoan["new_sotaikhoan"];
                        }

                        if (fullEntity.Contains("new_taikhoannganhangttcs"))
                        {
                            var taiKhoan = service.Retrieve("new_taikhoannganhangcuattcs", ((EntityReference)fullEntity["new_taikhoannganhangttcs"]).Id, new ColumnSet(true));
                            apply_STA["new_supplierbankname"] = taiKhoan["new_name"];
                        }
                        //}

                        Entity etl_entityBenPhaiTraSTA = service.Retrieve("new_etltransaction", phaiTraSTAID, new ColumnSet(new string[] { "new_name" }));
                        if (etl_PhaiTraSTA != null && etl_PhaiTraSTA.Contains("new_name"))
                        {
                            apply_STA["new_name"] = (string)etl_PhaiTraSTA["new_name"];
                        }
                        //apply_PGNhomgiong_STA["new_name"] = "new_phieugiaonhanhomgiong";
                        apply_STA["new_paymentamount"] = new Money(((Money)fullEntity["new_tongtienthanhtoan"]).Value);
                        apply_STA["new_suppliernumber"] = KH["new_makhachhang"];
                        apply_STA["new_paymentdate"] = fullEntity["new_ngayduyet"];
                        apply_STA["new_paymentdocumentname"] = "CANTRU_03";
                        apply_STA["new_vouchernumber"] = "CTND";
                        apply_STA["new_cashflow"] = "00.00";
                        apply_STA["new_referencenumber"] = fullEntity["new_masophieu"].ToString();
                        apply_STA["new_paymentnum"] = 1;
                        apply_STA["new_documentnum"] = fullEntity["new_masophieu"].ToString();

                        if (fullEntity.Contains("new_khachhang"))
                            apply_STA["new_khachhang"] = fullEntity["new_khachhang"];
                        else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                            apply_STA["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                        apply_STA.Id = service.Create(apply_STA);

                        apply_STA["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                        apply_STA["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                        apply_STA["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                        apply_STA["new_type"] = "TYPE4";
                        Send(apply_STA);
                    }

                    #region Thu No
                    if (fullEntity.Contains("new_phieudenghithuno"))
                    {
                        Entity phieuDNThuNo = service.Retrieve("new_phieudenghithuno", ((EntityReference)fullEntity["new_phieudenghithuno"]).Id, new ColumnSet(true));

                        var queryPhieuTinhLai = new QueryExpression("new_phieutinhlai");
                        queryPhieuTinhLai.Criteria = new FilterExpression();
                        queryPhieuTinhLai.Criteria.AddCondition("new_phieudenghithuno", ConditionOperator.Equal, phieuDNThuNo.Id);
                        queryPhieuTinhLai.ColumnSet = new ColumnSet(true);

                        var dsPhieuTinhLai = service.RetrieveMultiple(queryPhieuTinhLai);

                        Entity kHPhaiTra = null;
                        if (phieuDNThuNo.Contains("new_khachhang"))
                            kHPhaiTra = service.Retrieve("contact", ((EntityReference)phieuDNThuNo["new_khachhang"]).Id,
                                new ColumnSet(true));
                        else
                            kHPhaiTra = service.Retrieve("account", ((EntityReference)phieuDNThuNo["new_khachhangdoanhnghiep"]).Id, new ColumnSet(true));


                        if (dsPhieuTinhLai != null && dsPhieuTinhLai.Entities.Count > 0)
                        {
                            int i = 0;
                            foreach (var phieuTinhLai in dsPhieuTinhLai.Entities)
                            {
                                ++i;
                                // phat sinh Apply 
                                #region Phat sint etl STA Theo phieu tinh lai
                                //Entity apply_PhaiTraSTA = new Entity("new_applytransaction");

                                //Entity etl_entity = service.Retrieve("new_etltransaction", phaiTraSTAID, new ColumnSet(new string[] { "new_name" }));
                                //if (etl_entity != null && etl_entity.Contains("new_name"))
                                //{
                                //    apply_PhaiTraSTA["new_name"] = (string)etl_entity["new_name"];
                                //}

                                //apply_PhaiTraSTA["new_suppliersitecode"] = "Tây Ninh";

                                //if (kHPhaiTra.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)kHPhaiTra["new_phuongthucthanhtoan"]).Value == 100000001)
                                //{
                                //    List<Entity> taikhoannganhang = RetrieveMultiRecord(service, "new_taikhoannganhang",
                                //        new ColumnSet(new string[] { "new_sotaikhoan", "new_giaodichchinh" }),
                                //        kHPhaiTra.LogicalName == "contact" ? "new_khachhang" : "new_khachhangdoanhnghiep", kHPhaiTra.Id);

                                //    Entity taikhoanchinh = null;

                                //    foreach (Entity en in taikhoannganhang)
                                //    {
                                //        if ((bool)en["new_giaodichchinh"] == true)
                                //            taikhoanchinh = en;
                                //    }

                                //    apply_PhaiTraSTA["new_supplierbankname"] = (taikhoanchinh == null ? "CTXL-VND-0" : taikhoanchinh["new_sotaikhoan"]);
                                //}
                                //else
                                //    apply_PhaiTraSTA["new_supplierbankname"] = "CTXL-VND-0";

                                ////apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                                //apply_PhaiTraSTA["new_referencenumber"] = i.ToString();
                                //apply_PhaiTraSTA["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienvay"]).Value);
                                ////apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                                //apply_PhaiTraSTA["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                //apply_PhaiTraSTA["new_paymentdocumentname"] = "CANTRU_03";
                                //apply_PhaiTraSTA["new_vouchernumber"] = "CTND";
                                //apply_PhaiTraSTA["new_cashflow"] = "00.00";
                                //apply_PhaiTraSTA["new_paymentnum"] = 1;
                                //apply_PhaiTraSTA["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();
                                //apply_PhaiTraSTA["new_documentsequence"] = "";// phieuDNThuNo["new_lannhan"];

                                //if (phieuDNThuNo.Contains("new_khachhang"))
                                //    apply_PhaiTraSTA["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                //else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                //    apply_PhaiTraSTA["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                //apply_PhaiTraSTA.Id = service.Create(apply_PhaiTraSTA);

                                //apply_PhaiTraSTA["new_makhachhang"] = kHPhaiTra.Contains("new_makhachhang") ? kHPhaiTra["new_makhachhang"].ToString() : "";
                                //apply_PhaiTraSTA["name"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("fullname") ? kHPhaiTra["fullname"].ToString() : "") : (kHPhaiTra.Contains("name") ? kHPhaiTra["name"].ToString() : ""));
                                //apply_PhaiTraSTA["new_socmnd"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("new_socmnd") ? kHPhaiTra["new_socmnd"].ToString() : "") : (kHPhaiTra.Contains("new_masothue") ? kHPhaiTra["new_masothue"].ToString() : ""));
                                //Send(apply_PhaiTraSTA);
                                #endregion

                                #region Can tru, Phat sinh CRE hoac PRE
                                // Can tru
                                Entity phanBoDauTu = service.Retrieve("new_phanbodautu", ((EntityReference)phieuTinhLai["new_phanbodautu"]).Id, new ColumnSet(true));

                                if (phanBoDauTu.Contains("new_etltransaction"))
                                {
                                    Entity etlTransaction = service.Retrieve("new_etltransaction", ((EntityReference)phanBoDauTu["new_etltransaction"]).Id, new ColumnSet(true));
                                    if (etlTransaction.Contains("new_invoicetype") && (etlTransaction["new_invoicetype"].ToString() == "PRE" || etlTransaction["new_invoicetype"].ToString() == "CRE"))
                                    {
                                        Entity apply_PhaiTraCanTruPRE = new Entity("new_applytransaction");

                                        //Entity etl_entity = service.Retrieve("new_etltransaction", phaiTraSTAID, new ColumnSet(new string[] { "new_name" }));
                                        if (etlTransaction != null && etlTransaction.Contains("new_name"))
                                        {
                                            apply_PhaiTraCanTruPRE["new_name"] = (string)etlTransaction["new_name"];
                                        }

                                        apply_PhaiTraCanTruPRE["new_suppliersitecode"] = "Tây Ninh";

                                        apply_PhaiTraCanTruPRE["new_bankcccountnum"] = "CTXL-VND-0";

                                        //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                                        apply_PhaiTraCanTruPRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienvay"]).Value * (-1));

                                        //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                                        apply_PhaiTraCanTruPRE["new_referencenumber"] = phieuDNThuNo["new_masophieu"].ToString();
                                        apply_PhaiTraCanTruPRE["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                        apply_PhaiTraCanTruPRE["new_paymentdocumentname"] = "CANTRU_03";
                                        apply_PhaiTraCanTruPRE["new_vouchernumber"] = "CTND";
                                        apply_PhaiTraCanTruPRE["new_cashflow"] = "00.00";
                                        apply_PhaiTraCanTruPRE["new_paymentnum"] = 1;
                                        apply_PhaiTraCanTruPRE["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();
                                        //apply_PhaiTraCanTruPRE["new_documentsequence"] = phieuDNThuNo["new_lannhan"];

                                        if (phieuDNThuNo.Contains("new_khachhang"))
                                            apply_PhaiTraCanTruPRE["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                        else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                            apply_PhaiTraCanTruPRE["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                        apply_PhaiTraCanTruPRE.Id = service.Create(apply_PhaiTraCanTruPRE);

                                        apply_PhaiTraCanTruPRE["new_makhachhang"] = kHPhaiTra.Contains("new_makhachhang") ? kHPhaiTra["new_makhachhang"].ToString() : "";
                                        apply_PhaiTraCanTruPRE["name"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("fullname") ? kHPhaiTra["fullname"].ToString() : "") : (kHPhaiTra.Contains("name") ? kHPhaiTra["name"].ToString() : ""));
                                        apply_PhaiTraCanTruPRE["new_socmnd"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("new_socmnd") ? kHPhaiTra["new_socmnd"].ToString() : "") : (kHPhaiTra.Contains("new_masothue") ? kHPhaiTra["new_masothue"].ToString() : ""));
                                        apply_PhaiTraCanTruPRE["new_type"] = etlTransaction["new_invoicetype"].ToString() == "PRE" ? "TYPE2" : "TYPE4";
                                        Send(apply_PhaiTraCanTruPRE);
                                    }
                                }
                                #endregion
                                // lai
                                #region Lai

                                if (phieuTinhLai.Contains("new_tienlai") && ((Money)phieuTinhLai["new_tienlai"]).Value > 0)
                                {
                                    #region phat sinh Etl CRE
                                    Entity etl_ND = new Entity("new_etltransaction");
                                    etl_ND["new_name"] = phieuDNThuNo["new_masophieu"].ToString() + "_" + i.ToString() + "_CRE";
                                    etl_ND["new_vouchernumber"] = "DTND";
                                    etl_ND["new_transactiontype"] = "5.4.2.b";
                                    etl_ND["new_customertype"] = new OptionSetValue(1);
                                    etl_ND["new_season"] = vuMua;//Vudautu["new_mavudautu"].ToString();
                                    //etl_ND["new_vudautu"] = fullEntity["new_vudautu"];
                                    etl_ND["new_sochungtu"] = phieuDNThuNo["new_masophieu"].ToString();
                                    //etl_ND["new_lannhan"] = phieuNghiemThu["new_lannhan"];
                                    etl_ND["new_contractnumber"] = phanBoDauTu["new_maphieuphanbo"].ToString();// lay hop dong mia tren phieu nghiem thu
                                    etl_ND["new_tradingpartner"] = (kHPhaiTra.LogicalName.ToLower().Trim() == "contact" ? // phieu nghiem thu
                                        ((kHPhaiTra.Contains("new_makhachhang") ? kHPhaiTra["new_makhachhang"].ToString() : "") + "_" + (kHPhaiTra.Contains("new_socmnd") ? kHPhaiTra["new_socmnd"].ToString() : ""))
                                        :
                                        ((kHPhaiTra.Contains("new_makhachhang") ? kHPhaiTra["new_makhachhang"].ToString() : "") + "_" + (kHPhaiTra.Contains("new_masothue") ? kHPhaiTra["new_masothue"].ToString() : ""))
                                        );
                                    etl_ND["new_suppliernumber"] = kHPhaiTra["new_makhachhang"].ToString();
                                    etl_ND["new_suppliersite"] = "TAY NINH";
                                    etl_ND["new_invoicedate"] = phieuDNThuNo["new_ngaythu"];// lay ngay nghiem thu (ngay thuc hien)
                                    etl_ND["new_descriptionheader"] = "Thu lãi_vụ_" + vuMua;
                                    etl_ND["new_terms"] = "Tra Ngay";
                                    etl_ND["new_taxtype"] = "";
                                    // tong tien chi tiet thanh toan
                                    etl_ND["new_invoiceamount"] = new Money(((Money)phieuTinhLai["new_tienlai"]).Value * (-1));
                                    etl_ND["new_gldate"] = phieuDNThuNo["new_ngaythu"]; // ngay duyet phieu nghiem thu
                                    etl_ND["new_invoicetype"] = "CRE";
                                    //if (phieuDNThuNo.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)phieuDNThuNo["new_phuongthucthanhtoan"]).Value == 100000001)
                                    //{
                                    //    etl_ND["new_paymenttype"] = "CK";
                                    //}else
                                    //{
                                    //    etl_ND["new_paymenttype"] = "TM";
                                    //}

                                    if (phieuDNThuNo.Contains("new_khachhang"))
                                    {
                                        etl_ND["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                    }
                                    else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                    {
                                        etl_ND["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];
                                    }

                                    var etl_LaiID = service.Create(etl_ND);

                                    etl_ND["new_makhachhang"] = kHPhaiTra.Contains("new_makhachhang") ? kHPhaiTra["new_makhachhang"].ToString() : "";
                                    etl_ND["name"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("fullname") ? kHPhaiTra["fullname"].ToString() : "") : (kHPhaiTra.Contains("name") ? kHPhaiTra["name"].ToString() : ""));
                                    etl_ND["new_socmnd"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("new_socmnd") ? kHPhaiTra["new_socmnd"].ToString() : "") : (kHPhaiTra.Contains("new_masothue") ? kHPhaiTra["new_masothue"].ToString() : ""));
                                    etl_ND["new_descriptionlines"] = phieuDNThuNo["new_name"].ToString();
                                    etl_ND["tran_type"] = "CRE";

                                    Send(etl_ND);
                                    #endregion

                                    #region Phat sinh Apply 

                                    // phat sinh apply CRE Lai
                                    Entity apply_PhaiTraCanTruCRE = new Entity("new_applytransaction");

                                    Entity etl_Lai = service.Retrieve("new_etltransaction", etl_LaiID, new ColumnSet(new string[] { "new_name" }));
                                    if (etl_Lai != null && etl_Lai.Contains("new_name"))
                                    {
                                        apply_PhaiTraCanTruCRE["new_name"] = (string)etl_Lai["new_name"];
                                    }

                                    apply_PhaiTraCanTruCRE["new_suppliersitecode"] = "Tây Ninh";

                                    apply_PhaiTraCanTruCRE["new_bankcccountnum"] = "CTXL-VND-0";

                                    //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                                    apply_PhaiTraCanTruCRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienlai"]).Value * (-1));

                                    //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                                    apply_PhaiTraCanTruCRE["new_referencenumber"] = phieuDNThuNo["new_masophieu"].ToString();
                                    apply_PhaiTraCanTruCRE["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                    apply_PhaiTraCanTruCRE["new_paymentdocumentname"] = "CANTRU_03";
                                    apply_PhaiTraCanTruCRE["new_vouchernumber"] = "CTND";
                                    apply_PhaiTraCanTruCRE["new_cashflow"] = "00.00";
                                    apply_PhaiTraCanTruCRE["new_paymentnum"] = 1;
                                    apply_PhaiTraCanTruCRE["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();
                                    //apply_PhaiTraCanTruCRE["new_documentsequence"] = phieuDNThuNo["new_lannhan"];

                                    if (phieuDNThuNo.Contains("new_khachhang"))
                                        apply_PhaiTraCanTruCRE["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                    else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                        apply_PhaiTraCanTruCRE["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                    apply_PhaiTraCanTruCRE.Id = service.Create(apply_PhaiTraCanTruCRE);

                                    apply_PhaiTraCanTruCRE["new_makhachhang"] = kHPhaiTra.Contains("new_makhachhang") ? kHPhaiTra["new_makhachhang"].ToString() : "";
                                    apply_PhaiTraCanTruCRE["name"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("fullname") ? kHPhaiTra["fullname"].ToString() : "") : (kHPhaiTra.Contains("name") ? kHPhaiTra["name"].ToString() : ""));
                                    apply_PhaiTraCanTruCRE["new_socmnd"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("new_socmnd") ? kHPhaiTra["new_socmnd"].ToString() : "") : (kHPhaiTra.Contains("new_masothue") ? kHPhaiTra["new_masothue"].ToString() : ""));
                                    apply_PhaiTraCanTruCRE["new_type"] = "TYPE4";
                                    Send(apply_PhaiTraCanTruCRE);

                                    #endregion
                                }
                                #endregion
                            }
                        }
                    }
                    #endregion

                    #endregion
                }
                else if (fullEntity.Contains("new_loaithanhtoan") && ((OptionSetValue)fullEntity["new_loaithanhtoan"]).Value == 100000001)// ha tang
                {
                    // - Lay chi tiet phieu de nghi thanh toan
                    //      Phieu nghiem thu cong trinh cho tung chi tiet de nghi thanh toan
                    //      Tinh ti le huong loi cua nong dan dua vao hop dong
                    //      Tinh so tien ND chiu theo ti le huong loi dua vao tong tien tren phieu nghiem thu
                    //      Phat sinh no cho tung ND
                    var dsCTDNThanhToan = RetrieveMultiRecord(service, "new_chitietphieudenghithanhtoan", new ColumnSet(true), "new_phieudenghithanhtoan", fullEntity.Id);
                    var hdHaTang = service.Retrieve("new_hopdongdautuhatang", ((EntityReference)fullEntity["new_hopdongdautuhatang"]).Id, new ColumnSet(true));

                    if (!hdHaTang.Contains("new_ngayduyet"))
                    {
                        throw new Exception("Hợp đồng hạ tầng chưa có ngày duyệt!");
                    }

                    decimal soTienNDChiu = 0;
                    if (hdHaTang.Contains("new_sotiennongdanchiu"))
                    {
                        soTienNDChiu = ((Money)hdHaTang["new_sotiennongdanchiu"]).Value;
                    }
                    var dsCTHaTang = RetrieveMultiRecord(service, "new_chitietgopdongdautuhatang", new ColumnSet(true), "new_hopdongdautuhatang", hdHaTang.Id);

                    var tongSoTienThanhToan = ((Money)fullEntity["new_tongtienthanhtoan"]).Value;
                    decimal soTienDiaPhuongHoTro = 0;
                    if (hdHaTang.Contains("new_sotienbenkhachotro"))
                    {
                        soTienDiaPhuongHoTro = ((Money)hdHaTang["new_sotienbenkhachotro"]).Value;
                    }
                    decimal soTienNhaMayHoTro = 0;
                    decimal giaTriHopDong = ((Money)hdHaTang["new_giatrihopdong"]).Value;

                    if (hdHaTang.Contains("new_sotienhotro") && ((Money)hdHaTang["new_sotienhotro"]).Value > 0)
                    {
                        soTienNhaMayHoTro = ((Money)hdHaTang["new_sotienhotro"]).Value;
                        decimal thanhTien = soTienNhaMayHoTro * (tongSoTienThanhToan / (giaTriHopDong - soTienDiaPhuongHoTro));

                        #region Phat Sinh STA
                        Entity etl_NDSTA = new Entity("new_etltransaction");
                        etl_NDSTA["new_name"] = hdHaTang["new_mahopdong"].ToString() + "_STA";
                        etl_NDSTA["new_vouchernumber"] = "DTND";
                        etl_NDSTA["new_transactiontype"] = "8.3.3.f";
                        etl_NDSTA["new_customertype"] = new OptionSetValue(1);
                        etl_NDSTA["new_season"] = vuMua;//Vudautu["new_mavudautu"].ToString();
                        //etl_ND["new_vudautu"] = fullEntity["new_vudautu"];
                        etl_NDSTA["new_sochungtu"] = fullEntity["new_masophieu"].ToString();
                        //etl_ND["new_lannhan"] =  phieuNghiemThu["new_lannhan"];
                        etl_NDSTA["new_contractnumber"] = hdHaTang["new_mahopdong"].ToString();// lay hop dong mia tren phieu nghiem thu
                        etl_NDSTA["new_tradingpartner"] = (KH.LogicalName.ToLower().Trim() == "contact" ? // phieu nghiem thu
                            ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : ""))
                            :
                            ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""))
                            );
                        etl_NDSTA["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                        etl_NDSTA["new_suppliersite"] = "TAY NINH";
                        etl_NDSTA["new_invoicedate"] = hdHaTang["new_ngaykyhopdong"];// lay ngay nghiem thu (ngay thuc hien)
                        etl_NDSTA["new_descriptionheader"] = "Trả tiền cho nhà thầu công ty hỗ trợ";
                        etl_NDSTA["new_terms"] = "Tra Ngay";
                        etl_NDSTA["new_taxtype"] = "";
                        // tong tien chi tiet thanh toan
                        etl_NDSTA["new_invoiceamount"] = new Money(thanhTien);
                        etl_NDSTA["new_gldate"] = hdHaTang["new_ngayduyet"]; // ngay duyet phieu nghiem thu
                        etl_NDSTA["new_invoicetype"] = "STA";
                        if (hdHaTang.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)hdHaTang["new_phuongthucthanhtoan"]).Value == 100000000)
                        {
                            etl_NDSTA["new_paymenttype"] = "TM";
                        }
                        else if (hdHaTang.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)hdHaTang["new_phuongthucthanhtoan"]).Value == 100000001)
                        {
                            etl_NDSTA["new_paymenttype"] = "CK";
                        }

                        if (hdHaTang.Contains("new_donvithicongkhachhang"))
                        {
                            etl_NDSTA["new_khachhang"] = hdHaTang["new_donvithicongkhachhang"];
                        }
                        else if (hdHaTang.Contains("new_donvithicongkhdn"))
                        {
                            etl_NDSTA["new_khachhangdoanhnghiep"] = hdHaTang["new_donvithicongkhdn"];
                        }

                        var staIdHoTro = service.Create(etl_NDSTA);

                        etl_NDSTA["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                        etl_NDSTA["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                        etl_NDSTA["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                        etl_NDSTA["new_descriptionlines"] = hdHaTang["new_name"].ToString();
                        etl_NDSTA["tran_type"] = "STA";

                        Send(etl_NDSTA);

                        if (fullEntity.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)fullEntity["new_phuongthucthanhtoan"]).Value == 100000001)
                        {
                            Entity apply_PhaiTraSTA = new Entity("new_applytransaction");

                            Entity etl_PhaiTra = service.Retrieve("new_etltransaction", staIdHoTro, new ColumnSet(true));
                            if (etl_PhaiTra != null && etl_PhaiTra.Contains("new_name"))
                            {
                                apply_PhaiTraSTA["new_name"] = (string)etl_PhaiTra["new_name"];
                            }

                            apply_PhaiTraSTA["new_suppliersitecode"] = "Tây Ninh";

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

                            //    apply_PhaiTraSTA["new_supplierbankname"] = (taikhoanchinh == null ? "CTXL-VND-0" : taikhoanchinh["new_sotaikhoan"]);
                            //}
                            //else
                            apply_PhaiTraSTA["new_bankcccountnum"] = "CTXL-VND-0";

                            //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                            apply_PhaiTraSTA["new_paymentamount"] = new Money(thanhTien);

                            //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                            apply_PhaiTraSTA["new_referencenumber"] = hdHaTang["new_mahopdong"].ToString();
                            apply_PhaiTraSTA["new_paymentdate"] = hdHaTang["new_ngayduyet"];
                            apply_PhaiTraSTA["new_paymentdocumentname"] = "CANTRU_03";
                            apply_PhaiTraSTA["new_vouchernumber"] = "CTND";
                            apply_PhaiTraSTA["new_cashflow"] = "00.00";
                            apply_PhaiTraSTA["new_paymentnum"] = 1;
                            apply_PhaiTraSTA["new_documentnum"] = hdHaTang["new_mahopdong"].ToString();
                            //apply_PhaiTraSTA["new_documentsequence"] = phieuNghiemThu["new_lannhan"];

                            if (fullEntity.Contains("new_khachhang"))
                                apply_PhaiTraSTA["new_khachhang"] = fullEntity["new_khachhang"];
                            else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                                apply_PhaiTraSTA["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                            apply_PhaiTraSTA.Id = service.Create(apply_PhaiTraSTA);

                            apply_PhaiTraSTA["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                            apply_PhaiTraSTA["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                            apply_PhaiTraSTA["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                            apply_PhaiTraSTA["new_type"] = "TYPE4";
                            Send(apply_PhaiTraSTA);
                        }
                        #endregion
                    }

                    // STA Tong Tien Thanh Toan
                    #region STA Tong tien thanh toan
                    Entity etl_TongTienSTA = new Entity("new_etltransaction");
                    etl_TongTienSTA["new_name"] = fullEntity["new_masophieu"].ToString() + "_STA";
                    etl_TongTienSTA["new_vouchernumber"] = "DTND";
                    etl_TongTienSTA["new_transactiontype"] = "8.3.3.h";
                    etl_TongTienSTA["new_customertype"] = new OptionSetValue(1);
                    etl_TongTienSTA["new_season"] = vuMua;//Vudautu["new_mavudautu"].ToString();
                                                          //etl_ND["new_vudautu"] = fullEntity["new_vudautu"];
                    etl_TongTienSTA["new_sochungtu"] = fullEntity["new_masophieu"].ToString();
                    //etl_ND["new_lannhan"] =  phieuNghiemThu["new_lannhan"];
                    etl_TongTienSTA["new_contractnumber"] = hdHaTang["new_mahopdong"].ToString();// lay hop dong mia tren phieu nghiem thu
                    etl_TongTienSTA["new_tradingpartner"] = (KH.LogicalName.ToLower().Trim() == "contact" ? // phieu nghiem thu
                        ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : ""))
                        :
                        ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""))
                        );
                    etl_TongTienSTA["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                    etl_TongTienSTA["new_suppliersite"] = "TAY NINH";
                    etl_TongTienSTA["new_invoicedate"] = fullEntity["new_ngaylapphieu"];
                    etl_TongTienSTA["new_descriptionheader"] = "Trả tiền cho nhà thầu";
                    etl_TongTienSTA["new_terms"] = "Tra Ngay";
                    etl_TongTienSTA["new_taxtype"] = "";
                    // tong tien chi tiet thanh toan
                    etl_TongTienSTA["new_invoiceamount"] = fullEntity["new_tongtienthanhtoan"];
                    etl_TongTienSTA["new_gldate"] = fullEntity["new_ngayduyet"]; // ngay duyet phieu nghiem thu
                    etl_TongTienSTA["new_invoicetype"] = "STA";
                    //if (phieuNghiemThu.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)phieuNghiemThu["new_phuongthucthanhtoan"]).Value == 100000000)
                    //{
                    //    etl_NDSTA["new_paymenttype"] = "TM";
                    //}
                    //else if (phieuNghiemThu.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)phieuNghiemThu["new_phuongthucthanhtoan"]).Value == 100000001)
                    //{
                    //    etl_NDSTA["new_paymenttype"] = "CK";
                    //}

                    if (fullEntity.Contains("new_khachhang"))
                    {
                        etl_TongTienSTA["new_khachhang"] = fullEntity["new_khachhang"];
                    }
                    else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                    {
                        etl_TongTienSTA["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];
                    }

                    var staId = service.Create(etl_TongTienSTA);

                    etl_TongTienSTA["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                    etl_TongTienSTA["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                    etl_TongTienSTA["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                    etl_TongTienSTA["new_descriptionlines"] = fullEntity["new_name"].ToString();
                    etl_TongTienSTA["tran_type"] = "STA";

                    Send(etl_TongTienSTA);

                    if (fullEntity.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)fullEntity["new_phuongthucthanhtoan"]).Value == 100000001)
                    {
                        Entity apply_PhaiTraSTA = new Entity("new_applytransaction");

                        Entity etl_PhaiTra = service.Retrieve("new_etltransaction", staId, new ColumnSet(new string[] { "new_name" }));
                        if (etl_PhaiTra != null && etl_PhaiTra.Contains("new_name"))
                        {
                            apply_PhaiTraSTA["new_name"] = (string)etl_PhaiTra["new_name"];
                        }

                        apply_PhaiTraSTA["new_suppliersitecode"] = "Tây Ninh";

                        if (fullEntity.Contains("new_taikhoan"))
                        {
                            var taiKhoan = service.Retrieve("new_taikhoannganhang", ((EntityReference)fullEntity["new_taikhoan"]).Id, new ColumnSet(true));
                            apply_PhaiTraSTA["new_bankcccountnum"] = taiKhoan["new_sotaikhoan"];
                        }

                        if (fullEntity.Contains("new_taikhoannganhangttcs"))
                        {
                            var taiKhoan = service.Retrieve("new_taikhoannganhangcuattcs", ((EntityReference)fullEntity["new_taikhoannganhangttcs"]).Id, new ColumnSet(true));
                            apply_PhaiTraSTA["new_supplierbankname"] = taiKhoan["new_name"];
                        }

                        //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                        apply_PhaiTraSTA["new_paymentamount"] = fullEntity["new_tongtienthanhtoan"];

                        //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                        apply_PhaiTraSTA["new_referencenumber"] = fullEntity["new_masophieu"].ToString();
                        apply_PhaiTraSTA["new_paymentdate"] = fullEntity["new_ngayduyet"];
                        apply_PhaiTraSTA["new_paymentdocumentname"] = "CANTRU_03";
                        apply_PhaiTraSTA["new_vouchernumber"] = "CTND";
                        apply_PhaiTraSTA["new_cashflow"] = "00.00";
                        apply_PhaiTraSTA["new_paymentnum"] = 1;
                        apply_PhaiTraSTA["new_documentnum"] = fullEntity["new_masophieu"].ToString();
                        //apply_PhaiTraSTA["new_documentsequence"] = phieuNghiemThu["new_lannhan"];

                        if (fullEntity.Contains("new_khachhang"))
                            apply_PhaiTraSTA["new_khachhang"] = fullEntity["new_khachhang"];
                        else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                            apply_PhaiTraSTA["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                        apply_PhaiTraSTA.Id = service.Create(apply_PhaiTraSTA);

                        apply_PhaiTraSTA["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                        apply_PhaiTraSTA["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                        apply_PhaiTraSTA["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                        apply_PhaiTraSTA["new_type"] = "TYPE4";
                        Send(apply_PhaiTraSTA);
                    }

                    #endregion

                    if (dsCTHaTang != null && dsCTHaTang.Count > 0)
                    {
                        if (dsCTDNThanhToan != null && dsCTDNThanhToan.Count > 0)
                        {
                            foreach (var chiTiet in dsCTDNThanhToan)
                            {
                                if (chiTiet.Contains("new_nghiemthucongtrinh"))
                                {
                                    soTienNDChiu = ((Money)chiTiet["new_nongdantratienmat"]).Value;
                                    var phieuNghiemThu = service.Retrieve("new_nghiemthucongtrinh", ((EntityReference)chiTiet["new_nghiemthucongtrinh"]).Id, new ColumnSet(true));
                                    if (!phieuNghiemThu.Contains("new_ngayduyet"))
                                    {
                                        throw new Exception("Nghiệm thu chưa có ngày duyệt!");
                                    }

                                    decimal tongTienCacND = 0;
                                    var i = 0;
                                    decimal tongDienTichHuongLoi = dsCTHaTang.Sum(s => (decimal)s["new_dientichhuongloi"]);
                                    foreach (var chiTietHaTang in dsCTHaTang)
                                    {
                                        ++i;
                                        #region Phat sinh ghi no cho tung nong dan CRE
                                        if (soTienNDChiu > 0)
                                        {
                                            Entity kHNhanNo = null;
                                            if (chiTietHaTang.Contains("new_khachhang"))
                                            {
                                                kHNhanNo = service.Retrieve("contact", ((EntityReference)chiTietHaTang["new_khachhang"]).Id, new ColumnSet(true));
                                            }
                                            else
                                            {
                                                kHNhanNo = service.Retrieve("account", ((EntityReference)chiTietHaTang["new_khachhang"]).Id, new ColumnSet(true));
                                            }
                                            var dtHuongLoi = (decimal)chiTietHaTang["new_dientichhuongloi"];
                                            var soTien = ((Money)chiTietHaTang["new_sotien"]).Value;
                                            decimal thanhTien = (dtHuongLoi / tongDienTichHuongLoi) * soTienNDChiu;
                                            if (i < dsCTHaTang.Count)
                                            {
                                                tongTienCacND += thanhTien;
                                            }
                                            else
                                            {
                                                thanhTien = soTienNDChiu - tongTienCacND;
                                            }
                                            Entity etl_ND = new Entity("new_etltransaction");
                                            etl_ND["new_name"] = phieuNghiemThu["new_manghiemthu"].ToString() + "_CRE";
                                            etl_ND["new_vouchernumber"] = "DTND";
                                            etl_ND["new_transactiontype"] = "8.2.2.h";
                                            etl_ND["new_customertype"] = new OptionSetValue(1);
                                            etl_ND["new_season"] = vuMua;//Vudautu["new_mavudautu"].ToString();
                                            //etl_ND["new_vudautu"] = fullEntity["new_vudautu"];
                                            etl_ND["new_sochungtu"] = fullEntity["new_masophieu"].ToString();
                                            //etl_ND["new_lannhan"] =  phieuNghiemThu["new_lannhan"];
                                            etl_ND["new_contractnumber"] = hdHaTang["new_mahopdong"].ToString();// lay hop dong mia tren phieu nghiem thu
                                            etl_ND["new_tradingpartner"] = (kHNhanNo.LogicalName.ToLower().Trim() == "contact" ? // phieu nghiem thu
                                                ((kHNhanNo.Contains("new_makhachhang") ? kHNhanNo["new_makhachhang"].ToString() : "") + "_" + (kHNhanNo.Contains("new_socmnd") ? kHNhanNo["new_socmnd"].ToString() : ""))
                                                :
                                                ((kHNhanNo.Contains("new_makhachhang") ? kHNhanNo["new_makhachhang"].ToString() : "") + "_" + (kHNhanNo.Contains("new_masothue") ? kHNhanNo["new_masothue"].ToString() : ""))
                                                );
                                            etl_ND["new_suppliernumber"] = kHNhanNo["new_makhachhang"].ToString();
                                            etl_ND["new_suppliersite"] = "TAY NINH";
                                            etl_ND["new_invoicedate"] = phieuNghiemThu["new_ngaynghiemthu"];// lay ngay nghiem thu (ngay thuc hien)
                                            etl_ND["new_descriptionheader"] = "Ghi nợ nông dân";
                                            etl_ND["new_terms"] = "Tra Ngay";
                                            etl_ND["new_taxtype"] = "";
                                            // tong tien chi tiet thanh toan
                                            etl_ND["new_invoiceamount"] = new Money(thanhTien * (-1));
                                            etl_ND["new_gldate"] = phieuNghiemThu["new_ngayduyet"]; // ngay duyet phieu nghiem thu
                                            etl_ND["new_invoicetype"] = "CRE";
                                            //if (phieuNghiemThu.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)phieuNghiemThu["new_phuongthucthanhtoan"]).Value == 100000001)
                                            //{
                                            //    etl_ND["new_paymenttype"] = "CK";
                                            //}
                                            //else
                                            //{
                                            //    etl_ND["new_paymenttype"] = "TM";
                                            //}

                                            if (phieuNghiemThu.Contains("new_khachhang"))
                                            {
                                                etl_ND["new_khachhang"] = phieuNghiemThu["new_khachhang"];
                                            }
                                            else if (phieuNghiemThu.Contains("new_khachhangdoanhnghiep"))
                                            {
                                                etl_ND["new_khachhangdoanhnghiep"] = phieuNghiemThu["new_khachhangdoanhnghiep"];
                                            }

                                            service.Create(etl_ND);

                                            etl_ND["new_makhachhang"] = kHNhanNo.Contains("new_makhachhang") ? kHNhanNo["new_makhachhang"].ToString() : "";
                                            etl_ND["name"] = (kHNhanNo.LogicalName.ToLower() == "contact" ? (kHNhanNo.Contains("fullname") ? kHNhanNo["fullname"].ToString() : "") : (kHNhanNo.Contains("name") ? kHNhanNo["name"].ToString() : ""));
                                            etl_ND["new_socmnd"] = (kHNhanNo.LogicalName.ToLower() == "contact" ? (kHNhanNo.Contains("new_socmnd") ? kHNhanNo["new_socmnd"].ToString() : "") : (kHNhanNo.Contains("new_masothue") ? kHNhanNo["new_masothue"].ToString() : ""));
                                            etl_ND["new_descriptionlines"] = phieuNghiemThu["new_name"].ToString();
                                            etl_ND["tran_type"] = "CRE";

                                            Send(etl_ND);

                                        }
                                        #endregion
                                    }
                                }
                            }
                        }
                    }

                    #region Thu No
                    if (fullEntity.Contains("new_phieudenghithuno"))
                    {
                        Entity phieuDNThuNo = service.Retrieve("new_phieudenghithuno", ((EntityReference)fullEntity["new_phieudenghithuno"]).Id, new ColumnSet(true));


                        var dsPhieuTinhLai = RetrieveMultiRecord(service, "new_phieutinhlai", new ColumnSet(true), "new_phieudenghithuno", phieuDNThuNo.Id);

                        Entity kHPhaiTra = null;
                        if (phieuDNThuNo.Contains("new_khachhang"))
                            kHPhaiTra = service.Retrieve("contact", ((EntityReference)phieuDNThuNo["new_khachhang"]).Id,
                                new ColumnSet(true));
                        else
                            kHPhaiTra = service.Retrieve("account", ((EntityReference)phieuDNThuNo["new_khachhangdoanhnghiep"]).Id, new ColumnSet(true));


                        if (dsPhieuTinhLai != null && dsPhieuTinhLai.Count > 0)
                        {
                            int i = 0;
                            foreach (var phieuTinhLai in dsPhieuTinhLai)
                            {
                                ++i;
                                // phat sinh Apply 

                                #region Can tru, Phat sinh CRE hoac PRE
                                // Can tru
                                Entity phanBoDauTu = service.Retrieve("new_phanbodautu", ((EntityReference)phieuTinhLai["new_phanbodautu"]).Id, new ColumnSet(true));
                                //if (phieuDNThuNo.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)phieuDNThuNo["new_phuongthucthanhtoan"]).Value == 100000001)
                                //{
                                if (phanBoDauTu.Contains("new_etltransaction"))
                                {
                                    Entity etlTransaction = service.Retrieve("new_etltransaction", ((EntityReference)phanBoDauTu["new_etltransaction"]).Id, new ColumnSet(true));
                                    if (etlTransaction.Contains("new_invoicetype") && (etlTransaction["new_invoicetype"].ToString() == "PRE" || etlTransaction["new_invoicetype"].ToString() == "CRE"))
                                    {
                                        Entity apply_PhaiTraCanTruPRE = new Entity("new_applytransaction");

                                        //Entity etl_entity = service.Retrieve("new_etltransaction", phaiTraSTAID, new ColumnSet(new string[] { "new_name" }));
                                        if (etlTransaction != null && etlTransaction.Contains("new_name"))
                                        {
                                            apply_PhaiTraCanTruPRE["new_name"] = (string)etlTransaction["new_name"];
                                        }

                                        apply_PhaiTraCanTruPRE["new_suppliersitecode"] = "Tây Ninh";

                                        //if (kHPhaiTra.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)kHPhaiTra["new_phuongthucthanhtoan"]).Value == 100000001)
                                        //{
                                        //    List<Entity> taikhoannganhang = RetrieveMultiRecord(service, "new_taikhoannganhang",
                                        //        new ColumnSet(new string[] { "new_sotaikhoan", "new_giaodichchinh" }),
                                        //        kHPhaiTra.LogicalName == "contact" ? "new_khachhang" : "new_khachhangdoanhnghiep", kHPhaiTra.Id);

                                        //    Entity taikhoanchinh = null;

                                        //    foreach (Entity en in taikhoannganhang)
                                        //    {
                                        //        if ((bool)en["new_giaodichchinh"] == true)
                                        //            taikhoanchinh = en;
                                        //    }

                                        //    apply_PhaiTraCanTruPRE["new_supplierbankname"] = (taikhoanchinh == null ? "CTXL-VND-0" : taikhoanchinh["new_sotaikhoan"]);
                                        //}
                                        //else
                                        apply_PhaiTraCanTruPRE["new_bankcccountnum"] = "CTXL-VND-0";

                                        //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                                        apply_PhaiTraCanTruPRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienvay"]).Value * (-1));

                                        //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                                        apply_PhaiTraCanTruPRE["new_referencenumber"] = phieuDNThuNo["new_masophieu"].ToString();
                                        apply_PhaiTraCanTruPRE["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                        apply_PhaiTraCanTruPRE["new_paymentdocumentname"] = "CANTRU_03";
                                        apply_PhaiTraCanTruPRE["new_vouchernumber"] = "CTND";
                                        apply_PhaiTraCanTruPRE["new_cashflow"] = "00.00";
                                        apply_PhaiTraCanTruPRE["new_paymentnum"] = 1;
                                        apply_PhaiTraCanTruPRE["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();
                                        //apply_PhaiTraCanTruPRE["new_documentsequence"] = phieuDNThuNo["new_lannhan"];

                                        if (phieuDNThuNo.Contains("new_khachhang"))
                                            apply_PhaiTraCanTruPRE["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                        else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                            apply_PhaiTraCanTruPRE["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                        apply_PhaiTraCanTruPRE.Id = service.Create(apply_PhaiTraCanTruPRE);

                                        apply_PhaiTraCanTruPRE["new_makhachhang"] = kHPhaiTra.Contains("new_makhachhang") ? kHPhaiTra["new_makhachhang"].ToString() : "";
                                        apply_PhaiTraCanTruPRE["name"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("fullname") ? kHPhaiTra["fullname"].ToString() : "") : (kHPhaiTra.Contains("name") ? kHPhaiTra["name"].ToString() : ""));
                                        apply_PhaiTraCanTruPRE["new_socmnd"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("new_socmnd") ? kHPhaiTra["new_socmnd"].ToString() : "") : (kHPhaiTra.Contains("new_masothue") ? kHPhaiTra["new_masothue"].ToString() : ""));
                                        apply_PhaiTraCanTruPRE["new_type"] = etlTransaction["new_invoicetype"].ToString() == "PRE" ? "TYPE2" : "TYPE4";
                                        Send(apply_PhaiTraCanTruPRE);
                                    }
                                    //}
                                }
                                #endregion
                                // lai
                                #region Lai

                                if (phieuTinhLai.Contains("new_tienlai") && ((Money)phieuTinhLai["new_tienlai"]).Value > 0)
                                {
                                    #region phat sinh Etl CRE
                                    Entity etl_ND = new Entity("new_etltransaction");
                                    etl_ND["new_name"] = phieuDNThuNo["new_masophieu"].ToString() + "_" + i.ToString() + "_CRE";
                                    etl_ND["new_vouchernumber"] = "DTND";
                                    etl_ND["new_transactiontype"] = "5.4.2.b";
                                    etl_ND["new_customertype"] = new OptionSetValue(1);
                                    etl_ND["new_season"] = vuMua;//Vudautu["new_mavudautu"].ToString();
                                    //etl_ND["new_vudautu"] = fullEntity["new_vudautu"];
                                    etl_ND["new_sochungtu"] = phieuDNThuNo["new_masophieu"].ToString();
                                    //etl_ND["new_lannhan"] = phieuNghiemThu["new_lannhan"];
                                    etl_ND["new_contractnumber"] = phanBoDauTu["new_maphieuphanbo"].ToString();// lay hop dong mia tren phieu nghiem thu
                                    etl_ND["new_tradingpartner"] = (kHPhaiTra.LogicalName.ToLower().Trim() == "contact" ? // phieu nghiem thu
                                        ((kHPhaiTra.Contains("new_makhachhang") ? kHPhaiTra["new_makhachhang"].ToString() : "") + "_" + (kHPhaiTra.Contains("new_socmnd") ? kHPhaiTra["new_socmnd"].ToString() : ""))
                                        :
                                        ((kHPhaiTra.Contains("new_makhachhang") ? kHPhaiTra["new_makhachhang"].ToString() : "") + "_" + (kHPhaiTra.Contains("new_masothue") ? kHPhaiTra["new_masothue"].ToString() : ""))
                                        );
                                    etl_ND["new_suppliernumber"] = kHPhaiTra["new_makhachhang"].ToString();
                                    etl_ND["new_suppliersite"] = "TAY NINH";
                                    etl_ND["new_invoicedate"] = phieuDNThuNo["new_ngaythu"];// lay ngay nghiem thu (ngay thuc hien)
                                    etl_ND["new_descriptionheader"] = "Tiền lãi_vụ_" + vuMua;
                                    etl_ND["new_terms"] = "Tra Ngay";
                                    etl_ND["new_taxtype"] = "";
                                    // tong tien chi tiet thanh toan
                                    etl_ND["new_invoiceamount"] = new Money(((Money)phieuTinhLai["new_tienlai"]).Value * (-1));
                                    etl_ND["new_gldate"] = phieuDNThuNo["new_ngaythu"]; // ngay duyet phieu nghiem thu
                                    etl_ND["new_invoicetype"] = "CRE";
                                    //if (phieuDNThuNo.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)phieuDNThuNo["new_phuongthucthanhtoan"]).Value == 100000001)
                                    //{
                                    //    etl_ND["new_paymenttype"] = "CK";
                                    //}else
                                    //{
                                    //    etl_ND["new_paymenttype"] = "TM";
                                    //}

                                    if (phieuDNThuNo.Contains("new_khachhang"))
                                    {
                                        etl_ND["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                    }
                                    else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                    {
                                        etl_ND["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];
                                    }

                                    var etl_LaiID = service.Create(etl_ND);

                                    etl_ND["new_makhachhang"] = kHPhaiTra.Contains("new_makhachhang") ? kHPhaiTra["new_makhachhang"].ToString() : "";
                                    etl_ND["name"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("fullname") ? kHPhaiTra["fullname"].ToString() : "") : (kHPhaiTra.Contains("name") ? kHPhaiTra["name"].ToString() : ""));
                                    etl_ND["new_socmnd"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("new_socmnd") ? kHPhaiTra["new_socmnd"].ToString() : "") : (kHPhaiTra.Contains("new_masothue") ? kHPhaiTra["new_masothue"].ToString() : ""));
                                    etl_ND["new_descriptionlines"] = phieuDNThuNo["new_name"].ToString();
                                    etl_ND["tran_type"] = "CRE";

                                    Send(etl_ND);
                                    #endregion

                                    #region Phat sinh Apply 
                                    //if (phieuDNThuNo.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)phieuDNThuNo["new_phuongthucthanhtoan"]).Value == 100000001)
                                    //{
                                    // phat sinh apply CRE Lai
                                    Entity apply_PhaiTraCanTruCRE = new Entity("new_applytransaction");

                                    Entity etl_Lai = service.Retrieve("new_etltransaction", etl_LaiID, new ColumnSet(true));
                                    if (etl_Lai != null && etl_Lai.Contains("new_name"))
                                    {
                                        apply_PhaiTraCanTruCRE["new_name"] = (string)etl_Lai["new_name"];
                                    }

                                    apply_PhaiTraCanTruCRE["new_suppliersitecode"] = "Tây Ninh";

                                    //if (kHPhaiTra.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)kHPhaiTra["new_phuongthucthanhtoan"]).Value == 100000001)
                                    //{
                                    //    List<Entity> taikhoannganhang = RetrieveMultiRecord(service, "new_taikhoannganhang",
                                    //        new ColumnSet(new string[] { "new_sotaikhoan", "new_giaodichchinh" }),
                                    //        kHPhaiTra.LogicalName == "contact" ? "new_khachhang" : "new_khachhangdoanhnghiep", kHPhaiTra.Id);

                                    //    Entity taikhoanchinh = null;

                                    //    foreach (Entity en in taikhoannganhang)
                                    //    {
                                    //        if ((bool)en["new_giaodichchinh"] == true)
                                    //            taikhoanchinh = en;
                                    //    }

                                    //    apply_PhaiTraCanTruCRE["new_supplierbankname"] = (taikhoanchinh == null ? "CTXL-VND-0" : taikhoanchinh["new_sotaikhoan"]);
                                    //}
                                    //else
                                    apply_PhaiTraCanTruCRE["new_bankcccountnum"] = "CTXL-VND-0";

                                    //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                                    apply_PhaiTraCanTruCRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienlai"]).Value * (-1));

                                    //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                                    apply_PhaiTraCanTruCRE["new_referencenumber"] = phieuDNThuNo["new_masophieu"].ToString();
                                    apply_PhaiTraCanTruCRE["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                    apply_PhaiTraCanTruCRE["new_paymentdocumentname"] = "CANTRU_03";
                                    apply_PhaiTraCanTruCRE["new_vouchernumber"] = "CTND";
                                    apply_PhaiTraCanTruCRE["new_cashflow"] = "00.00";
                                    apply_PhaiTraCanTruCRE["new_paymentnum"] = 1;
                                    apply_PhaiTraCanTruCRE["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();
                                    //apply_PhaiTraCanTruCRE["new_documentsequence"] = phieuDNThuNo["new_lannhan"];

                                    if (phieuDNThuNo.Contains("new_khachhang"))
                                        apply_PhaiTraCanTruCRE["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                    else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                        apply_PhaiTraCanTruCRE["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                    apply_PhaiTraCanTruCRE.Id = service.Create(apply_PhaiTraCanTruCRE);

                                    apply_PhaiTraCanTruCRE["new_makhachhang"] = kHPhaiTra.Contains("new_makhachhang") ? kHPhaiTra["new_makhachhang"].ToString() : "";
                                    apply_PhaiTraCanTruCRE["name"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("fullname") ? kHPhaiTra["fullname"].ToString() : "") : (kHPhaiTra.Contains("name") ? kHPhaiTra["name"].ToString() : ""));
                                    apply_PhaiTraCanTruCRE["new_socmnd"] = (kHPhaiTra.LogicalName.ToLower() == "contact" ? (kHPhaiTra.Contains("new_socmnd") ? kHPhaiTra["new_socmnd"].ToString() : "") : (kHPhaiTra.Contains("new_masothue") ? kHPhaiTra["new_masothue"].ToString() : ""));
                                    apply_PhaiTraCanTruCRE["new_type"] = "TYPE4";
                                    Send(apply_PhaiTraCanTruCRE);
                                    //}
                                    #endregion
                                }
                                #endregion
                            }
                        }
                    }
                    #endregion
                }
                Send(null);
            }
        }

        public void Send(Entity tmp)
        {
            MessageQueue mq;

            if (MessageQueue.Exists(@".\Private$\DynamicCRM2Oracle"))
                mq = new MessageQueue(@".\Private$\DynamicCRM2Oracle");
            else
                mq = MessageQueue.Create(@".\Private$\DynamicCRM2Oracle");

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

        public string Serialize(object obj)
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

        public object Deserialize(string xml, Type toType)
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
    }
}
