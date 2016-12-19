using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.Workflow;
using System;
using System.Activities;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Messaging;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Workflow_LenhChiTinhTienMia
{
    public sealed class Workflow_LenhChiTinhTienMia : CodeActivity
    {
        [RequiredArgument]
        [Input("LenhChiTienTienMia")]
        [ReferenceTarget("new_bangketienmia")]
        public InArgument<EntityReference> inputEntity { get; set; }
        private IOrganizationService service;
        ITracingService trace = null;
        IWorkflowContext context = null;
        protected override void Execute(CodeActivityContext currentContext)
        {
            context = currentContext.GetExtension<IWorkflowContext>();
            trace = (ITracingService)currentContext.GetExtension<ITracingService>();
            var currEntity = inputEntity.Get(currentContext);

            List<Entity> listSend = new List<Entity>();

            if (currEntity.LogicalName == "new_bangketienmia")
            {
                IOrganizationServiceFactory factory = currentContext.GetExtension<IOrganizationServiceFactory>();
                service = factory.CreateOrganizationService(context.UserId);
                var fullEntity = service.Retrieve(currEntity.LogicalName, currEntity.Id, new Microsoft.Xrm.Sdk.Query.ColumnSet(true));
                if (fullEntity.Contains("statuscode") && ((OptionSetValue)fullEntity["statuscode"]).Value == 100000000)
                {
                    string test = "";

                    //var dsPhieuTTMia = RetrieveMultiRecord(service, "new_phieutinhtienmia", new ColumnSet(true), "new_bangke", fullEntity.Id);
                    QueryExpression q = new QueryExpression("new_phieutinhtienmia");
                    q.ColumnSet = new ColumnSet(true);
                    q.Criteria = new FilterExpression();
                    q.Criteria.AddCondition(new ConditionExpression("new_bangke", ConditionOperator.Equal, fullEntity.Id));
                    q.PageInfo = new PagingInfo();
                    q.PageInfo.PageNumber = 1;
                    q.PageInfo.PagingCookie = null;
                    q.PageInfo.Count = 200;
                    while (true)
                    {
                        EntityCollection entc = service.RetrieveMultiple(q);
                        var dsPhieuTTMia = entc.Entities.ToList<Entity>();

                        Entity khNongDan = null, khVanChuyen = null, khThuHoach = null;
                        Entity hDMia = null, hDVanChuyen = null, hDThuHoach = null;
                        Entity vuThuHoach = null;
                        decimal tamGiuNongDan = 0, tamGiuVanChuyen = 0, tamGiuThuHoach = 0;

                        if (!fullEntity.Contains("new_ngayduyet"))
                        {
                            throw new Exception("Lệnh chi chưa có ngày duyệt!");
                        }
                        if (dsPhieuTTMia != null && dsPhieuTTMia.Count == 0)
                        {
                            throw new Exception("Lệnh chi chưa có phiếu tính tiền mía!");
                        }
                        //vudautu = service.Retrieve("new_vudautu", , new ColumnSet(new string[] { "new_mavudautu" }));
                        var lsVuThuHoach = RetrieveMultiRecord(service, "new_vuthuhoach", new ColumnSet(true), "new_vuthuhoachid", ((EntityReference)fullEntity["new_vuthuhoach"]).Id);
                        vuThuHoach = lsVuThuHoach.Count > 0 ? lsVuThuHoach[0] : null;
                        string vuMua = "";
                        if (vuThuHoach != null)
                        {
                            vuMua = ((DateTime)vuThuHoach["new_tungay"]).ToString("yyyy") + "-" + ((DateTime)vuThuHoach["new_denngay"]).ToString("yyyy");
                        }

                        foreach (var phieuTTMia in dsPhieuTTMia)
                        {
                            trace.Trace("START " + phieuTTMia["new_masophieu"].ToString());

                            if (phieuTTMia.Contains("new_khachhang"))
                                khNongDan = service.Retrieve("contact", ((EntityReference)phieuTTMia["new_khachhang"]).Id, new ColumnSet(true));
                            else
                                khNongDan = service.Retrieve("account", ((EntityReference)phieuTTMia["new_khachhangdoanhnghiep"]).Id, new ColumnSet(true));

                            if (phieuTTMia.Contains("new_doitacthuhoach"))
                                khThuHoach = service.Retrieve("contact", ((EntityReference)phieuTTMia["new_doitacthuhoach"]).Id,
                                    new ColumnSet(true));
                            else if (phieuTTMia.Contains("new_doitacthuhoachkhdn"))
                                khThuHoach = service.Retrieve("account", ((EntityReference)phieuTTMia["new_doitacthuhoachkhdn"]).Id,
                                    new ColumnSet(true));
                            else
                                khThuHoach = khNongDan;

                            if (phieuTTMia.Contains("new_doitacvanchuyen"))
                                khVanChuyen = service.Retrieve("contact", ((EntityReference)phieuTTMia["new_doitacvanchuyen"]).Id,
                                    new ColumnSet(true));
                            else if (phieuTTMia.Contains("new_doitacvanchuyenkhdn"))
                                khVanChuyen = service.Retrieve("account", ((EntityReference)phieuTTMia["new_doitacvanchuyenkhdn"]).Id,
                                    new ColumnSet(true));
                            else
                                khThuHoach = khNongDan;
                            if (phieuTTMia.Contains("new_hopdongdautumia"))
                            {
                                hDMia = service.Retrieve("new_hopdongdautumia", ((EntityReference)phieuTTMia["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_masohopdong" }));
                            }
                            else
                            {
                                hDMia = service.Retrieve("new_hopdongmuabanmiangoai", ((EntityReference)phieuTTMia["new_hdmuabanmiangoai"]).Id, new ColumnSet(new string[] { "new_masohopdong" }));
                            }
                            tamGiuNongDan = phieuTTMia.Contains("new_tamgiunongdan") ? ((Money)phieuTTMia["new_tamgiunongdan"]).Value : 0;
                            tamGiuThuHoach = phieuTTMia.Contains("new_tamgiuthuhoach") ? ((Money)phieuTTMia["new_tamgiuthuhoach"]).Value : 0;
                            tamGiuVanChuyen = phieuTTMia.Contains("new_tamgiuvanchuyen") ? ((Money)phieuTTMia["new_tamgiuvanchuyen"]).Value : 0;

                            trace.Trace("Start chi chu mia");
                            Guid etlTienMia = Guid.Empty;
                            // Tien chi chu mia
                            #region Chi Tien Mia
                            if (phieuTTMia.Contains("new_chiphinhamaymuamia") && ((Money)phieuTTMia["new_chiphinhamaymuamia"]).Value > 0)
                            {
                                var tienMia = ((Money)phieuTTMia["new_chiphinhamaymuamia"]).Value - tamGiuVanChuyen - tamGiuThuHoach - tamGiuNongDan - ((Money)phieuTTMia["new_tienchichumia"]).Value;
                                trace.Trace("ETL Tien Mia");
                                #region ETL Tien Mia                        
                                // STA chi phi nha may - chi chu mia
                                Entity etl_STA = new Entity("new_etltransaction");
                                etl_STA["new_name"] = fullEntity["new_masophieu"].ToString() + "_CHIPHIMUAMIA_" + phieuTTMia["new_masophieu"].ToString() + "_STA" + test;//Lenh chi, vu thu hoach, 
                                etl_STA["new_vouchernumber"] = "MMND";
                                etl_STA["new_transactiontype"] = "4.3.3.b";
                                etl_STA["new_customertype"] = new OptionSetValue(phieuTTMia.Contains("new_khachhang") ? 1 : 2);
                                etl_STA["new_season"] = vuMua;//vudautu["new_mavudautu"].ToString();
                                                              //etl_STA["new_vudautu"] = fullEntity["new_vudautu"];
                                etl_STA["new_sochungtu"] = fullEntity["new_masophieu"].ToString() + "_" + phieuTTMia["new_masophieu"].ToString();
                                etl_STA["new_lannhan"] = 1;// fullEntity["new_lannhan"];
                                etl_STA["new_contractnumber"] = hDMia["new_masohopdong"].ToString();//fullEntity["new_masophieu"].ToString() + "_" + "Tiền mía_vụ_" + vuMua;
                                etl_STA["new_tradingpartner"] = (khNongDan.LogicalName.ToLower().Trim() == "contact" ?
                                    ((khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "") + "_" + (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : ""))
                                    :
                                    ((khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "") + "_" + (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""))
                                    );

                                etl_STA["new_suppliersite"] = "TAY NINH";
                                etl_STA["new_invoicedate"] = phieuTTMia["new_ngaylap"];
                                etl_STA["new_descriptionheader"] = "Tiền mía_vụ_" + vuMua;
                                etl_STA["new_terms"] = "Tra Ngay";
                                etl_STA["new_taxtype"] = "";

                                etl_STA["new_invoiceamount"] = new Money(tienMia);
                                etl_STA["new_gldate"] = fullEntity["new_ngayduyet"];
                                etl_STA["new_invoicetype"] = "STA";

                                if (phieuTTMia.Contains("new_khachhang"))
                                    etl_STA["new_khachhang"] = phieuTTMia["new_khachhang"];
                                else if (phieuTTMia.Contains("new_khachhangdoanhnghiep"))
                                    etl_STA["new_khachhangdoanhnghiep"] = phieuTTMia["new_khachhangdoanhnghiep"];
                                etlTienMia = service.Create(etl_STA);

                                etl_STA["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                                etl_STA["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                                etl_STA["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                                etl_STA["tran_type"] = "STA";
                                //etl_STA["new_descriptionlines"] = phieuTTMia["new_masophieu"].ToString();

                                listSend.Add(etl_STA);
                                //Send(etl_STA);

                                #endregion

                                if (phieuTTMia.Contains("new_tienchichumia") && ((Money)phieuTTMia["new_tienchichumia"]).Value > 0)
                                {
                                    #region STA chi chu mia
                                    // STA chi chu mia
                                    Entity etl_STAChiChuMia = new Entity("new_etltransaction");
                                    etl_STAChiChuMia["new_name"] = fullEntity["new_masophieu"].ToString() + "_MIA_" + phieuTTMia["new_masophieu"].ToString() + "_STA" + test;//Lenh chi, vu thu hoach, 
                                    etl_STAChiChuMia["new_vouchernumber"] = "MMND";
                                    etl_STAChiChuMia["new_transactiontype"] = "4.3.3.b";
                                    etl_STAChiChuMia["new_customertype"] = new OptionSetValue(phieuTTMia.Contains("new_khachhang") ? 1 : 2);
                                    etl_STAChiChuMia["new_season"] = vuMua;//vudautu["new_mavudautu"].ToString();
                                                                           //etl_STA["new_vudautu"] = fullEntity["new_vudautu"];
                                    etl_STAChiChuMia["new_sochungtu"] = fullEntity["new_masophieu"].ToString() + "_" + phieuTTMia["new_masophieu"].ToString();
                                    etl_STAChiChuMia["new_lannhan"] = 1;// fullEntity["new_lannhan"];
                                    etl_STAChiChuMia["new_contractnumber"] = hDMia["new_masohopdong"].ToString();//fullEntity["new_masophieu"].ToString() + "_" + "Tiền mía_vụ_" + vuMua;
                                    etl_STAChiChuMia["new_tradingpartner"] = (khNongDan.LogicalName.ToLower().Trim() == "contact" ?
                                        ((khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "") + "_" + (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : ""))
                                        :
                                        ((khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "") + "_" + (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""))
                                        );

                                    etl_STAChiChuMia["new_suppliersite"] = "TAY NINH";
                                    etl_STAChiChuMia["new_invoicedate"] = phieuTTMia["new_ngaylap"];
                                    etl_STAChiChuMia["new_descriptionheader"] = "Tiền mía_vụ_" + vuMua;
                                    etl_STAChiChuMia["new_terms"] = "Tra Ngay";
                                    etl_STAChiChuMia["new_taxtype"] = "";

                                    etl_STAChiChuMia["new_invoiceamount"] = (Money)phieuTTMia["new_tienchichumia"];
                                    etl_STAChiChuMia["new_gldate"] = fullEntity["new_ngayduyet"];
                                    etl_STAChiChuMia["new_invoicetype"] = "STA";
                                    if (khNongDan.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)khNongDan["new_phuongthucthanhtoan"]).Value == 100000001)
                                    {
                                        etl_STAChiChuMia["new_paymenttype"] = "CK";
                                    }
                                    else
                                    {
                                        etl_STAChiChuMia["new_paymenttype"] = "TM";
                                    }

                                    if (phieuTTMia.Contains("new_khachhang"))
                                        etl_STAChiChuMia["new_khachhang"] = phieuTTMia["new_khachhang"];
                                    else if (phieuTTMia.Contains("new_khachhangdoanhnghiep"))
                                        etl_STAChiChuMia["new_khachhangdoanhnghiep"] = phieuTTMia["new_khachhangdoanhnghiep"];

                                    var etl_STAChiChuMiaID = service.Create(etl_STAChiChuMia);

                                    etl_STAChiChuMia["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                                    etl_STAChiChuMia["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                                    etl_STAChiChuMia["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                                    etl_STAChiChuMia["tran_type"] = "STA";
                                    //etl_STA["new_descriptionlines"] = phieuTTMia["new_masophieu"].ToString();

                                    listSend.Add(etl_STAChiChuMia);
                                    #endregion

                                    trace.Trace("Apply Tien Mia");
                                    #region Apply Tien Mia
                                    if (khNongDan.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)khNongDan["new_phuongthucthanhtoan"]).Value == 100000001)
                                    {
                                        Entity apply_STAMia = new Entity("new_applytransaction");

                                        Entity etl_Mia = service.Retrieve("new_etltransaction", etl_STAChiChuMiaID, new ColumnSet(true));
                                        if (etl_Mia != null && etl_Mia.Contains("new_name"))
                                        {
                                            apply_STAMia["new_name"] = (string)etl_Mia["new_name"];
                                        }

                                        apply_STAMia["new_suppliersitecode"] = "Tây Ninh";

                                        //Entity taikhoanchinh = service.Retrieve("new_taikhoannganhang", ((EntityReference)phieuTTMia["new_taikhoan"]).Id, new ColumnSet(true));
                                        //apply_STAMia["new_supplierbankname"] = taikhoanchinh["new_sotaikhoan"];
                                        if (khNongDan.Contains("new_taikhoannganhangttcs"))
                                        {
                                            Entity taiKhoanTTCS = service.Retrieve("new_taikhoannganhangcuattcs", ((EntityReference)khNongDan["new_taikhoannganhangttcs"]).Id, new ColumnSet(true));
                                            apply_STAMia["new_bankcccountnum"] = taiKhoanTTCS["new_name"];
                                        }

                                        apply_STAMia["new_paymentamount"] = phieuTTMia["new_tienchichumia"];// new Money(tienMia);

                                        apply_STAMia["new_referencenumber"] = phieuTTMia["new_masophieu"].ToString() + "_" + apply_STAMia["new_name"];
                                        apply_STAMia["new_paymentdate"] = phieuTTMia["new_ngaylap"];
                                        apply_STAMia["new_paymentdocumentname"] = "Tiền mía_vụ_" + vuMua;
                                        apply_STAMia["new_vouchernumber"] = "BN";
                                        apply_STAMia["new_cashflow"] = "02.01";
                                        apply_STAMia["new_paymentnum"] = "1";
                                        apply_STAMia["new_documentnum"] = phieuTTMia["new_masophieu"].ToString();
                                        //apply_STAMia["new_documentsequence"] = 1;

                                        if (phieuTTMia.Contains("new_khachhang"))
                                            apply_STAMia["new_khachhang"] = phieuTTMia["new_khachhang"];
                                        else if (phieuTTMia.Contains("new_khachhangdoanhnghiep"))
                                            apply_STAMia["new_khachhangdoanhnghiep"] = phieuTTMia["new_khachhangdoanhnghiep"];

                                        apply_STAMia.Id = service.Create(apply_STAMia);

                                        apply_STAMia["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                                        apply_STAMia["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                                        apply_STAMia["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                                        apply_STAMia["new_type"] = "TYPE5";
                                        //Send(apply_STAMia);
                                        listSend.Add(apply_STAMia);
                                    }
                                    #endregion
                                }
                            }
                            #endregion

                            trace.Trace("Thuong chu mia - thuong ccs");
                            // Thuong chu mia - thuong ccs
                            #region ThuongChuMia
                            //if (phieuTTMia.Contains("new_tienthuongccs") && ((Money)phieuTTMia["new_tienthuongccs"]).Value > 0)
                            //{
                            //    #region ETL Thuong Mia
                            //    // STA
                            //    Entity etl_STA = new Entity("new_etltransaction");
                            //    etl_STA["new_name"] = fullEntity["new_masophieu"].ToString() + "_THUONG_MIA_" + phieuTTMia["new_masophieu"].ToString() + "_STA" + test;
                            //    etl_STA["new_vouchernumber"] = "MMND";
                            //    etl_STA["new_transactiontype"] = "4.3.3.b";
                            //    etl_STA["new_customertype"] = new OptionSetValue(phieuTTMia.Contains("new_khachhang") ? 1 : 2);
                            //    etl_STA["new_season"] = vuMua;
                            //    //etl_STA["new_vudautu"] = fullEntity["new_vudautu"];
                            //    etl_STA["new_sochungtu"] = fullEntity["new_masophieu"].ToString() + "_" + phieuTTMia["new_masophieu"].ToString();
                            //    etl_STA["new_lannhan"] = 1;// fullEntity["new_lannhan"];
                            //    etl_STA["new_contractnumber"] = hDMia["new_masohopdong"].ToString();//fullEntity["new_masophieu"].ToString() + "_" + "Tiền thưởng mía - ccs_vụ_" + vuMua;
                            //    etl_STA["new_tradingpartner"] = (khNongDan.LogicalName.ToLower().Trim() == "contact" ?
                            //        ((khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "") + "_" + (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : ""))
                            //        :
                            //        ((khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "") + "_" + (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""))
                            //        );

                            //    etl_STA["new_suppliersite"] = "TAY NINH";
                            //    etl_STA["new_invoicedate"] = phieuTTMia["new_ngaylap"];
                            //    etl_STA["new_descriptionheader"] = "Tiền thưởng mía - ccs_vụ_" + vuMua;
                            //    etl_STA["new_terms"] = "Tra Ngay";
                            //    etl_STA["new_taxtype"] = "";

                            //    etl_STA["new_invoiceamount"] = phieuTTMia["new_tienthuongccs"];
                            //    etl_STA["new_gldate"] = fullEntity["new_ngayduyet"];
                            //    etl_STA["new_invoicetype"] = "STA";
                            //    //if (khNongDan.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)khNongDan["new_phuongthucthanhtoan"]).Value == 100000001)
                            //    //{
                            //    //    etl_STA["new_paymenttype"] = "CK";
                            //    //}
                            //    //else
                            //    //{
                            //    //    etl_STA["new_paymenttype"] = "TM";
                            //    //}

                            //    if (phieuTTMia.Contains("new_khachhang"))
                            //        etl_STA["new_khachhang"] = phieuTTMia["new_khachhang"];
                            //    else if (phieuTTMia.Contains("new_khachhangdoanhnghiep"))
                            //        etl_STA["new_khachhangdoanhnghiep"] = phieuTTMia["new_khachhangdoanhnghiep"];
                            //    Guid etl_STAId = service.Create(etl_STA);

                            //    etl_STA["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                            //    etl_STA["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                            //    etl_STA["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                            //    etl_STA["tran_type"] = "STA";
                            //    //etl_STA["new_descriptionlines"] = phieuTTMia["new_masophieu"].ToString();

                            //    Send(etl_STA);
                            //    #endregion

                            //    #region ApplyThuongTienMia
                            //    if (khNongDan.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)khNongDan["new_phuongthucthanhtoan"]).Value == 100000001)
                            //    {
                            //        // phat sinh apply CRE Lai
                            //        Entity apply_STAThuongMia = new Entity("new_applytransaction");

                            //        Entity etl_Mia = service.Retrieve("new_etltransaction", etl_STAId, new ColumnSet(true));
                            //        if (etl_Mia != null && etl_Mia.Contains("new_name"))
                            //        {
                            //            apply_STAThuongMia["new_name"] = (string)etl_Mia["new_name"];
                            //        }

                            //        apply_STAThuongMia["new_suppliersitecode"] = "Tây Ninh";

                            //        if (khNongDan.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)khNongDan["new_phuongthucthanhtoan"]).Value == 100000001)
                            //        {
                            //            List<Entity> taikhoannganhang = RetrieveMultiRecord(service, "new_taikhoannganhang",
                            //                new ColumnSet(new string[] { "new_sotaikhoan", "new_giaodichchinh" }),
                            //                khNongDan.LogicalName == "contact" ? "new_khachhang" : "new_khachhangdoanhnghiep", khNongDan.Id);

                            //            Entity taikhoanchinh = null;

                            //            foreach (Entity en in taikhoannganhang)
                            //            {
                            //                if ((bool)en["new_giaodichchinh"] == true)
                            //                    taikhoanchinh = en;
                            //            }

                            //            //apply_STAThuongMia["new_supplierbankname"] = (taikhoanchinh == null ? "TTCS-MIA-VND" : taikhoanchinh["new_sotaikhoan"]);
                            //            apply_STAThuongMia["new_bankcccountnum"] = (taikhoanchinh == null ? "TTCS-MIA-VND" : taikhoanchinh["new_sotaikhoan"]);
                            //        }
                            //        else
                            //        {
                            //            apply_STAThuongMia["new_bankcccountnum"] = "TTCS-MIA-VND";
                            //        }

                            //        //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                            //        apply_STAThuongMia["new_paymentamount"] = (Money)phieuTTMia["new_tienthuongccs"];

                            //        //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                            //        apply_STAThuongMia["new_referencenumber"] = phieuTTMia["new_masophieu"].ToString();
                            //        apply_STAThuongMia["new_paymentdate"] = phieuTTMia["new_ngaylap"];
                            //        apply_STAThuongMia["new_paymentdocumentname"] = "Tiền thuong mía - ccs_vụ_" + vuMua;
                            //        apply_STAThuongMia["new_vouchernumber"] = "BN";
                            //        apply_STAThuongMia["new_cashflow"] = "00.00";
                            //        apply_STAThuongMia["new_paymentnum"] = "1";
                            //        apply_STAThuongMia["new_documentnum"] = phieuTTMia["new_masophieu"].ToString();
                            //        //apply_STAThuongMia["new_documentsequence"] = 1;

                            //        if (phieuTTMia.Contains("new_khachhang"))
                            //            apply_STAThuongMia["new_khachhang"] = phieuTTMia["new_khachhang"];
                            //        else if (phieuTTMia.Contains("new_khachhangdoanhnghiep"))
                            //            apply_STAThuongMia["new_khachhangdoanhnghiep"] = phieuTTMia["new_khachhangdoanhnghiep"];

                            //        apply_STAThuongMia.Id = service.Create(apply_STAThuongMia);

                            //        apply_STAThuongMia["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                            //        apply_STAThuongMia["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                            //        apply_STAThuongMia["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                            //        apply_STAThuongMia["new_type"] = "TYPE5";
                            //        Send(apply_STAThuongMia);
                            //    }
                            //    #endregion
                            //}
                            #endregion

                            trace.Trace("Can tru chu mia");
                            //Can tru chu mia
                            #region CanTruChuMia
                            if (phieuTTMia.Contains("new_pdnthuno"))
                            {
                                Entity phieuDNThuNo = service.Retrieve("new_phieudenghithuno", ((EntityReference)phieuTTMia["new_pdnthuno"]).Id, new ColumnSet(true));

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

                                        #region Can tru, Phat sinh CRE hoac PRE
                                        // Can tru
                                        Entity phanBoDauTu = service.Retrieve("new_phanbodautu", ((EntityReference)phieuTinhLai["new_phanbodautu"]).Id, new ColumnSet(true));
                                        if (phanBoDauTu.Contains("new_etltransaction"))
                                        {
                                            Entity etlTransaction = service.Retrieve("new_etltransaction", ((EntityReference)phanBoDauTu["new_etltransaction"]).Id, new ColumnSet(true));
                                            if (etlTransaction.Contains("new_invoicetype") && (etlTransaction["new_invoicetype"].ToString() == "PRE" || etlTransaction["new_invoicetype"].ToString() == "CRE"))
                                            {
                                                if (etlTransaction["new_invoicetype"].ToString() == "CRE")
                                                {
                                                    #region CanTruTienGoc Từ TienThanhToan
                                                    Entity apply_CanTruTienNoGoc = new Entity("new_applytransaction");
                                                    Entity etl_entity = service.Retrieve("new_etltransaction", etlTienMia, new ColumnSet(new string[] { "new_name" }));
                                                    if (etl_entity != null && etl_entity.Contains("new_name"))
                                                    {
                                                        apply_CanTruTienNoGoc["new_name"] = (string)etl_entity["new_name"];
                                                    }

                                                    apply_CanTruTienNoGoc["new_suppliersitecode"] = "Tây Ninh";
                                                    apply_CanTruTienNoGoc["new_bankcccountnum"] = "CTXL-VND-0";

                                                    apply_CanTruTienNoGoc["new_referencenumber"] = phieuDNThuNo["new_masophieu"].ToString() + "_" + i.ToString();
                                                    apply_CanTruTienNoGoc["new_paymentamount"] = phieuTinhLai["new_tienvay"];
                                                    apply_CanTruTienNoGoc["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                                    apply_CanTruTienNoGoc["new_paymentdocumentname"] = "CANTRU_03";
                                                    apply_CanTruTienNoGoc["new_vouchernumber"] = "CTND";
                                                    apply_CanTruTienNoGoc["new_cashflow"] = "00.00";
                                                    apply_CanTruTienNoGoc["new_paymentnum"] = "1";
                                                    apply_CanTruTienNoGoc["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();

                                                    if (phieuDNThuNo.Contains("new_khachhang"))
                                                        apply_CanTruTienNoGoc["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                                    else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                                        apply_CanTruTienNoGoc["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                                    apply_CanTruTienNoGoc.Id = service.Create(apply_CanTruTienNoGoc);

                                                    apply_CanTruTienNoGoc["new_makhachhang"] = khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "";
                                                    apply_CanTruTienNoGoc["name"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("fullname") ? khThuHoach["fullname"].ToString() : "") : (khThuHoach.Contains("name") ? khThuHoach["name"].ToString() : ""));
                                                    apply_CanTruTienNoGoc["new_socmnd"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : "") : (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""));
                                                    apply_CanTruTienNoGoc["new_type"] = "TYPE4";

                                                    listSend.Add(apply_CanTruTienNoGoc);
                                                    //Send(apply_CanTruTienNoGoc);
                                                    #endregion
                                                }

                                                #region Apply tien vay
                                                Entity apply_PhaiTraCanTruPRE = new Entity("new_applytransaction");
                                                if (etlTransaction["new_invoicetype"].ToString() == "PRE")
                                                {
                                                    Entity etl_entity = service.Retrieve("new_etltransaction", etlTienMia, new ColumnSet(new string[] { "new_name" }));
                                                    apply_PhaiTraCanTruPRE["new_name"] = (string)etl_entity["new_name"];
                                                }
                                                else
                                                {
                                                    if (etlTransaction != null && etlTransaction.Contains("new_name"))
                                                    {
                                                        apply_PhaiTraCanTruPRE["new_name"] = (string)etlTransaction["new_name"];
                                                    }
                                                    apply_PhaiTraCanTruPRE["new_bankcccountnum"] = "CTXL-VND-0";
                                                }

                                                apply_PhaiTraCanTruPRE["new_suppliersitecode"] = "Tây Ninh";

                                                apply_PhaiTraCanTruPRE["new_referencenumber"] = phieuDNThuNo["new_masophieu"] + "_" + i.ToString();
                                                apply_PhaiTraCanTruPRE["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                                apply_PhaiTraCanTruPRE["new_paymentdocumentname"] = "CANTRU_04";
                                                apply_PhaiTraCanTruPRE["new_vouchernumber"] = "CTND";
                                                apply_PhaiTraCanTruPRE["new_cashflow"] = "00.00";
                                                apply_PhaiTraCanTruPRE["new_paymentnum"] = "1";
                                                if (etlTransaction["new_invoicetype"].ToString() == "PRE")
                                                {
                                                    apply_PhaiTraCanTruPRE["new_prepay_num"] = etlTransaction["new_name"];
                                                    apply_PhaiTraCanTruPRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienvay"]).Value);
                                                }
                                                else
                                                {
                                                    apply_PhaiTraCanTruPRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienvay"]).Value * (-1));
                                                }
                                                apply_PhaiTraCanTruPRE["new_documentnum"] = etlTransaction["new_sochungtu"];//phieuDNThuNo["new_masophieu"].ToString();

                                                if (phieuDNThuNo.Contains("new_khachhang"))
                                                    apply_PhaiTraCanTruPRE["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                                else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                                    apply_PhaiTraCanTruPRE["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                                apply_PhaiTraCanTruPRE.Id = service.Create(apply_PhaiTraCanTruPRE);

                                                apply_PhaiTraCanTruPRE["new_makhachhang"] = khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "";
                                                apply_PhaiTraCanTruPRE["name"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("fullname") ? khThuHoach["fullname"].ToString() : "") : (khThuHoach.Contains("name") ? khThuHoach["name"].ToString() : ""));
                                                apply_PhaiTraCanTruPRE["new_socmnd"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : "") : (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""));
                                                apply_PhaiTraCanTruPRE["new_type"] = etlTransaction["new_invoicetype"].ToString() == "PRE" ? "TYPE2" : "TYPE4";

                                                //Send(apply_PhaiTraCanTruPRE);
                                                listSend.Add(apply_PhaiTraCanTruPRE);
                                                #endregion
                                            }
                                        }
                                        #endregion
                                        ++i;
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
                                            etl_ND["new_season"] = vuMua;
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
                                            etl_ND["new_invoicedate"] = phieuTTMia["new_ngaylap"];// lay ngay nghiem thu (ngay thuc hien)
                                            etl_ND["new_descriptionheader"] = "Nợ lãi chủ mía_vụ_" + vuMua;
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

                                            etl_ND["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                                            etl_ND["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                                            etl_ND["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                                            //etl_ND["new_descriptionlines"] = phieuDNThuNo["new_name"].ToString();
                                            etl_ND["tran_type"] = "CRE";

                                            listSend.Add(etl_ND);
                                            //Send(etl_ND);
                                            #endregion

                                            #region Phat sinh Apply                                     
                                            // phat sinh apply CRE Lai
                                            Entity apply_PhaiTraCanTruCRE = new Entity("new_applytransaction");

                                            Entity etl_Lai = service.Retrieve("new_etltransaction", etl_LaiID, new ColumnSet(true));
                                            if (etl_Lai != null && etl_Lai.Contains("new_name"))
                                            {
                                                apply_PhaiTraCanTruCRE["new_name"] = (string)etl_Lai["new_name"];
                                            }

                                            apply_PhaiTraCanTruCRE["new_suppliersitecode"] = "Tây Ninh";

                                            apply_PhaiTraCanTruCRE["new_bankcccountnum"] = "CTXL-VND-0";

                                            apply_PhaiTraCanTruCRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienlai"]).Value * (-1));

                                            apply_PhaiTraCanTruCRE["new_referencenumber"] = phieuDNThuNo["new_masophieu"] + "_" + i.ToString();
                                            apply_PhaiTraCanTruCRE["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                            apply_PhaiTraCanTruCRE["new_paymentdocumentname"] = "CANTRU_04";
                                            apply_PhaiTraCanTruCRE["new_vouchernumber"] = "CTND";
                                            apply_PhaiTraCanTruCRE["new_cashflow"] = "00.00";
                                            apply_PhaiTraCanTruCRE["new_paymentnum"] = "1";
                                            apply_PhaiTraCanTruCRE["new_documentnum"] = etl_Lai["new_sochungtu"];//phieuDNThuNo["new_masophieu"].ToString();
                                                                                                                 //apply_PhaiTraCanTruCRE["new_documentsequence"] = phieuDNThuNo["new_lannhan"];

                                            if (phieuDNThuNo.Contains("new_khachhang"))
                                                apply_PhaiTraCanTruCRE["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                            else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                                apply_PhaiTraCanTruCRE["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                            apply_PhaiTraCanTruCRE.Id = service.Create(apply_PhaiTraCanTruCRE);

                                            apply_PhaiTraCanTruCRE["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                                            apply_PhaiTraCanTruCRE["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                                            apply_PhaiTraCanTruCRE["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                                            apply_PhaiTraCanTruCRE["new_type"] = "TYPE4";
                                            //Send(apply_PhaiTraCanTruCRE);
                                            listSend.Add(apply_PhaiTraCanTruCRE);
                                            // }
                                            #endregion

                                            #region CanTruTienLai tu TienThanhToan
                                            Entity apply_CanTruTienNoGoc = new Entity("new_applytransaction");
                                            Entity etl_entity = service.Retrieve("new_etltransaction", etlTienMia, new ColumnSet(new string[] { "new_name" }));
                                            if (etl_entity != null && etl_entity.Contains("new_name"))
                                            {
                                                apply_CanTruTienNoGoc["new_name"] = (string)etl_entity["new_name"];
                                            }
                                            apply_CanTruTienNoGoc["new_suppliersitecode"] = "Tây Ninh";
                                            apply_CanTruTienNoGoc["new_bankcccountnum"] = "CTXL-VND-0";

                                            apply_CanTruTienNoGoc["new_referencenumber"] = phieuDNThuNo["new_masophieu"].ToString() + "_" + i.ToString();
                                            apply_CanTruTienNoGoc["new_paymentamount"] = phieuTinhLai["new_tienlai"];
                                            apply_CanTruTienNoGoc["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                            apply_CanTruTienNoGoc["new_paymentdocumentname"] = "CANTRU_03";
                                            apply_CanTruTienNoGoc["new_vouchernumber"] = "CTND";
                                            apply_CanTruTienNoGoc["new_cashflow"] = "00.00";
                                            apply_CanTruTienNoGoc["new_paymentnum"] = "1";
                                            apply_CanTruTienNoGoc["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();

                                            if (phieuDNThuNo.Contains("new_khachhang"))
                                                apply_CanTruTienNoGoc["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                            else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                                apply_CanTruTienNoGoc["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                            apply_CanTruTienNoGoc.Id = service.Create(apply_CanTruTienNoGoc);

                                            apply_CanTruTienNoGoc["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                                            apply_CanTruTienNoGoc["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                                            apply_CanTruTienNoGoc["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                                            apply_CanTruTienNoGoc["new_type"] = "TYPE4";
                                            //Send(apply_CanTruTienNoGoc);
                                            listSend.Add(apply_CanTruTienNoGoc);
                                            #endregion
                                        }
                                        #endregion
                                    }
                                }
                            }
                            #endregion

                            trace.Trace("Tien van chuyen");
                            // Tien van chuyen
                            #region Tien Van Chuyen
                            if (phieuTTMia.Contains("new_tienxe") && phieuTTMia.Contains("new_hopdongvanchuyen") && ((Money)phieuTTMia["new_tienxe"]).Value > 0)
                            {
                                #region ETL VanChuyen
                                hDVanChuyen = service.Retrieve("new_hopdongvanchuyen", ((EntityReference)phieuTTMia["new_hopdongvanchuyen"]).Id, new ColumnSet(true));

                                decimal tienChiVanChuyen = 0;
                                if (phieuTTMia.Contains("new_tienchivanchuyen") && ((Money)phieuTTMia["new_tienchivanchuyen"]).Value > 0)
                                {
                                    tienChiVanChuyen = ((Money)phieuTTMia["new_tienchivanchuyen"]).Value;
                                }
                                // STA
                                Entity etl_STA = new Entity("new_etltransaction");
                                etl_STA["new_name"] = fullEntity["new_masophieu"].ToString() + "_VC_" + phieuTTMia["new_masophieu"].ToString() + "_STA" + test;
                                etl_STA["new_vouchernumber"] = "MMND";
                                etl_STA["new_transactiontype"] = "3.3.3.d";
                                etl_STA["new_customertype"] = new OptionSetValue(phieuTTMia.Contains("new_doitacvanchuyen") ? 1 : 2);
                                etl_STA["new_season"] = vuMua;
                                //etl_STA["new_vudautu"] = fullEntity["new_vudautu"];
                                etl_STA["new_sochungtu"] = fullEntity["new_masophieu"].ToString() + "_" + phieuTTMia["new_masophieu"].ToString();
                                etl_STA["new_lannhan"] = 1;// fullEntity["new_lannhan"];
                                etl_STA["new_contractnumber"] = hDVanChuyen["new_sohopdongvanchuyen"].ToString();//fullEntity["new_masophieu"].ToString() + "_" + "Tiền vận chuyển (chủ xe)_vụ_" + vuMua;// 
                                etl_STA["new_tradingpartner"] = (khVanChuyen.LogicalName.ToLower().Trim() == "contact" ?
                                    ((khVanChuyen.Contains("new_makhachhang") ? khVanChuyen["new_makhachhang"].ToString() : "") + "_" + (khVanChuyen.Contains("new_socmnd") ? khVanChuyen["new_socmnd"].ToString() : ""))
                                    :
                                    ((khVanChuyen.Contains("new_makhachhang") ? khVanChuyen["new_makhachhang"].ToString() : "") + "_" + (khVanChuyen.Contains("new_masothue") ? khVanChuyen["new_masothue"].ToString() : ""))
                                    );

                                etl_STA["new_suppliersite"] = "TAY NINH";
                                etl_STA["new_invoicedate"] = phieuTTMia["new_ngaylap"];
                                etl_STA["new_descriptionheader"] = "Tiền vận chuyển (chủ xe)_vụ_" + vuMua;
                                etl_STA["new_terms"] = "Tra Ngay";
                                etl_STA["new_taxtype"] = "";
                                etl_STA["new_invoiceamount"] = new Money(((Money)phieuTTMia["new_tienxe"]).Value - tienChiVanChuyen);
                                etl_STA["new_gldate"] = fullEntity["new_ngayduyet"];
                                etl_STA["new_invoicetype"] = "STA";

                                if (phieuTTMia.Contains("new_doitacvanchuyen"))
                                    etl_STA["new_khachhang"] = phieuTTMia["new_doitacvanchuyen"];
                                else if (phieuTTMia.Contains("new_doitacvanchuyenkhdn"))
                                    etl_STA["new_khachhangdoanhnghiep"] = phieuTTMia["new_doitacvanchuyenkhdn"];
                                Guid etl_STAId = service.Create(etl_STA);

                                etl_STA["new_makhachhang"] = khVanChuyen.Contains("new_makhachhang") ? khVanChuyen["new_makhachhang"].ToString() : "";
                                etl_STA["name"] = (khVanChuyen.LogicalName.ToLower() == "contact" ? (khVanChuyen.Contains("fullname") ? khVanChuyen["fullname"].ToString() : "") : (khVanChuyen.Contains("name") ? khVanChuyen["name"].ToString() : ""));
                                etl_STA["new_socmnd"] = (khVanChuyen.LogicalName.ToLower() == "contact" ? (khVanChuyen.Contains("new_socmnd") ? khVanChuyen["new_socmnd"].ToString() : "") : (khVanChuyen.Contains("new_masothue") ? khVanChuyen["new_masothue"].ToString() : ""));
                                etl_STA["tran_type"] = "STA";
                                //etl_STA["new_descriptionlines"] = phieuTTMia["new_masophieu"].ToString();

                                listSend.Add(etl_STA);
                                //Send(etl_STA);
                                #endregion

                                if (tienChiVanChuyen > 0)
                                {
                                    #region STA tien chi van chuyen
                                    Entity etl_STAChiVanChuyen = new Entity("new_etltransaction");
                                    etl_STAChiVanChuyen["new_name"] = fullEntity["new_masophieu"].ToString() + "_CHI_VC_" + phieuTTMia["new_masophieu"].ToString() + "_STA" + test;
                                    etl_STAChiVanChuyen["new_vouchernumber"] = "MMND";
                                    etl_STAChiVanChuyen["new_transactiontype"] = "3.3.3.d";
                                    etl_STAChiVanChuyen["new_customertype"] = new OptionSetValue(phieuTTMia.Contains("new_doitacvanchuyen") ? 1 : 2);
                                    etl_STAChiVanChuyen["new_season"] = vuMua;
                                    //etl_STA["new_vudautu"] = fullEntity["new_vudautu"];
                                    etl_STAChiVanChuyen["new_sochungtu"] = fullEntity["new_masophieu"].ToString() + "_" + phieuTTMia["new_masophieu"].ToString();
                                    etl_STAChiVanChuyen["new_lannhan"] = 1;// fullEntity["new_lannhan"];
                                    etl_STAChiVanChuyen["new_contractnumber"] = hDVanChuyen["new_sohopdongvanchuyen"].ToString();//fullEntity["new_masophieu"].ToString() + "_" + "Tiền vận chuyển (chủ xe)_vụ_" + vuMua;// 
                                    etl_STAChiVanChuyen["new_tradingpartner"] = (khVanChuyen.LogicalName.ToLower().Trim() == "contact" ?
                                        ((khVanChuyen.Contains("new_makhachhang") ? khVanChuyen["new_makhachhang"].ToString() : "") + "_" + (khVanChuyen.Contains("new_socmnd") ? khVanChuyen["new_socmnd"].ToString() : ""))
                                        :
                                        ((khVanChuyen.Contains("new_makhachhang") ? khVanChuyen["new_makhachhang"].ToString() : "") + "_" + (khVanChuyen.Contains("new_masothue") ? khVanChuyen["new_masothue"].ToString() : ""))
                                        );

                                    etl_STAChiVanChuyen["new_suppliersite"] = "TAY NINH";
                                    etl_STAChiVanChuyen["new_invoicedate"] = phieuTTMia["new_ngaylap"];
                                    etl_STAChiVanChuyen["new_descriptionheader"] = "Tiền vận chuyển (chủ xe)_vụ_" + vuMua;
                                    etl_STAChiVanChuyen["new_terms"] = "Tra Ngay";
                                    etl_STAChiVanChuyen["new_taxtype"] = "";
                                    etl_STAChiVanChuyen["new_invoiceamount"] = new Money(tienChiVanChuyen);
                                    etl_STAChiVanChuyen["new_gldate"] = fullEntity["new_ngayduyet"];
                                    etl_STAChiVanChuyen["new_invoicetype"] = "STA";
                                    if (khVanChuyen.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)khVanChuyen["new_phuongthucthanhtoan"]).Value == 100000001)
                                    {
                                        etl_STAChiVanChuyen["new_paymenttype"] = "CK";
                                    }
                                    else
                                    {
                                        etl_STAChiVanChuyen["new_paymenttype"] = "TM";
                                    }

                                    if (phieuTTMia.Contains("new_doitacvanchuyen"))
                                        etl_STAChiVanChuyen["new_khachhang"] = phieuTTMia["new_doitacvanchuyen"];
                                    else if (phieuTTMia.Contains("new_doitacvanchuyenkhdn"))
                                        etl_STAChiVanChuyen["new_khachhangdoanhnghiep"] = phieuTTMia["new_doitacvanchuyenkhdn"];
                                    Guid etl_STAChiVanChuyenId = service.Create(etl_STAChiVanChuyen);

                                    etl_STAChiVanChuyen["new_makhachhang"] = khVanChuyen.Contains("new_makhachhang") ? khVanChuyen["new_makhachhang"].ToString() : "";
                                    etl_STAChiVanChuyen["name"] = (khVanChuyen.LogicalName.ToLower() == "contact" ? (khVanChuyen.Contains("fullname") ? khVanChuyen["fullname"].ToString() : "") : (khVanChuyen.Contains("name") ? khVanChuyen["name"].ToString() : ""));
                                    etl_STAChiVanChuyen["new_socmnd"] = (khVanChuyen.LogicalName.ToLower() == "contact" ? (khVanChuyen.Contains("new_socmnd") ? khVanChuyen["new_socmnd"].ToString() : "") : (khVanChuyen.Contains("new_masothue") ? khVanChuyen["new_masothue"].ToString() : ""));
                                    etl_STAChiVanChuyen["tran_type"] = "STA";
                                    //etl_STA["new_descriptionlines"] = phieuTTMia["new_masophieu"].ToString();

                                    listSend.Add(etl_STAChiVanChuyen);
                                    #endregion

                                    #region ApplyVanChuyen STA
                                    if (khVanChuyen.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)khVanChuyen["new_phuongthucthanhtoan"]).Value == 100000001)
                                    {
                                        // phat sinh apply CRE Lai
                                        Entity apply_STAVC = new Entity("new_applytransaction");

                                        Entity etl_Mia = service.Retrieve("new_etltransaction", etl_STAChiVanChuyenId, new ColumnSet(true));
                                        if (etl_Mia != null && etl_Mia.Contains("new_name"))
                                        {
                                            apply_STAVC["new_name"] = (string)etl_Mia["new_name"];
                                        }

                                        apply_STAVC["new_suppliersitecode"] = "Tây Ninh";

                                        //if (khVanChuyen.Contains("new_taikhoannganhang"))
                                        //{
                                        //    Entity taikhoanchinh = service.Retrieve("new_taikhoannganhang", ((EntityReference)khVanChuyen["new_taikhoannganhang"]).Id, new ColumnSet(true));
                                        //    apply_STAVC["new_supplierbankname"] = (taikhoanchinh == null ? "TTCS-MIA-VND" : taikhoanchinh["new_sotaikhoan"]);
                                        //}

                                        if (khVanChuyen.Contains("new_taikhoannganhangttcs"))
                                        {
                                            Entity taiKhoanTTCS = service.Retrieve("new_taikhoannganhangcuattcs", ((EntityReference)khVanChuyen["new_taikhoannganhangttcs"]).Id, new ColumnSet(true));
                                            apply_STAVC["new_bankcccountnum"] = taiKhoanTTCS["new_name"];
                                        }

                                        //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                                        apply_STAVC["new_paymentamount"] = new Money(tienChiVanChuyen);
                                        //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                                        apply_STAVC["new_referencenumber"] = phieuTTMia["new_masophieu"] + "_" + apply_STAVC["new_name"];
                                        apply_STAVC["new_paymentdate"] = phieuTTMia["new_ngaylap"];
                                        apply_STAVC["new_paymentdocumentname"] = "Tiền vận chuyển (chủ xe)_vụ_" + vuMua;
                                        apply_STAVC["new_vouchernumber"] = "BN";
                                        apply_STAVC["new_cashflow"] = "02.01";
                                        apply_STAVC["new_paymentnum"] = "1";
                                        apply_STAVC["new_documentnum"] = phieuTTMia["new_masophieu"].ToString();
                                        //apply_STAVC["new_documentsequence"] = 1;

                                        if (khVanChuyen.LogicalName == "contact")
                                            apply_STAVC["new_khachhang"] = new EntityReference("contact", khVanChuyen.Id);
                                        else if (khVanChuyen.LogicalName == "account")
                                            apply_STAVC["new_khachhangdoanhnghiep"] = new EntityReference("account", khVanChuyen.Id);

                                        apply_STAVC.Id = service.Create(apply_STAVC);

                                        apply_STAVC["new_makhachhang"] = khVanChuyen.Contains("new_makhachhang") ? khVanChuyen["new_makhachhang"].ToString() : "";
                                        apply_STAVC["name"] = (khVanChuyen.LogicalName.ToLower() == "contact" ? (khVanChuyen.Contains("fullname") ? khVanChuyen["fullname"].ToString() : "") : (khVanChuyen.Contains("name") ? khVanChuyen["name"].ToString() : ""));
                                        apply_STAVC["new_socmnd"] = (khVanChuyen.LogicalName.ToLower() == "contact" ? (khVanChuyen.Contains("new_socmnd") ? khVanChuyen["new_socmnd"].ToString() : "") : (khVanChuyen.Contains("new_masothue") ? khVanChuyen["new_masothue"].ToString() : ""));
                                        apply_STAVC["new_type"] = "TYPE5";
                                        //Send(apply_STAVC);
                                        listSend.Add(apply_STAVC);
                                    }
                                    #endregion
                                }

                                #region Apply van chuyen can tru tu tien mia
                                Entity apply_STACanTruTHVC = new Entity("new_applytransaction");

                                Entity etl_MiaDeCanTru = service.Retrieve("new_etltransaction", etlTienMia, new ColumnSet(true));
                                if (etl_MiaDeCanTru != null && etl_MiaDeCanTru.Contains("new_name"))
                                {
                                    apply_STACanTruTHVC["new_name"] = (string)etl_MiaDeCanTru["new_name"];
                                }

                                apply_STACanTruTHVC["new_suppliersitecode"] = "Tây Ninh";
                                apply_STACanTruTHVC["new_bankcccountnum"] = "CTXL-VND-0";
                                apply_STACanTruTHVC["new_paymentamount"] = (Money)phieuTTMia["new_tienxe"];
                                //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                                apply_STACanTruTHVC["new_referencenumber"] = phieuTTMia["new_masophieu"] + "_VC";
                                apply_STACanTruTHVC["new_paymentdate"] = phieuTTMia["new_ngaylap"];
                                apply_STACanTruTHVC["new_vouchernumber"] = "CTND";
                                apply_STACanTruTHVC["new_cashflow"] = "00.00";
                                apply_STACanTruTHVC["new_paymentnum"] = "1";
                                apply_STACanTruTHVC["new_documentnum"] = phieuTTMia["new_masophieu"].ToString();
                                //apply_STAVC["new_documentsequence"] = 1;

                                if (khNongDan.LogicalName == "contact")
                                    apply_STACanTruTHVC["new_khachhang"] = new EntityReference("contact", khNongDan.Id);
                                else if (khNongDan.LogicalName == "account")
                                    apply_STACanTruTHVC["new_khachhangdoanhnghiep"] = new EntityReference("account", khNongDan.Id);

                                apply_STACanTruTHVC.Id = service.Create(apply_STACanTruTHVC);

                                apply_STACanTruTHVC["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                                apply_STACanTruTHVC["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                                apply_STACanTruTHVC["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                                apply_STACanTruTHVC["new_type"] = "TYPE4";
                                //Send(apply_STACanTruTHVC);
                                listSend.Add(apply_STACanTruTHVC);
                                #endregion
                                // Thu ho van chuyen
                                #region Thu ho GhiNoVanChuyenChoNongDan
                                // CRE
                                Entity etl_CRE = new Entity("new_etltransaction");
                                etl_CRE["new_name"] = fullEntity["new_masophieu"].ToString() + "_THUHO_VC_" + phieuTTMia["new_masophieu"].ToString() + "_CRE" + test;
                                etl_CRE["new_vouchernumber"] = "MMND";
                                etl_CRE["new_transactiontype"] = "3.4.2.d";
                                etl_CRE["new_customertype"] = new OptionSetValue(phieuTTMia.Contains("new_doitacvanchuyen") ? 1 : 2);
                                etl_CRE["new_season"] = vuMua;
                                //etl_STA["new_vudautu"] = fullEntity["new_vudautu"];
                                etl_CRE["new_sochungtu"] = fullEntity["new_masophieu"].ToString() + "_" + phieuTTMia["new_masophieu"].ToString();
                                etl_CRE["new_lannhan"] = 1;// fullEntity["new_lannhan"];
                                etl_CRE["new_contractnumber"] = hDVanChuyen["new_sohopdongvanchuyen"].ToString();//fullEntity["new_masophieu"].ToString() + "_" + "Thu hộ vận chuyển (chủ mía)_vụ_" + vuMua;// 
                                etl_CRE["new_tradingpartner"] = (khNongDan.LogicalName.ToLower().Trim() == "contact" ?
                                    ((khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "") + "_" + (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : ""))
                                    :
                                    ((khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "") + "_" + (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""))
                                    );

                                etl_CRE["new_suppliersite"] = "TAY NINH";
                                etl_CRE["new_invoicedate"] = phieuTTMia["new_ngaylap"];
                                etl_CRE["new_descriptionheader"] = "Thu hộ vận chuyển (chủ mía)_vụ_" + vuMua;
                                etl_CRE["new_terms"] = "Tra Ngay";
                                etl_CRE["new_taxtype"] = "";
                                etl_CRE["new_invoiceamount"] = new Money(((Money)phieuTTMia["new_tienxe"]).Value * (-1));
                                etl_CRE["new_gldate"] = fullEntity["new_ngayduyet"];
                                etl_CRE["new_invoicetype"] = "CRE";

                                if (phieuTTMia.Contains("new_khachhang"))
                                    etl_CRE["new_khachhang"] = phieuTTMia["new_khachhang"];
                                else if (phieuTTMia.Contains("new_khachhangdoanhnghiep"))
                                    etl_CRE["new_khachhangdoanhnghiep"] = phieuTTMia["new_khachhangdoanhnghiep"];
                                Guid etl_CREId = service.Create(etl_CRE);

                                etl_CRE["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                                etl_CRE["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                                etl_CRE["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                                etl_CRE["tran_type"] = "CRE";
                                //etl_CRE["new_descriptionlines"] = phieuTTMia["new_masophieu"].ToString();

                                //Send(etl_CRE);
                                listSend.Add(etl_CRE);
                                #endregion
                                //Phat sinh apply ghi no van chuyen nong dan
                                #region ApplyGhiNoVanChuyenNongDan                        
                                // phat sinh apply CRE van chuyen nong dan
                                Entity apply_CREVC = new Entity("new_applytransaction");

                                Entity etl_VCNDLai = service.Retrieve("new_etltransaction", etl_CREId, new ColumnSet(true));
                                if (etl_VCNDLai != null && etl_VCNDLai.Contains("new_name"))
                                {
                                    apply_CREVC["new_name"] = (string)etl_VCNDLai["new_name"];
                                }

                                apply_CREVC["new_suppliersitecode"] = "Tây Ninh";
                                apply_CREVC["new_bankcccountnum"] = "CTXL-VND-0";

                                //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                                apply_CREVC["new_paymentamount"] = new Money(((Money)phieuTTMia["new_tienxe"]).Value * (-1));

                                //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                                apply_CREVC["new_referencenumber"] = phieuTTMia["new_masophieu"] + "_VC";
                                apply_CREVC["new_paymentdate"] = phieuTTMia["new_ngaylap"];
                                apply_CREVC["new_paymentdocumentname"] = "Thu hộ vận chuyển (chủ mía)_vụ_" + vuMua;
                                apply_CREVC["new_vouchernumber"] = "CTND";
                                apply_CREVC["new_cashflow"] = "00.00";
                                apply_CREVC["new_paymentnum"] = "1";
                                apply_CREVC["new_documentnum"] = phieuTTMia["new_masophieu"].ToString();
                                //apply_CREVC["new_documentsequence"] = 1;

                                if (phieuTTMia.Contains("new_khachhang"))
                                    apply_CREVC["new_khachhang"] = phieuTTMia["new_khachhang"];
                                else if (phieuTTMia.Contains("new_khachhangdoanhnghiep"))
                                    apply_CREVC["new_khachhangdoanhnghiep"] = phieuTTMia["new_khachhangdoanhnghiep"];

                                apply_CREVC.Id = service.Create(apply_CREVC);

                                apply_CREVC["new_makhachhang"] = khVanChuyen.Contains("new_makhachhang") ? khVanChuyen["new_makhachhang"].ToString() : "";
                                apply_CREVC["name"] = (khVanChuyen.LogicalName.ToLower() == "contact" ? (khVanChuyen.Contains("fullname") ? khVanChuyen["fullname"].ToString() : "") : (khVanChuyen.Contains("name") ? khVanChuyen["name"].ToString() : ""));
                                apply_CREVC["new_socmnd"] = (khVanChuyen.LogicalName.ToLower() == "contact" ? (khVanChuyen.Contains("new_socmnd") ? khVanChuyen["new_socmnd"].ToString() : "") : (khVanChuyen.Contains("new_masothue") ? khVanChuyen["new_masothue"].ToString() : ""));
                                apply_CREVC["new_type"] = "TYPE4";
                                //Send(apply_CREVC);
                                listSend.Add(apply_CREVC);
                                //}
                                #endregion

                                //Can tru van chuyen
                                #region CanTruVanChuyen
                                if (phieuTTMia.Contains("new_pdnthuno_vanchuyen"))
                                {
                                    Entity phieuDNThuNo = service.Retrieve("new_phieudenghithuno", ((EntityReference)phieuTTMia["new_pdnthuno_vanchuyen"]).Id, new ColumnSet(true));

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

                                            #region Can tru, Phat sinh CRE hoac PRE
                                            // Can tru
                                            Entity phanBoDauTu = service.Retrieve("new_phanbodautu", ((EntityReference)phieuTinhLai["new_phanbodautu"]).Id, new ColumnSet(true));
                                            if (phanBoDauTu.Contains("new_etltransaction"))
                                            {
                                                Entity etlTransaction = service.Retrieve("new_etltransaction", ((EntityReference)phanBoDauTu["new_etltransaction"]).Id, new ColumnSet(true));
                                                if (etlTransaction.Contains("new_invoicetype") && (etlTransaction["new_invoicetype"].ToString() == "PRE" || etlTransaction["new_invoicetype"].ToString() == "CRE"))
                                                {
                                                    if (etlTransaction.Contains("new_invoicetype") && (etlTransaction["new_invoicetype"].ToString() == "PRE" || etlTransaction["new_invoicetype"].ToString() == "CRE"))
                                                    {
                                                        if (etlTransaction["new_invoicetype"].ToString() == "CRE")
                                                        {
                                                            #region CanTruTienGoc Từ TienThanhToan
                                                            Entity apply_CanTruTienNoGoc = new Entity("new_applytransaction");
                                                            Entity etl_entity = service.Retrieve("new_etltransaction", etl_STAId, new ColumnSet(new string[] { "new_name" }));
                                                            if (etl_entity != null && etl_entity.Contains("new_name"))
                                                            {
                                                                apply_CanTruTienNoGoc["new_name"] = (string)etl_entity["new_name"];
                                                            }

                                                            apply_CanTruTienNoGoc["new_suppliersitecode"] = "Tây Ninh";

                                                            apply_CanTruTienNoGoc["new_bankcccountnum"] = "CTXL-VND-0";

                                                            apply_CanTruTienNoGoc["new_referencenumber"] = phieuDNThuNo["new_masophieu"].ToString() + "_" + i.ToString();
                                                            apply_CanTruTienNoGoc["new_paymentamount"] = phieuTinhLai["new_tienvay"];
                                                            apply_CanTruTienNoGoc["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                                            apply_CanTruTienNoGoc["new_paymentdocumentname"] = "CANTRU_03";
                                                            apply_CanTruTienNoGoc["new_vouchernumber"] = "CTND";
                                                            apply_CanTruTienNoGoc["new_cashflow"] = "00.00";
                                                            apply_CanTruTienNoGoc["new_paymentnum"] = "1";
                                                            apply_CanTruTienNoGoc["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();

                                                            if (phieuDNThuNo.Contains("new_khachhang"))
                                                                apply_CanTruTienNoGoc["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                                            else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                                                apply_CanTruTienNoGoc["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                                            apply_CanTruTienNoGoc.Id = service.Create(apply_CanTruTienNoGoc);

                                                            apply_CanTruTienNoGoc["new_makhachhang"] = khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "";
                                                            apply_CanTruTienNoGoc["name"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("fullname") ? khThuHoach["fullname"].ToString() : "") : (khThuHoach.Contains("name") ? khThuHoach["name"].ToString() : ""));
                                                            apply_CanTruTienNoGoc["new_socmnd"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : "") : (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""));
                                                            apply_CanTruTienNoGoc["new_type"] = "TYPE4";
                                                            //Send(apply_CanTruTienNoGoc);
                                                            listSend.Add(apply_CanTruTienNoGoc);
                                                            #endregion
                                                        }

                                                        #region Apply tien vay
                                                        Entity apply_PhaiTraCanTruPRE = new Entity("new_applytransaction");
                                                        if (etlTransaction["new_invoicetype"].ToString() == "PRE")
                                                        {
                                                            Entity etl_entity = service.Retrieve("new_etltransaction", etl_STAId, new ColumnSet(new string[] { "new_name" }));
                                                            apply_PhaiTraCanTruPRE["new_name"] = (string)etl_entity["new_name"];
                                                        }
                                                        else
                                                        {
                                                            if (etlTransaction != null && etlTransaction.Contains("new_name"))
                                                            {
                                                                apply_PhaiTraCanTruPRE["new_name"] = (string)etlTransaction["new_name"];
                                                            }
                                                            apply_PhaiTraCanTruPRE["new_bankcccountnum"] = "CTXL-VND-0";
                                                        }

                                                        apply_PhaiTraCanTruPRE["new_suppliersitecode"] = "Tây Ninh";

                                                        apply_PhaiTraCanTruPRE["new_referencenumber"] = phieuDNThuNo["new_masophieu"] + "_" + i.ToString();
                                                        apply_PhaiTraCanTruPRE["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                                        apply_PhaiTraCanTruPRE["new_paymentdocumentname"] = "CANTRU_04";
                                                        apply_PhaiTraCanTruPRE["new_vouchernumber"] = "CTND";
                                                        apply_PhaiTraCanTruPRE["new_cashflow"] = "00.00";
                                                        apply_PhaiTraCanTruPRE["new_paymentnum"] = "1";
                                                        if (etlTransaction["new_invoicetype"].ToString() == "PRE")
                                                        {
                                                            apply_PhaiTraCanTruPRE["new_prepay_num"] = etlTransaction["new_name"];
                                                            apply_PhaiTraCanTruPRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienvay"]).Value);
                                                        }
                                                        else
                                                        {
                                                            apply_PhaiTraCanTruPRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienvay"]).Value * (-1));
                                                        }
                                                        apply_PhaiTraCanTruPRE["new_documentnum"] = etlTransaction["new_sochungtu"];//phieuDNThuNo["new_masophieu"].ToString();

                                                        if (phieuDNThuNo.Contains("new_khachhang"))
                                                            apply_PhaiTraCanTruPRE["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                                        else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                                            apply_PhaiTraCanTruPRE["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                                        apply_PhaiTraCanTruPRE.Id = service.Create(apply_PhaiTraCanTruPRE);

                                                        apply_PhaiTraCanTruPRE["new_makhachhang"] = khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "";
                                                        apply_PhaiTraCanTruPRE["name"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("fullname") ? khThuHoach["fullname"].ToString() : "") : (khThuHoach.Contains("name") ? khThuHoach["name"].ToString() : ""));
                                                        apply_PhaiTraCanTruPRE["new_socmnd"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : "") : (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""));
                                                        apply_PhaiTraCanTruPRE["new_type"] = etlTransaction["new_invoicetype"].ToString() == "PRE" ? "TYPE2" : "TYPE4";
                                                        //Send(apply_PhaiTraCanTruPRE);
                                                        listSend.Add(apply_PhaiTraCanTruPRE);
                                                        #endregion
                                                    }
                                                }
                                            }
                                            #endregion
                                            ++i;
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
                                                etl_ND["new_season"] = vuMua;
                                                //etl_ND["new_vudautu"] = fullEntity["new_vudautu"];
                                                etl_ND["new_sochungtu"] = phieuDNThuNo["new_masophieu"].ToString();
                                                //etl_ND["new_lannhan"] = phieuNghiemThu["new_lannhan"];
                                                etl_ND["new_contractnumber"] = phanBoDauTu["new_maphieuphanbo"].ToString();// lay hop dong mia tren phieu nghiem thu
                                                etl_ND["new_tradingpartner"] = (khVanChuyen.LogicalName.ToLower().Trim() == "contact" ? // phieu nghiem thu
                                                    ((khVanChuyen.Contains("new_makhachhang") ? khVanChuyen["new_makhachhang"].ToString() : "") + "_" + (khVanChuyen.Contains("new_socmnd") ? khVanChuyen["new_socmnd"].ToString() : ""))
                                                    :
                                                    ((khVanChuyen.Contains("new_makhachhang") ? khVanChuyen["new_makhachhang"].ToString() : "") + "_" + (khVanChuyen.Contains("new_masothue") ? khVanChuyen["new_masothue"].ToString() : ""))
                                                    );
                                                etl_ND["new_suppliernumber"] = khVanChuyen["new_makhachhang"].ToString();
                                                etl_ND["new_suppliersite"] = "TAY NINH";
                                                etl_ND["new_invoicedate"] = phieuTTMia["new_ngaylap"];// lay ngay nghiem thu (ngay thuc hien)
                                                etl_ND["new_descriptionheader"] = "Ghi nợ lãi cấn trừ vận chuyển_vụ_" + vuMua;
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

                                                etl_ND["new_makhachhang"] = khVanChuyen.Contains("new_makhachhang") ? khVanChuyen["new_makhachhang"].ToString() : "";
                                                etl_ND["name"] = (khVanChuyen.LogicalName.ToLower() == "contact" ? (khVanChuyen.Contains("fullname") ? khVanChuyen["fullname"].ToString() : "") : (khVanChuyen.Contains("name") ? khVanChuyen["name"].ToString() : ""));
                                                etl_ND["new_socmnd"] = (khVanChuyen.LogicalName.ToLower() == "contact" ? (khVanChuyen.Contains("new_socmnd") ? khVanChuyen["new_socmnd"].ToString() : "") : (khVanChuyen.Contains("new_masothue") ? khVanChuyen["new_masothue"].ToString() : ""));
                                                //etl_ND["new_descriptionlines"] = phieuDNThuNo["new_name"].ToString();
                                                etl_ND["tran_type"] = "CRE";

                                                //Send(etl_ND);
                                                listSend.Add(etl_ND);
                                                #endregion

                                                #region Phat sinh Apply                                         
                                                // phat sinh apply CRE Lai
                                                Entity apply_PhaiTraCanTruCRE = new Entity("new_applytransaction");

                                                Entity etl_Lai = service.Retrieve("new_etltransaction", etl_LaiID, new ColumnSet(true));
                                                if (etl_Lai != null && etl_Lai.Contains("new_name"))
                                                {
                                                    apply_PhaiTraCanTruCRE["new_name"] = (string)etl_Lai["new_name"];
                                                }

                                                apply_PhaiTraCanTruCRE["new_suppliersitecode"] = "Tây Ninh";
                                                apply_PhaiTraCanTruCRE["new_bankcccountnum"] = "CTXL-VND-0";

                                                //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                                                apply_PhaiTraCanTruCRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienlai"]).Value * (-1));

                                                //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                                                apply_PhaiTraCanTruCRE["new_referencenumber"] = phieuDNThuNo["new_masophieu"] + "_" + i.ToString();
                                                apply_PhaiTraCanTruCRE["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                                apply_PhaiTraCanTruCRE["new_paymentdocumentname"] = "CANTRU_04";
                                                apply_PhaiTraCanTruCRE["new_vouchernumber"] = "CTND";
                                                apply_PhaiTraCanTruCRE["new_cashflow"] = "00.00";
                                                apply_PhaiTraCanTruCRE["new_paymentnum"] = "1";
                                                apply_PhaiTraCanTruCRE["new_documentnum"] = etl_Lai["new_sochungtu"];//phieuDNThuNo["new_masophieu"].ToString();

                                                if (phieuDNThuNo.Contains("new_khachhang"))
                                                    apply_PhaiTraCanTruCRE["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                                else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                                    apply_PhaiTraCanTruCRE["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                                apply_PhaiTraCanTruCRE.Id = service.Create(apply_PhaiTraCanTruCRE);

                                                apply_PhaiTraCanTruCRE["new_makhachhang"] = khVanChuyen.Contains("new_makhachhang") ? khVanChuyen["new_makhachhang"].ToString() : "";
                                                apply_PhaiTraCanTruCRE["name"] = (khVanChuyen.LogicalName.ToLower() == "contact" ? (khVanChuyen.Contains("fullname") ? khVanChuyen["fullname"].ToString() : "") : (khVanChuyen.Contains("name") ? khVanChuyen["name"].ToString() : ""));
                                                apply_PhaiTraCanTruCRE["new_socmnd"] = (khVanChuyen.LogicalName.ToLower() == "contact" ? (khVanChuyen.Contains("new_socmnd") ? khVanChuyen["new_socmnd"].ToString() : "") : (khVanChuyen.Contains("new_masothue") ? khVanChuyen["new_masothue"].ToString() : ""));
                                                apply_PhaiTraCanTruCRE["new_type"] = "TYPE4";
                                                //Send(apply_PhaiTraCanTruCRE);
                                                listSend.Add(apply_PhaiTraCanTruCRE);
                                                //}
                                                #endregion

                                                #region CanTruTienLai tu TienThanhToan
                                                Entity apply_CanTruTienNoGoc = new Entity("new_applytransaction");
                                                Entity etl_entity = service.Retrieve("new_etltransaction", etl_STAId, new ColumnSet(new string[] { "new_name" }));
                                                if (etl_entity != null && etl_entity.Contains("new_name"))
                                                {
                                                    apply_CanTruTienNoGoc["new_name"] = (string)etl_entity["new_name"];
                                                }
                                                apply_CanTruTienNoGoc["new_suppliersitecode"] = "Tây Ninh";
                                                apply_CanTruTienNoGoc["new_bankcccountnum"] = "CTXL-VND-0";

                                                apply_CanTruTienNoGoc["new_referencenumber"] = phieuDNThuNo["new_masophieu"].ToString() + "_" + i.ToString();
                                                apply_CanTruTienNoGoc["new_paymentamount"] = phieuTinhLai["new_tienlai"];
                                                apply_CanTruTienNoGoc["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                                apply_CanTruTienNoGoc["new_paymentdocumentname"] = "CANTRU_03";
                                                apply_CanTruTienNoGoc["new_vouchernumber"] = "CTND";
                                                apply_CanTruTienNoGoc["new_cashflow"] = "00.00";
                                                apply_CanTruTienNoGoc["new_paymentnum"] = "1";
                                                apply_CanTruTienNoGoc["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();

                                                if (phieuDNThuNo.Contains("new_khachhang"))
                                                    apply_CanTruTienNoGoc["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                                else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                                    apply_CanTruTienNoGoc["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                                apply_CanTruTienNoGoc.Id = service.Create(apply_CanTruTienNoGoc);

                                                apply_CanTruTienNoGoc["new_makhachhang"] = khVanChuyen.Contains("new_makhachhang") ? khVanChuyen["new_makhachhang"].ToString() : "";
                                                apply_CanTruTienNoGoc["name"] = (khVanChuyen.LogicalName.ToLower() == "contact" ? (khVanChuyen.Contains("fullname") ? khVanChuyen["fullname"].ToString() : "") : (khVanChuyen.Contains("name") ? khVanChuyen["name"].ToString() : ""));
                                                apply_CanTruTienNoGoc["new_socmnd"] = (khVanChuyen.LogicalName.ToLower() == "contact" ? (khVanChuyen.Contains("new_socmnd") ? khVanChuyen["new_socmnd"].ToString() : "") : (khVanChuyen.Contains("new_masothue") ? khVanChuyen["new_masothue"].ToString() : ""));
                                                apply_CanTruTienNoGoc["new_type"] = "TYPE4";
                                                //Send(apply_CanTruTienNoGoc);
                                                listSend.Add(apply_CanTruTienNoGoc);
                                                #endregion
                                            }
                                            #endregion
                                        }
                                    }
                                }

                                #endregion

                            }
                            #endregion

                            trace.Trace("Tien cong don");
                            // Tien cong don
                            #region ChiTienCongDon
                            if (phieuTTMia.Contains("new_tiencongdon") && phieuTTMia.Contains("new_hopdongthuhoach") && ((Money)phieuTTMia["new_tiencongdon"]).Value > 0)
                            {
                                #region ETL CongDon
                                hDThuHoach = service.Retrieve("new_hopdongthuhoach", ((EntityReference)phieuTTMia["new_hopdongthuhoach"]).Id, new ColumnSet(true));
                                decimal tienChiDauCong = 0;
                                if (phieuTTMia.Contains("new_tienchidaucong") && ((Money)phieuTTMia["new_tienchidaucong"]).Value > 0)
                                {
                                    tienChiDauCong = ((Money)phieuTTMia["new_tienchidaucong"]).Value;
                                }
                                // STA
                                Entity etl_STA = new Entity("new_etltransaction");
                                etl_STA["new_name"] = fullEntity["new_masophieu"].ToString() + "_CD_" + phieuTTMia["new_masophieu"].ToString() + "_STA" + test;
                                etl_STA["new_vouchernumber"] = "MMND";
                                etl_STA["new_transactiontype"] = "3.3.3.e";
                                etl_STA["new_customertype"] = new OptionSetValue(phieuTTMia.Contains("new_doitacthuhoach") ? 1 : 2);
                                etl_STA["new_season"] = vuMua;
                                //etl_STA["new_vudautu"] = fullEntity["new_vudautu"];
                                etl_STA["new_sochungtu"] = fullEntity["new_masophieu"].ToString() + "_" + phieuTTMia["new_masophieu"].ToString();
                                etl_STA["new_lannhan"] = 1;// fullEntity["new_lannhan"];
                                etl_STA["new_contractnumber"] = hDThuHoach["new_sohopdong"].ToString();//fullEntity["new_masophieu"].ToString() + "_" + "Tiền công đốn (đầu công)_vụ_" + vuMua;//
                                etl_STA["new_tradingpartner"] = (khThuHoach.LogicalName.ToLower().Trim() == "contact" ?
                                    ((khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "") + "_" + (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : ""))
                                    :
                                    ((khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "") + "_" + (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""))
                                    );

                                etl_STA["new_suppliersite"] = "TAY NINH";
                                etl_STA["new_invoicedate"] = phieuTTMia["new_ngaylap"];
                                etl_STA["new_descriptionheader"] = "Tiền công đốn (đầu công)_vụ_" + vuMua;
                                etl_STA["new_terms"] = "Tra Ngay";
                                etl_STA["new_taxtype"] = "";
                                etl_STA["new_invoiceamount"] = new Money(((Money)phieuTTMia["new_tiencongdon"]).Value - tienChiDauCong);
                                etl_STA["new_gldate"] = fullEntity["new_ngayduyet"];
                                etl_STA["new_invoicetype"] = "STA";

                                if (phieuTTMia.Contains("new_doitacthuhoach"))
                                    etl_STA["new_khachhang"] = phieuTTMia["new_doitacthuhoach"];
                                else if (phieuTTMia.Contains("new_doitacthuhoachkhdn"))
                                    etl_STA["new_khachhangdoanhnghiep"] = phieuTTMia["new_doitacthuhoachkhdn"];
                                Guid etl_STAId = service.Create(etl_STA);

                                etl_STA["new_makhachhang"] = khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "";
                                etl_STA["name"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("fullname") ? khThuHoach["fullname"].ToString() : "") : (khThuHoach.Contains("name") ? khThuHoach["name"].ToString() : ""));
                                etl_STA["new_socmnd"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : "") : (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""));
                                etl_STA["tran_type"] = "STA";
                                //etl_STA["new_descriptionlines"] = phieuTTMia["new_masophieu"].ToString();

                                //Send(etl_STA);
                                listSend.Add(etl_STA);
                                #endregion

                                if (tienChiDauCong > 0)
                                {
                                    #region Tien chi dau cong
                                    // STA
                                    Entity etl_STAChiDauCong = new Entity("new_etltransaction");
                                    etl_STAChiDauCong["new_name"] = fullEntity["new_masophieu"].ToString() + "_CHI_CD_" + phieuTTMia["new_masophieu"].ToString() + "_STA" + test;
                                    etl_STAChiDauCong["new_vouchernumber"] = "MMND";
                                    etl_STAChiDauCong["new_transactiontype"] = "3.3.3.e";
                                    etl_STAChiDauCong["new_customertype"] = new OptionSetValue(phieuTTMia.Contains("new_doitacthuhoach") ? 1 : 2);
                                    etl_STAChiDauCong["new_season"] = vuMua;
                                    //etl_STA["new_vudautu"] = fullEntity["new_vudautu"];
                                    etl_STAChiDauCong["new_sochungtu"] = fullEntity["new_masophieu"].ToString() + "_" + phieuTTMia["new_masophieu"].ToString();
                                    etl_STAChiDauCong["new_lannhan"] = 1;// fullEntity["new_lannhan"];
                                    etl_STAChiDauCong["new_contractnumber"] = hDThuHoach["new_sohopdong"].ToString();//fullEntity["new_masophieu"].ToString() + "_" + "Tiền công đốn (đầu công)_vụ_" + vuMua;//
                                    etl_STAChiDauCong["new_tradingpartner"] = (khThuHoach.LogicalName.ToLower().Trim() == "contact" ?
                                        ((khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "") + "_" + (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : ""))
                                        :
                                        ((khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "") + "_" + (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""))
                                        );

                                    etl_STAChiDauCong["new_suppliersite"] = "TAY NINH";
                                    etl_STAChiDauCong["new_invoicedate"] = phieuTTMia["new_ngaylap"];
                                    etl_STAChiDauCong["new_descriptionheader"] = "Tiền công đốn (đầu công)_vụ_" + vuMua;
                                    etl_STAChiDauCong["new_terms"] = "Tra Ngay";
                                    etl_STAChiDauCong["new_taxtype"] = "";
                                    etl_STAChiDauCong["new_invoiceamount"] = new Money(tienChiDauCong);
                                    etl_STAChiDauCong["new_gldate"] = fullEntity["new_ngayduyet"];
                                    etl_STAChiDauCong["new_invoicetype"] = "STA";
                                    if (khThuHoach.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)khThuHoach["new_phuongthucthanhtoan"]).Value == 100000001)
                                    {
                                        etl_STAChiDauCong["new_paymenttype"] = "CK";
                                    }
                                    else
                                    {
                                        etl_STAChiDauCong["new_paymenttype"] = "TM";
                                    }

                                    if (phieuTTMia.Contains("new_doitacthuhoach"))
                                        etl_STAChiDauCong["new_khachhang"] = phieuTTMia["new_doitacthuhoach"];
                                    else if (phieuTTMia.Contains("new_doitacthuhoachkhdn"))
                                        etl_STAChiDauCong["new_khachhangdoanhnghiep"] = phieuTTMia["new_doitacthuhoachkhdn"];
                                    Guid etl_STAChiDauCongId = service.Create(etl_STAChiDauCong);

                                    etl_STAChiDauCong["new_makhachhang"] = khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "";
                                    etl_STAChiDauCong["name"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("fullname") ? khThuHoach["fullname"].ToString() : "") : (khThuHoach.Contains("name") ? khThuHoach["name"].ToString() : ""));
                                    etl_STAChiDauCong["new_socmnd"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : "") : (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""));
                                    etl_STAChiDauCong["tran_type"] = "STA";
                                    //etl_STA["new_descriptionlines"] = phieuTTMia["new_masophieu"].ToString();

                                    //Send(etl_STA);
                                    listSend.Add(etl_STAChiDauCong);
                                    #endregion

                                    #region Apply Cong Don
                                    if (khThuHoach.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)khThuHoach["new_phuongthucthanhtoan"]).Value == 100000001)
                                    {
                                        Entity apply_STAVC = new Entity("new_applytransaction");

                                        Entity etl_Mia = service.Retrieve("new_etltransaction", etl_STAChiDauCongId, new ColumnSet(true));
                                        if (etl_Mia != null && etl_Mia.Contains("new_name"))
                                        {
                                            apply_STAVC["new_name"] = (string)etl_Mia["new_name"];
                                        }

                                        apply_STAVC["new_suppliersitecode"] = "Tây Ninh";

                                        /*if (khThuHoach.Contains("new_taikhoannganhang"))
                                        {
                                            Entity taikhoanchinh = service.Retrieve("new_taikhoannganhang", ((EntityReference)khThuHoach["new_taikhoannganhang"]).Id, new ColumnSet(true));
                                            //apply_STAVC["new_supplierbankname"] = (taikhoanchinh == null ? "TTCS-MIA-VND" : taikhoanchinh["new_sotaikhoan"]);
                                            apply_STAVC["new_supplierbankname"] = (taikhoanchinh == null ? "TTCS-MIA-VND" : taikhoanchinh["new_sotaikhoan"]);
                                        }*/

                                        if (khThuHoach.Contains("new_taikhoannganhangttcs"))
                                        {
                                            Entity taiKhoanTTCS = service.Retrieve("new_taikhoannganhangcuattcs", ((EntityReference)khThuHoach["new_taikhoannganhangttcs"]).Id, new ColumnSet(true));
                                            apply_STAVC["new_bankcccountnum"] = taiKhoanTTCS["new_name"];
                                        }

                                        //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                                        apply_STAVC["new_paymentamount"] = new Money(tienChiDauCong);
                                        //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                                        apply_STAVC["new_referencenumber"] = phieuTTMia["new_masophieu"];
                                        apply_STAVC["new_paymentdate"] = phieuTTMia["new_ngaylap"];
                                        apply_STAVC["new_paymentdocumentname"] = "Tiền công đốn (đầu công)_vụ_" + vuMua;
                                        apply_STAVC["new_vouchernumber"] = "BN";
                                        apply_STAVC["new_cashflow"] = "02.01";
                                        apply_STAVC["new_paymentnum"] = "1";
                                        apply_STAVC["new_documentnum"] = phieuTTMia["new_masophieu"].ToString();
                                        //apply_STAVC["new_documentsequence"] = 1;

                                        if (khThuHoach.LogicalName == "contact")
                                            apply_STAVC["new_khachhang"] = new EntityReference("contact", khThuHoach.Id);
                                        else if (khThuHoach.LogicalName == "account")
                                            apply_STAVC["new_khachhangdoanhnghiep"] = new EntityReference("account", khThuHoach.Id);

                                        apply_STAVC.Id = service.Create(apply_STAVC);

                                        apply_STAVC["new_makhachhang"] = khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "";
                                        apply_STAVC["name"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("fullname") ? khThuHoach["fullname"].ToString() : "") : (khThuHoach.Contains("name") ? khThuHoach["name"].ToString() : ""));
                                        apply_STAVC["new_socmnd"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : "") : (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""));
                                        apply_STAVC["new_type"] = "TYPE5";
                                        //Send(apply_STAVC);
                                        listSend.Add(apply_STAVC);
                                    }
                                    #endregion
                                }

                                #region Apply cong don can tru tu tien mia
                                Entity apply_STACanTruTHCD = new Entity("new_applytransaction");

                                Entity etl_MiaDeCanTru = service.Retrieve("new_etltransaction", etlTienMia, new ColumnSet(true));
                                if (etl_MiaDeCanTru != null && etl_MiaDeCanTru.Contains("new_name"))
                                {
                                    apply_STACanTruTHCD["new_name"] = (string)etl_MiaDeCanTru["new_name"];
                                }

                                apply_STACanTruTHCD["new_suppliersitecode"] = "Tây Ninh";
                                apply_STACanTruTHCD["new_bankcccountnum"] = "CTXL-VND-0";

                                apply_STACanTruTHCD["new_paymentamount"] = (Money)phieuTTMia["new_tiencongdon"];

                                //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                                apply_STACanTruTHCD["new_referencenumber"] = phieuTTMia["new_masophieu"] + "_CD";
                                apply_STACanTruTHCD["new_paymentdate"] = phieuTTMia["new_ngaylap"];
                                apply_STACanTruTHCD["new_vouchernumber"] = "CTND";
                                apply_STACanTruTHCD["new_cashflow"] = "00.00";
                                apply_STACanTruTHCD["new_paymentnum"] = "1";
                                apply_STACanTruTHCD["new_documentnum"] = phieuTTMia["new_masophieu"].ToString();
                                //apply_STAVC["new_documentsequence"] = 1;

                                if (khNongDan.LogicalName == "contact")
                                    apply_STACanTruTHCD["new_khachhang"] = new EntityReference("contact", khNongDan.Id);
                                else if (khNongDan.LogicalName == "account")
                                    apply_STACanTruTHCD["new_khachhangdoanhnghiep"] = new EntityReference("account", khNongDan.Id);

                                apply_STACanTruTHCD.Id = service.Create(apply_STACanTruTHCD);

                                apply_STACanTruTHCD["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                                apply_STACanTruTHCD["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                                apply_STACanTruTHCD["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                                apply_STACanTruTHCD["new_type"] = "TYPE4";
                                //Send(apply_STACanTruTHCD);
                                listSend.Add(apply_STACanTruTHCD);
                                #endregion

                                // Thu ho cong don
                                #region ThuHoCongDon GhiNoVanCongDonNongDan
                                // CRE                        
                                Entity etl_CRE = new Entity("new_etltransaction");
                                etl_CRE["new_name"] = fullEntity["new_masophieu"].ToString() + "_THUHO_CD_" + phieuTTMia["new_masophieu"].ToString() + "_CRE" + test;
                                etl_CRE["new_vouchernumber"] = "MMND";
                                etl_CRE["new_transactiontype"] = "3.4.2.e";
                                etl_CRE["new_customertype"] = new OptionSetValue(phieuTTMia.Contains("new_doitacthuhoach") ? 1 : 2);
                                etl_CRE["new_season"] = vuMua;
                                //etl_STA["new_vudautu"] = fullEntity["new_vudautu"];
                                etl_CRE["new_sochungtu"] = fullEntity["new_masophieu"].ToString() + "_" + phieuTTMia["new_masophieu"].ToString();
                                etl_CRE["new_lannhan"] = 1;// fullEntity["new_lannhan"];
                                etl_CRE["new_contractnumber"] = hDThuHoach["new_sohopdong"].ToString();//fullEntity["new_masophieu"].ToString() + "_" + "Thu hộ công đốn (chủ mía)_vụ_" + vuMua;//
                                etl_CRE["new_tradingpartner"] = (khNongDan.LogicalName.ToLower().Trim() == "contact" ?
                                    ((khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "") + "_" + (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : ""))
                                    :
                                    ((khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "") + "_" + (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""))
                                    );

                                etl_CRE["new_suppliersite"] = "TAY NINH";
                                etl_CRE["new_invoicedate"] = phieuTTMia["new_ngaylap"];
                                etl_CRE["new_descriptionheader"] = "Thu hộ công đốn (chủ mía)_vụ_" + vuMua;
                                etl_CRE["new_terms"] = "Tra Ngay";
                                etl_CRE["new_taxtype"] = "";
                                etl_CRE["new_invoiceamount"] = new Money(((Money)phieuTTMia["new_tiencongdon"]).Value * (-1));
                                etl_CRE["new_gldate"] = fullEntity["new_ngayduyet"];
                                etl_CRE["new_invoicetype"] = "CRE";
                                //if (khNongDan.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)khNongDan["new_phuongthucthanhtoan"]).Value == 100000001)
                                //{
                                //    etl_CRE["new_paymenttype"] = "CK";
                                //}
                                //else
                                //{
                                //    etl_CRE["new_paymenttype"] = "TM";
                                //}

                                if (phieuTTMia.Contains("new_doitacthuhoach"))
                                    etl_CRE["new_khachhang"] = phieuTTMia["new_doitacthuhoach"];
                                else if (phieuTTMia.Contains("new_doitacthuhoachkhdn"))
                                    etl_CRE["new_khachhangdoanhnghiep"] = phieuTTMia["new_doitacthuhoachkhdn"];
                                Guid etl_CREId = service.Create(etl_CRE);

                                etl_CRE["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                                etl_CRE["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                                etl_CRE["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                                etl_CRE["tran_type"] = "CRE";
                                //etl_CRE["new_descriptionlines"] = phieuTTMia["new_masophieu"].ToString();

                                //Send(etl_CRE);
                                listSend.Add(etl_CRE);
                                #endregion

                                // Apply thu ho cong don
                                #region ApplyThuHoCongDong - ApplyGhiNoVanChuyenNongDan
                                //if (khNongDan.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)khNongDan["new_phuongthucthanhtoan"]).Value == 100000001)
                                //{
                                // phat sinh apply CRE van chuyen nong dan
                                Entity apply_CRETH = new Entity("new_applytransaction");

                                Entity etl_VCLai = service.Retrieve("new_etltransaction", etl_CREId, new ColumnSet(true));
                                if (etl_VCLai != null && etl_VCLai.Contains("new_name"))
                                {
                                    apply_CRETH["new_name"] = (string)etl_VCLai["new_name"];
                                }

                                apply_CRETH["new_suppliersitecode"] = "Tây Ninh";

                                apply_CRETH["new_bankcccountnum"] = "CTXL-VND-0";

                                //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                                apply_CRETH["new_paymentamount"] = new Money(((Money)phieuTTMia["new_tiencongdon"]).Value * (-1));

                                //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                                apply_CRETH["new_referencenumber"] = phieuTTMia["new_masophieu"] + "_CD";
                                apply_CRETH["new_paymentdate"] = phieuTTMia["new_ngaylap"];
                                apply_CRETH["new_paymentdocumentname"] = "Thu hộ công đốn (chủ mía)_vụ_" + vuMua;
                                apply_CRETH["new_vouchernumber"] = "CTND";
                                apply_CRETH["new_cashflow"] = "00.00";
                                apply_CRETH["new_paymentnum"] = "1";
                                apply_CRETH["new_documentnum"] = phieuTTMia["new_masophieu"].ToString();
                                //apply_CRETH["new_documentsequence"] = 1;

                                if (phieuTTMia.Contains("new_khachhang"))
                                    apply_CRETH["new_khachhang"] = phieuTTMia["new_khachhang"];
                                else if (phieuTTMia.Contains("new_khachhangdoanhnghiep"))
                                    apply_CRETH["new_khachhangdoanhnghiep"] = phieuTTMia["new_khachhangdoanhnghiep"];

                                apply_CRETH.Id = service.Create(apply_CRETH);

                                apply_CRETH["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                                apply_CRETH["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                                apply_CRETH["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                                apply_CRETH["new_type"] = "TYPE4";
                                //Send(apply_CRETH);
                                listSend.Add(apply_CRETH);
                                //}
                                #endregion

                                //Can tru thu hoach - cong don
                                #region CanTruCongDon
                                if (phieuTTMia.Contains("new_pdnthuno_thuhoach"))
                                {
                                    Entity phieuDNThuNo = service.Retrieve("new_phieudenghithuno", ((EntityReference)phieuTTMia["new_pdnthuno_thuhoach"]).Id, new ColumnSet(true));

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
                                            #region Can tru, Phat sinh CRE hoac PRE
                                            // Can tru
                                            Entity phanBoDauTu = service.Retrieve("new_phanbodautu", ((EntityReference)phieuTinhLai["new_phanbodautu"]).Id, new ColumnSet(true));
                                            if (phanBoDauTu.Contains("new_etltransaction"))
                                            {
                                                Entity etlTransaction = service.Retrieve("new_etltransaction", ((EntityReference)phanBoDauTu["new_etltransaction"]).Id, new ColumnSet(true));
                                                if (etlTransaction.Contains("new_invoicetype") && (etlTransaction["new_invoicetype"].ToString() == "PRE" || etlTransaction["new_invoicetype"].ToString() == "CRE"))
                                                {
                                                    if (etlTransaction["new_invoicetype"].ToString() == "CRE")
                                                    {
                                                        #region CanTruTienGoc Từ TienThanhToan
                                                        Entity apply_CanTruTienNoGoc = new Entity("new_applytransaction");
                                                        Entity etl_entity = service.Retrieve("new_etltransaction", etl_STAId, new ColumnSet(new string[] { "new_name" }));
                                                        if (etl_entity != null && etl_entity.Contains("new_name"))
                                                        {
                                                            apply_CanTruTienNoGoc["new_name"] = (string)etl_entity["new_name"];
                                                        }

                                                        apply_CanTruTienNoGoc["new_suppliersitecode"] = "Tây Ninh";

                                                        apply_CanTruTienNoGoc["new_bankcccountnum"] = "CTXL-VND-0";

                                                        apply_CanTruTienNoGoc["new_referencenumber"] = phieuDNThuNo["new_masophieu"].ToString() + "_" + i.ToString();
                                                        apply_CanTruTienNoGoc["new_paymentamount"] = phieuTinhLai["new_tienvay"];
                                                        apply_CanTruTienNoGoc["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                                        apply_CanTruTienNoGoc["new_paymentdocumentname"] = "CANTRU_03";
                                                        apply_CanTruTienNoGoc["new_vouchernumber"] = "CTND";
                                                        apply_CanTruTienNoGoc["new_cashflow"] = "00.00";
                                                        apply_CanTruTienNoGoc["new_paymentnum"] = "1";
                                                        apply_CanTruTienNoGoc["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();

                                                        if (phieuDNThuNo.Contains("new_khachhang"))
                                                            apply_CanTruTienNoGoc["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                                        else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                                            apply_CanTruTienNoGoc["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                                        apply_CanTruTienNoGoc.Id = service.Create(apply_CanTruTienNoGoc);

                                                        apply_CanTruTienNoGoc["new_makhachhang"] = khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "";
                                                        apply_CanTruTienNoGoc["name"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("fullname") ? khThuHoach["fullname"].ToString() : "") : (khThuHoach.Contains("name") ? khThuHoach["name"].ToString() : ""));
                                                        apply_CanTruTienNoGoc["new_socmnd"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : "") : (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""));
                                                        apply_CanTruTienNoGoc["new_type"] = "TYPE4";
                                                        //Send(apply_CanTruTienNoGoc);
                                                        listSend.Add(apply_CanTruTienNoGoc);
                                                        #endregion
                                                    }
                                                    #region Apply Tien vay
                                                    Entity apply_PhaiTraCanTruPRE = new Entity("new_applytransaction");
                                                    if (etlTransaction["new_invoicetype"].ToString() == "PRE")
                                                    {
                                                        Entity etl_entity = service.Retrieve("new_etltransaction", etl_STAId, new ColumnSet(new string[] { "new_name" }));
                                                        apply_PhaiTraCanTruPRE["new_name"] = (string)etl_entity["new_name"];
                                                    }
                                                    else
                                                    {
                                                        if (etlTransaction != null && etlTransaction.Contains("new_name"))
                                                        {
                                                            apply_PhaiTraCanTruPRE["new_name"] = (string)etlTransaction["new_name"];
                                                        }
                                                        apply_PhaiTraCanTruPRE["new_bankcccountnum"] = "CTXL-VND-0";
                                                    }

                                                    apply_PhaiTraCanTruPRE["new_suppliersitecode"] = "Tây Ninh";

                                                    apply_PhaiTraCanTruPRE["new_referencenumber"] = phieuDNThuNo["new_masophieu"] + "_" + i.ToString();
                                                    apply_PhaiTraCanTruPRE["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                                    apply_PhaiTraCanTruPRE["new_paymentdocumentname"] = "CANTRU_04";
                                                    apply_PhaiTraCanTruPRE["new_vouchernumber"] = "CTND";
                                                    apply_PhaiTraCanTruPRE["new_cashflow"] = "00.00";
                                                    apply_PhaiTraCanTruPRE["new_paymentnum"] = "1";
                                                    if (etlTransaction["new_invoicetype"].ToString() == "PRE")
                                                    {
                                                        apply_PhaiTraCanTruPRE["new_prepay_num"] = etlTransaction["new_name"];
                                                        apply_PhaiTraCanTruPRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienvay"]).Value);
                                                    }
                                                    else
                                                    {

                                                        apply_PhaiTraCanTruPRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienvay"]).Value * (-1));
                                                    }
                                                    apply_PhaiTraCanTruPRE["new_documentnum"] = etlTransaction["new_sochungtu"];//phieuDNThuNo["new_masophieu"].ToString();

                                                    if (phieuDNThuNo.Contains("new_khachhang"))
                                                        apply_PhaiTraCanTruPRE["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                                    else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                                        apply_PhaiTraCanTruPRE["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                                    apply_PhaiTraCanTruPRE.Id = service.Create(apply_PhaiTraCanTruPRE);

                                                    apply_PhaiTraCanTruPRE["new_makhachhang"] = khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "";
                                                    apply_PhaiTraCanTruPRE["name"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("fullname") ? khThuHoach["fullname"].ToString() : "") : (khThuHoach.Contains("name") ? khThuHoach["name"].ToString() : ""));
                                                    apply_PhaiTraCanTruPRE["new_socmnd"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : "") : (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""));
                                                    apply_PhaiTraCanTruPRE["new_type"] = etlTransaction["new_invoicetype"].ToString() == "PRE" ? "TYPE2" : "TYPE4";
                                                    //Send(apply_PhaiTraCanTruPRE);
                                                    listSend.Add(apply_PhaiTraCanTruPRE);
                                                    #endregion
                                                }
                                            }
                                            #endregion

                                            ++i;
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
                                                etl_ND["new_season"] = vuMua;
                                                //etl_ND["new_vudautu"] = fullEntity["new_vudautu"];
                                                etl_ND["new_sochungtu"] = phieuDNThuNo["new_masophieu"].ToString();
                                                //etl_ND["new_lannhan"] = phieuNghiemThu["new_lannhan"];
                                                etl_ND["new_contractnumber"] = phanBoDauTu["new_maphieuphanbo"].ToString();// lay hop dong mia tren phieu nghiem thu
                                                etl_ND["new_tradingpartner"] = (khThuHoach.LogicalName.ToLower().Trim() == "contact" ? // phieu nghiem thu
                                                    ((khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "") + "_" + (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : ""))
                                                    :
                                                    ((khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "") + "_" + (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""))
                                                    );
                                                etl_ND["new_suppliernumber"] = khThuHoach["new_makhachhang"].ToString();
                                                etl_ND["new_suppliersite"] = "TAY NINH";
                                                etl_ND["new_invoicedate"] = phieuTTMia["new_ngaylap"];// lay ngay nghiem thu (ngay thuc hien)
                                                etl_ND["new_descriptionheader"] = "Ghi nợ cấn trừ công đốn_vụ_" + vuMua;
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

                                                etl_ND["new_makhachhang"] = khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "";
                                                etl_ND["name"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("fullname") ? khThuHoach["fullname"].ToString() : "") : (khThuHoach.Contains("name") ? khThuHoach["name"].ToString() : ""));
                                                etl_ND["new_socmnd"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : "") : (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""));
                                                //etl_ND["new_descriptionlines"] = phieuDNThuNo["new_name"].ToString();
                                                etl_ND["tran_type"] = "CRE";

                                                //Send(etl_ND);
                                                listSend.Add(etl_ND);
                                                #endregion

                                                #region Phat sinh Apply 

                                                // phat sinh apply CRE Lai
                                                Entity apply_PhaiTraCanTruCRE = new Entity("new_applytransaction");

                                                Entity etl_Lai = service.Retrieve("new_etltransaction", etl_LaiID, new ColumnSet(true));
                                                if (etl_Lai != null && etl_Lai.Contains("new_name"))
                                                {
                                                    apply_PhaiTraCanTruCRE["new_name"] = (string)etl_Lai["new_name"];
                                                }

                                                apply_PhaiTraCanTruCRE["new_suppliersitecode"] = "Tây Ninh";
                                                apply_PhaiTraCanTruCRE["new_bankcccountnum"] = "CTXL-VND-0";

                                                apply_PhaiTraCanTruCRE["new_paymentamount"] = new Money(((Money)phieuTinhLai["new_tienlai"]).Value * (-1));

                                                apply_PhaiTraCanTruCRE["new_referencenumber"] = phieuDNThuNo["new_masophieu"] + "_" + i.ToString();
                                                apply_PhaiTraCanTruCRE["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                                apply_PhaiTraCanTruCRE["new_paymentdocumentname"] = "CANTRU_04";
                                                apply_PhaiTraCanTruCRE["new_vouchernumber"] = "CTND";
                                                apply_PhaiTraCanTruCRE["new_cashflow"] = "00.00";
                                                apply_PhaiTraCanTruCRE["new_paymentnum"] = "1";
                                                apply_PhaiTraCanTruCRE["new_documentnum"] = etl_Lai["new_sochungtu"];//phieuDNThuNo["new_masophieu"].ToString();

                                                if (phieuDNThuNo.Contains("new_khachhang"))
                                                    apply_PhaiTraCanTruCRE["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                                else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                                    apply_PhaiTraCanTruCRE["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                                apply_PhaiTraCanTruCRE.Id = service.Create(apply_PhaiTraCanTruCRE);

                                                apply_PhaiTraCanTruCRE["new_makhachhang"] = khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "";
                                                apply_PhaiTraCanTruCRE["name"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("fullname") ? khThuHoach["fullname"].ToString() : "") : (khThuHoach.Contains("name") ? khThuHoach["name"].ToString() : ""));
                                                apply_PhaiTraCanTruCRE["new_socmnd"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : "") : (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""));
                                                apply_PhaiTraCanTruCRE["new_type"] = "TYPE4";
                                                //Send(apply_PhaiTraCanTruCRE);
                                                listSend.Add(apply_PhaiTraCanTruCRE);
                                                //}
                                                #endregion

                                                #region CanTruTienLai tu TienThanhToan
                                                Entity apply_CanTruTienNoGoc = new Entity("new_applytransaction");
                                                Entity etl_entity = service.Retrieve("new_etltransaction", etl_STAId, new ColumnSet(new string[] { "new_name" }));
                                                if (etl_entity != null && etl_entity.Contains("new_name"))
                                                {
                                                    apply_CanTruTienNoGoc["new_name"] = (string)etl_entity["new_name"];
                                                }
                                                apply_CanTruTienNoGoc["new_suppliersitecode"] = "Tây Ninh";
                                                apply_CanTruTienNoGoc["new_bankcccountnum"] = "CTXL-VND-0";

                                                apply_CanTruTienNoGoc["new_referencenumber"] = phieuDNThuNo["new_masophieu"].ToString() + "_" + i.ToString();
                                                apply_CanTruTienNoGoc["new_paymentamount"] = phieuTinhLai["new_tienlai"];
                                                apply_CanTruTienNoGoc["new_paymentdate"] = phieuDNThuNo["new_ngaythu"];
                                                apply_CanTruTienNoGoc["new_paymentdocumentname"] = "CANTRU_03";
                                                apply_CanTruTienNoGoc["new_vouchernumber"] = "CTND";
                                                apply_CanTruTienNoGoc["new_cashflow"] = "00.00";
                                                apply_CanTruTienNoGoc["new_paymentnum"] = "1";
                                                apply_CanTruTienNoGoc["new_documentnum"] = phieuDNThuNo["new_masophieu"].ToString();

                                                if (phieuDNThuNo.Contains("new_khachhang"))
                                                    apply_CanTruTienNoGoc["new_khachhang"] = phieuDNThuNo["new_khachhang"];
                                                else if (phieuDNThuNo.Contains("new_khachhangdoanhnghiep"))
                                                    apply_CanTruTienNoGoc["new_khachhangdoanhnghiep"] = phieuDNThuNo["new_khachhangdoanhnghiep"];

                                                apply_CanTruTienNoGoc.Id = service.Create(apply_CanTruTienNoGoc);

                                                apply_CanTruTienNoGoc["new_makhachhang"] = khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "";
                                                apply_CanTruTienNoGoc["name"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("fullname") ? khThuHoach["fullname"].ToString() : "") : (khThuHoach.Contains("name") ? khThuHoach["name"].ToString() : ""));
                                                apply_CanTruTienNoGoc["new_socmnd"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : "") : (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""));
                                                apply_CanTruTienNoGoc["new_type"] = "TYPE4";
                                                //Send(apply_CanTruTienNoGoc);
                                                listSend.Add(apply_CanTruTienNoGoc);
                                                #endregion
                                            }
                                            #endregion
                                        }
                                    }
                                }

                                #endregion
                            }
                            #endregion

                            trace.Trace("Thuong dau cong - Tap chat");
                            // Thuong dau cong - Tap chat
                            #region Thuong tap chat
                            //if (phieuTTMia.Contains("new_tienthuongtapchat") && ((Money)phieuTTMia["new_tienthuongtapchat"]).Value > 0)
                            //{
                            //    #region ETl Thu hoach
                            //    hDThuHoach = service.Retrieve("new_hopdongthuhoach", ((EntityReference)phieuTTMia["new_hopdongthuhoach"]).Id, new ColumnSet(true));

                            //    // STA
                            //    Entity etl_STA = new Entity("new_etltransaction");
                            //    etl_STA["new_name"] = fullEntity["new_masophieu"].ToString() + "_THUONG_CD_" + phieuTTMia["new_masophieu"].ToString() + "_STA" + test;
                            //    etl_STA["new_vouchernumber"] = "MMND";
                            //    etl_STA["new_transactiontype"] = "4.5.3.e";
                            //    etl_STA["new_customertype"] = new OptionSetValue(phieuTTMia.Contains("new_doitacthuhoach") ? 1 : 2);
                            //    etl_STA["new_season"] = vuMua;
                            //    //etl_STA["new_vudautu"] = fullEntity["new_vudautu"];
                            //    etl_STA["new_sochungtu"] = fullEntity["new_masophieu"].ToString() + "_" + phieuTTMia["new_masophieu"].ToString();
                            //    etl_STA["new_lannhan"] = 1;// fullEntity["new_lannhan"];
                            //    etl_STA["new_contractnumber"] = hDThuHoach["new_sohopdong"].ToString();//fullEntity["new_masophieu"].ToString() + "_" + "Tiền thưởng công đốn (tạp chất)_vụ_" + vuMua;//
                            //    etl_STA["new_tradingpartner"] = (khThuHoach.LogicalName.ToLower().Trim() == "contact" ?
                            //        ((khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "") + "_" + (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : ""))
                            //        :
                            //        ((khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "") + "_" + (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""))
                            //        );

                            //    etl_STA["new_suppliersite"] = "TAY NINH";
                            //    etl_STA["new_invoicedate"] = phieuTTMia["new_ngaylap"];
                            //    etl_STA["new_descriptionheader"] = "Tiền thưởng công đốn (tạp chất)_vụ_" + vuMua;
                            //    etl_STA["new_terms"] = "Tra Ngay";
                            //    etl_STA["new_taxtype"] = "";
                            //    etl_STA["new_invoiceamount"] = (Money)phieuTTMia["new_tienthuongtapchat"];
                            //    etl_STA["new_gldate"] = fullEntity["new_ngayduyet"];
                            //    etl_STA["new_invoicetype"] = "STA";

                            //    if (phieuTTMia.Contains("new_doitacvanchuyen"))
                            //        etl_STA["new_khachhang"] = phieuTTMia["new_doitacvanchuyen"];
                            //    else if (phieuTTMia.Contains("new_doitacvanchuyenkhdn"))
                            //        etl_STA["new_khachhangdoanhnghiep"] = phieuTTMia["new_doitacvanchuyenkhdn"];
                            //    Guid etl_STAId = service.Create(etl_STA);

                            //    etl_STA["new_makhachhang"] = khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "";
                            //    etl_STA["name"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("fullname") ? khThuHoach["fullname"].ToString() : "") : (khThuHoach.Contains("name") ? khThuHoach["name"].ToString() : ""));
                            //    etl_STA["new_socmnd"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : "") : (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""));
                            //    etl_STA["tran_type"] = "STA";

                            //    Send(etl_STA);
                            //    #endregion

                            //    #region Apply Thuong Thu hoach Cong Don - Tap chat
                            //    if (khThuHoach.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)khThuHoach["new_phuongthucthanhtoan"]).Value == 100000001)
                            //    {
                            //        // phat sinh apply CRE Lai
                            //        Entity apply_STAVC = new Entity("new_applytransaction");

                            //        Entity etl_Mia = service.Retrieve("new_etltransaction", etl_STAId, new ColumnSet(true));
                            //        if (etl_Mia != null && etl_Mia.Contains("new_name"))
                            //        {
                            //            apply_STAVC["new_name"] = (string)etl_Mia["new_name"];
                            //        }

                            //        apply_STAVC["new_suppliersitecode"] = "Tây Ninh";


                            //        List<Entity> taikhoannganhang = RetrieveMultiRecord(service, "new_taikhoannganhang",
                            //            new ColumnSet(new string[] { "new_sotaikhoan", "new_giaodichchinh" }),
                            //            khThuHoach.LogicalName == "contact" ? "new_khachhang" : "new_khachhangdoanhnghiep", khThuHoach.Id);

                            //        Entity taikhoanchinh = null;

                            //        foreach (Entity en in taikhoannganhang)
                            //        {
                            //            if ((bool)en["new_giaodichchinh"] == true)
                            //                taikhoanchinh = en;
                            //        }

                            //        //apply_STAVC["new_supplierbankname"] = (taikhoanchinh == null ? "TTCS-MIA-VND" : taikhoanchinh["new_sotaikhoan"]);
                            //        apply_STAVC["new_bankcccountnum"] = (taikhoanchinh == null ? "TTCS-MIA-VND" : taikhoanchinh["new_sotaikhoan"]);

                            //        //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                            //        apply_STAVC["new_paymentamount"] = (Money)phieuTTMia["new_tienthuongtapchat"];

                            //        //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                            //        apply_STAVC["new_referencenumber"] = phieuTTMia["new_masophieu"];
                            //        apply_STAVC["new_paymentdate"] = phieuTTMia["new_ngaylap"];
                            //        apply_STAVC["new_paymentdocumentname"] = "Tiền thưởng công đốn (tạp chất)_vụ_" + vuMua;
                            //        apply_STAVC["new_vouchernumber"] = "BN";
                            //        apply_STAVC["new_cashflow"] = "00.00";
                            //        apply_STAVC["new_paymentnum"] = "1";
                            //        apply_STAVC["new_documentnum"] = phieuTTMia["new_masophieu"].ToString();
                            //        //apply_STAVC["new_documentsequence"] = 1;

                            //        if (khThuHoach.LogicalName == "contact")
                            //            apply_STAVC["new_khachhang"] = new EntityReference("contact", khThuHoach.Id);
                            //        else if (khThuHoach.LogicalName == "account")
                            //            apply_STAVC["new_khachhangdoanhnghiep"] = new EntityReference("account", khThuHoach.Id);

                            //        apply_STAVC.Id = service.Create(apply_STAVC);

                            //        apply_STAVC["new_makhachhang"] = khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "";
                            //        apply_STAVC["name"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("fullname") ? khThuHoach["fullname"].ToString() : "") : (khThuHoach.Contains("name") ? khThuHoach["name"].ToString() : ""));
                            //        apply_STAVC["new_socmnd"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : "") : (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""));
                            //        apply_STAVC["new_type"] = "TYPE5";
                            //        Send(apply_STAVC);
                            //    }
                            //    #endregion

                            //}
                            #endregion

                            trace.Trace("Phat tap chat");
                            // Phat tap chat
                            #region Phat tap chat - cong don
                            //if (phieuTTMia.Contains("new_tienphattapchat") && ((Money)phieuTTMia["new_tienphattapchat"]).Value > 0)
                            //{
                            //    #region ETL Phat tap chat                        
                            //    Entity etl_CRE = new Entity("new_etltransaction");
                            //    etl_CRE["new_name"] = fullEntity["new_masophieu"].ToString() + "_PHAT_CD_" + phieuTTMia["new_masophieu"].ToString() + "_CRE" + test;
                            //    etl_CRE["new_vouchernumber"] = "MMND";
                            //    etl_CRE["new_transactiontype"] = "5.6.2.e";
                            //    etl_CRE["new_customertype"] = new OptionSetValue(phieuTTMia.Contains("new_doitacthuhoach") ? 1 : 2);
                            //    etl_CRE["new_season"] = vuMua;
                            //    //etl_STA["new_vudautu"] = fullEntity["new_vudautu"];
                            //    etl_CRE["new_sochungtu"] = fullEntity["new_masophieu"].ToString() + "_" + phieuTTMia["new_masophieu"].ToString();
                            //    etl_CRE["new_lannhan"] = 1;// fullEntity["new_lannhan"];
                            //    etl_CRE["new_contractnumber"] = hDThuHoach["new_sohopdong"].ToString();//fullEntity["new_masophieu"].ToString() + "_" + "Tiền phạt tạp chất_vụ_" + vuMua;// 
                            //    etl_CRE["new_tradingpartner"] = (khThuHoach.LogicalName.ToLower().Trim() == "contact" ?
                            //        ((khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "") + "_" + (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : ""))
                            //        :
                            //        ((khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "") + "_" + (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""))
                            //        );

                            //    etl_CRE["new_suppliersite"] = "TAY NINH";
                            //    etl_CRE["new_invoicedate"] = phieuTTMia["new_ngaylap"];
                            //    etl_CRE["new_descriptionheader"] = "Tiền phạt tạp chất_vụ_" + vuMua;
                            //    etl_CRE["new_terms"] = "Tra Ngay";
                            //    etl_CRE["new_taxtype"] = "";
                            //    etl_CRE["new_invoiceamount"] = new Money(((Money)phieuTTMia["new_tienphattapchat"]).Value * (-1));
                            //    etl_CRE["new_gldate"] = fullEntity["new_ngayduyet"];
                            //    etl_CRE["new_invoicetype"] = "CRE";

                            //    if (phieuTTMia.Contains("new_doitacthuhoach"))
                            //        etl_CRE["new_khachhang"] = phieuTTMia["new_doitacthuhoach"];
                            //    else if (phieuTTMia.Contains("new_doitacthuhoachkhdn"))
                            //        etl_CRE["new_khachhangdoanhnghiep"] = phieuTTMia["new_doitacthuhoachkhdn"];
                            //    Guid etl_CREId = service.Create(etl_CRE);

                            //    etl_CRE["new_makhachhang"] = khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "";
                            //    etl_CRE["name"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("fullname") ? khThuHoach["fullname"].ToString() : "") : (khThuHoach.Contains("name") ? khThuHoach["name"].ToString() : ""));
                            //    etl_CRE["new_socmnd"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : "") : (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""));
                            //    etl_CRE["tran_type"] = "CRE";
                            //    //etl_CRE["new_descriptionlines"] = phieuTTMia["new_masophieu"].ToString();

                            //    Send(etl_CRE);
                            //    #endregion

                            //    #region Apply Phat cong don
                            //    if (khThuHoach.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)khThuHoach["new_phuongthucthanhtoan"]).Value == 100000001)
                            //    {
                            //        // phat sinh apply CRE phat tap chat
                            //        Entity apply_CREPhatTH = new Entity("new_applytransaction");

                            //        Entity etl_Lai = service.Retrieve("new_etltransaction", etl_CREId, new ColumnSet(true));
                            //        if (etl_Lai != null && etl_Lai.Contains("new_name"))
                            //        {
                            //            apply_CREPhatTH["new_name"] = (string)etl_Lai["new_name"];
                            //        }

                            //        apply_CREPhatTH["new_suppliersitecode"] = "Tây Ninh";

                            //        if (khThuHoach.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)khThuHoach["new_phuongthucthanhtoan"]).Value == 100000001)
                            //        {
                            //            List<Entity> taikhoannganhang = RetrieveMultiRecord(service, "new_taikhoannganhang",
                            //                new ColumnSet(new string[] { "new_sotaikhoan", "new_giaodichchinh" }),
                            //                khThuHoach.LogicalName == "contact" ? "new_khachhang" : "new_khachhangdoanhnghiep", khThuHoach.Id);

                            //            Entity taikhoanchinh = null;

                            //            foreach (Entity en in taikhoannganhang)
                            //            {
                            //                if ((bool)en["new_giaodichchinh"] == true)
                            //                    taikhoanchinh = en;
                            //            }

                            //            //apply_CREPhatTH["new_supplierbankname"] = (taikhoanchinh == null ? "CTXL-VND-0" : taikhoanchinh["new_sotaikhoan"]);
                            //            apply_CREPhatTH["new_bankcccountnum"] = (taikhoanchinh == null ? "CTXL-VND-0" : taikhoanchinh["new_sotaikhoan"]);
                            //        }
                            //        else
                            //            apply_CREPhatTH["new_bankcccountnum"] = "CTXL-VND-0";

                            //        //apply_PGNhomgiong_CRE["new_name"] = "new_phieugiaonhanhomgiong";
                            //        apply_CREPhatTH["new_paymentamount"] = new Money(((Money)phieuTTMia["new_tienphattapchat"]).Value * (-1));

                            //        //apply_PGNhomgiong_CRE["new_suppliernumber"] = KH["new_makhachhang"];
                            //        apply_CREPhatTH["new_referencenumber"] = phieuTTMia["new_masophieu"];
                            //        apply_CREPhatTH["new_paymentdate"] = phieuTTMia["new_ngaylap"];
                            //        apply_CREPhatTH["new_paymentdocumentname"] = "Tiền phạt tạp chất_vụ_" + vuMua;
                            //        apply_CREPhatTH["new_vouchernumber"] = "CTND";
                            //        apply_CREPhatTH["new_cashflow"] = "00.00";
                            //        apply_CREPhatTH["new_paymentnum"] = "1";
                            //        apply_CREPhatTH["new_documentnum"] = phieuTTMia["new_masophieu"].ToString();
                            //        //apply_CREPhatTH["new_documentsequence"] = 1;

                            //        if (phieuTTMia.Contains("new_khachhang"))
                            //            apply_CREPhatTH["new_khachhang"] = phieuTTMia["new_khachhang"];
                            //        else if (phieuTTMia.Contains("new_khachhangdoanhnghiep"))
                            //            apply_CREPhatTH["new_khachhangdoanhnghiep"] = phieuTTMia["new_khachhangdoanhnghiep"];

                            //        apply_CREPhatTH.Id = service.Create(apply_CREPhatTH);

                            //        apply_CREPhatTH["new_makhachhang"] = khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "";
                            //        apply_CREPhatTH["name"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("fullname") ? khThuHoach["fullname"].ToString() : "") : (khThuHoach.Contains("name") ? khThuHoach["name"].ToString() : ""));
                            //        apply_CREPhatTH["new_socmnd"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : "") : (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""));
                            //        apply_CREPhatTH["new_type"] = "TYPE4";
                            //        Send(apply_CREPhatTH);
                            //    }
                            //    #endregion
                            //}
                            #endregion

                            trace.Trace("Tam giu nong dan");
                            // Tam giu nong dan
                            #region Tam giu nong dan
                            if (phieuTTMia.Contains("new_tamgiunongdan") && ((Money)phieuTTMia["new_tamgiunongdan"]).Value > 0)
                            {
                                #region Tam giu nong dan
                                Entity etl_STA = new Entity("new_etltransaction");
                                etl_STA["new_name"] = fullEntity["new_masophieu"].ToString() + "_TAMGIU_ND_" + phieuTTMia["new_masophieu"].ToString() + "_STA" + test;
                                etl_STA["new_vouchernumber"] = "MMND";
                                etl_STA["new_transactiontype"] = "4.3.3.b";
                                etl_STA["new_customertype"] = new OptionSetValue(phieuTTMia.Contains("new_khachhang") ? 1 : 2);
                                etl_STA["new_season"] = vuMua;
                                //etl_STA["new_vudautu"] = fullEntity["new_vudautu"];
                                etl_STA["new_sochungtu"] = fullEntity["new_masophieu"].ToString() + "_" + phieuTTMia["new_masophieu"].ToString();
                                etl_STA["new_lannhan"] = 1;// fullEntity["new_lannhan"];
                                etl_STA["new_contractnumber"] = hDMia["new_masohopdong"].ToString();//fullEntity["new_masophieu"].ToString() + "_" + "Tiền tạm giữ nông dân_vụ_" + vuMua;// 
                                etl_STA["new_tradingpartner"] = (khNongDan.LogicalName.ToLower().Trim() == "contact" ?
                                    ((khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "") + "_" + (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : ""))
                                    :
                                    ((khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "") + "_" + (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""))
                                    );

                                etl_STA["new_suppliersite"] = "TAY NINH";
                                etl_STA["new_invoicedate"] = phieuTTMia["new_ngaylap"];
                                etl_STA["new_descriptionheader"] = "Tiền tạm giữ nông dân_vụ_" + vuMua;
                                etl_STA["new_terms"] = "Tra Ngay";
                                etl_STA["new_taxtype"] = "";

                                etl_STA["new_invoiceamount"] = new Money(tamGiuNongDan);
                                etl_STA["new_gldate"] = fullEntity["new_ngayduyet"];
                                etl_STA["new_invoicetype"] = "STA";

                                if (phieuTTMia.Contains("new_khachhang"))
                                    etl_STA["new_khachhang"] = phieuTTMia["new_khachhang"];
                                else if (phieuTTMia.Contains("new_khachhangdoanhnghiep"))
                                    etl_STA["new_khachhangdoanhnghiep"] = phieuTTMia["new_khachhangdoanhnghiep"];
                                Guid etl_STAId = service.Create(etl_STA);

                                etl_STA["new_makhachhang"] = khNongDan.Contains("new_makhachhang") ? khNongDan["new_makhachhang"].ToString() : "";
                                etl_STA["name"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("fullname") ? khNongDan["fullname"].ToString() : "") : (khNongDan.Contains("name") ? khNongDan["name"].ToString() : ""));
                                etl_STA["new_socmnd"] = (khNongDan.LogicalName.ToLower() == "contact" ? (khNongDan.Contains("new_socmnd") ? khNongDan["new_socmnd"].ToString() : "") : (khNongDan.Contains("new_masothue") ? khNongDan["new_masothue"].ToString() : ""));
                                etl_STA["tran_type"] = "STA";
                                //etl_STA["new_descriptionlines"] = phieuTTMia["new_masophieu"].ToString();

                                //Send(etl_STA);
                                listSend.Add(etl_STA);
                                #endregion
                            }
                            #endregion

                            trace.Trace("Tam giu van chuyen");
                            // Tam giu van chuyen
                            #region Tam giu Van Chuyen
                            if (phieuTTMia.Contains("new_tamgiuvanchuyen") && phieuTTMia.Contains("new_hopdongvanchuyen") && ((Money)phieuTTMia["new_tamgiuvanchuyen"]).Value > 0)
                            {
                                hDVanChuyen = service.Retrieve("new_hopdongvanchuyen", ((EntityReference)phieuTTMia["new_hopdongvanchuyen"]).Id, new ColumnSet(true));

                                // STA
                                Entity etl_STA = new Entity("new_etltransaction");
                                etl_STA["new_name"] = fullEntity["new_masophieu"].ToString() + "_TAMGIU_VC_" + phieuTTMia["new_masophieu"].ToString() + "_STA" + test;
                                etl_STA["new_vouchernumber"] = "MMND";
                                etl_STA["new_transactiontype"] = "3.3.3.d";
                                etl_STA["new_customertype"] = new OptionSetValue(phieuTTMia.Contains("new_doitacvanchuyen") ? 1 : 2);
                                etl_STA["new_season"] = vuMua;
                                //etl_STA["new_vudautu"] = fullEntity["new_vudautu"];
                                etl_STA["new_sochungtu"] = fullEntity["new_masophieu"].ToString() + "_" + phieuTTMia["new_masophieu"].ToString();
                                etl_STA["new_lannhan"] = 1;// fullEntity["new_lannhan"];
                                etl_STA["new_contractnumber"] = hDVanChuyen["new_sohopdongvanchuyen"].ToString();//fullEntity["new_masophieu"].ToString() + "_" + "Tiền tạm giữ vận chuyển_vụ_" + vuMua;// 
                                etl_STA["new_tradingpartner"] = (khVanChuyen.LogicalName.ToLower().Trim() == "contact" ?
                                    ((khVanChuyen.Contains("new_makhachhang") ? khVanChuyen["new_makhachhang"].ToString() : "") + "_" + (khVanChuyen.Contains("new_socmnd") ? khVanChuyen["new_socmnd"].ToString() : ""))
                                    :
                                    ((khVanChuyen.Contains("new_makhachhang") ? khVanChuyen["new_makhachhang"].ToString() : "") + "_" + (khVanChuyen.Contains("new_masothue") ? khVanChuyen["new_masothue"].ToString() : ""))
                                    );

                                etl_STA["new_suppliersite"] = "TAY NINH";
                                etl_STA["new_invoicedate"] = phieuTTMia["new_ngaylap"];
                                etl_STA["new_descriptionheader"] = "Tiền tạm giữ vận chuyển_vụ_" + vuMua;
                                etl_STA["new_terms"] = "Tra Ngay";
                                etl_STA["new_taxtype"] = "";
                                etl_STA["new_invoiceamount"] = new Money(tamGiuVanChuyen);
                                etl_STA["new_gldate"] = fullEntity["new_ngayduyet"];
                                etl_STA["new_invoicetype"] = "STA";

                                if (phieuTTMia.Contains("new_doitacvanchuyen"))
                                    etl_STA["new_khachhang"] = phieuTTMia["new_doitacvanchuyen"];
                                else if (phieuTTMia.Contains("new_doitacvanchuyenkhdn"))
                                    etl_STA["new_khachhangdoanhnghiep"] = phieuTTMia["new_doitacvanchuyenkhdn"];
                                Guid etl_STAId = service.Create(etl_STA);

                                etl_STA["new_makhachhang"] = khVanChuyen.Contains("new_makhachhang") ? khVanChuyen["new_makhachhang"].ToString() : "";
                                etl_STA["name"] = (khVanChuyen.LogicalName.ToLower() == "contact" ? (khVanChuyen.Contains("fullname") ? khVanChuyen["fullname"].ToString() : "") : (khVanChuyen.Contains("name") ? khVanChuyen["name"].ToString() : ""));
                                etl_STA["new_socmnd"] = (khVanChuyen.LogicalName.ToLower() == "contact" ? (khVanChuyen.Contains("new_socmnd") ? khVanChuyen["new_socmnd"].ToString() : "") : (khVanChuyen.Contains("new_masothue") ? khVanChuyen["new_masothue"].ToString() : ""));
                                etl_STA["tran_type"] = "STA";
                                //etl_STA["new_descriptionlines"] = phieuTTMia["new_masophieu"].ToString();

                                //Send(etl_STA);
                                listSend.Add(etl_STA);
                            }
                            #endregion

                            trace.Trace("Tam giu thu hoach");
                            // Tam giu thu hoach
                            #region ETL Tam giu Thu Hoach
                            if (phieuTTMia.Contains("new_tamgiuthuhoach") && phieuTTMia.Contains("new_hopdongthuhoach") && ((Money)phieuTTMia["new_tamgiuthuhoach"]).Value > 0)
                            {
                                hDThuHoach = service.Retrieve("new_hopdongthuhoach", ((EntityReference)phieuTTMia["new_hopdongthuhoach"]).Id, new ColumnSet(true));
                                // STA
                                Entity etl_STA = new Entity("new_etltransaction");
                                etl_STA["new_name"] = fullEntity["new_masophieu"].ToString() + "_TAMGIU_CD_" + phieuTTMia["new_masophieu"].ToString() + "_STA" + test;
                                etl_STA["new_vouchernumber"] = "MMND";
                                etl_STA["new_transactiontype"] = "3.3.3.e";
                                etl_STA["new_customertype"] = new OptionSetValue(phieuTTMia.Contains("new_doitacthuhoach") ? 1 : 2);
                                etl_STA["new_season"] = vuMua;
                                //etl_STA["new_vudautu"] = fullEntity["new_vudautu"];
                                etl_STA["new_sochungtu"] = fullEntity["new_masophieu"].ToString() + "_" + phieuTTMia["new_masophieu"].ToString();
                                etl_STA["new_lannhan"] = 1;// fullEntity["new_lannhan"];
                                etl_STA["new_contractnumber"] = hDThuHoach["new_sohopdong"].ToString();//fullEntity["new_masophieu"].ToString() + "_" + "Tiền tạm giữ thu hoạch_vụ_" + vuMua;// 
                                etl_STA["new_tradingpartner"] = (khThuHoach.LogicalName.ToLower().Trim() == "contact" ?
                                    ((khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "") + "_" + (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : ""))
                                    :
                                    ((khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "") + "_" + (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""))
                                    );

                                etl_STA["new_suppliersite"] = "TAY NINH";
                                etl_STA["new_invoicedate"] = phieuTTMia["new_ngaylap"];
                                etl_STA["new_descriptionheader"] = "Tiền tạm giữ thu hoạch_vụ_" + vuMua;
                                etl_STA["new_terms"] = "Tra Ngay";
                                etl_STA["new_taxtype"] = "";
                                etl_STA["new_invoiceamount"] = new Money(tamGiuThuHoach);
                                etl_STA["new_gldate"] = fullEntity["new_ngayduyet"];
                                etl_STA["new_invoicetype"] = "STA";

                                if (phieuTTMia.Contains("new_doitacthuhoach"))
                                    etl_STA["new_khachhang"] = phieuTTMia["new_doitacthuhoach"];
                                else if (phieuTTMia.Contains("new_doitacthuhoachkhdn"))
                                    etl_STA["new_khachhangdoanhnghiep"] = phieuTTMia["new_doitacthuhoachkhdn"];
                                Guid etl_STAId = service.Create(etl_STA);

                                etl_STA["new_makhachhang"] = khThuHoach.Contains("new_makhachhang") ? khThuHoach["new_makhachhang"].ToString() : "";
                                etl_STA["name"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("fullname") ? khThuHoach["fullname"].ToString() : "") : (khThuHoach.Contains("name") ? khThuHoach["name"].ToString() : ""));
                                etl_STA["new_socmnd"] = (khThuHoach.LogicalName.ToLower() == "contact" ? (khThuHoach.Contains("new_socmnd") ? khThuHoach["new_socmnd"].ToString() : "") : (khThuHoach.Contains("new_masothue") ? khThuHoach["new_masothue"].ToString() : ""));
                                etl_STA["tran_type"] = "STA";
                                //etl_STA["new_descriptionlines"] = phieuTTMia["new_masophieu"].ToString();

                                //Send(etl_STA);
                                listSend.Add(etl_STA);

                                trace.Trace("END " + phieuTTMia["new_masophieu"].ToString());
                            }
                            #endregion
                        }

                        //Send(null);

                        if (entc.MoreRecords)
                        {
                            q.PageInfo.PageNumber++;
                            q.PageInfo.PagingCookie = entc.PagingCookie;
                        }
                        else
                        {
                            break;
                        }
                    }

                    if (listSend.Count > 0)
                    {
                        foreach (var item in listSend)
                        {
                            Send(item);
                        }
                        Send(null);
                    }
                    Console.WriteLine("het ");
                    Console.ReadLine();
                    trace.Trace("Het");
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

            if (MessageQueue.Exists(@".\Private$\DynamicCRM2Oracle_" + context.OrganizationName.ToUpper()))
                mq = new MessageQueue(@".\Private$\DynamicCRM2Oracle_" + context.OrganizationName.ToUpper());
            else
                mq = MessageQueue.Create(@".\Private$\DynamicCRM2Oracle_" + context.OrganizationName.ToUpper());

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
