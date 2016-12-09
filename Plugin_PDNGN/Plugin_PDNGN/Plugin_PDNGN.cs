using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Messaging;

namespace Plugin_PDNGN
{
    public class Plugin_PDNGN : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        public ITracingService trace;
        IPluginExecutionContext context;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            try
            {
                context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

                Entity target = (Entity)context.InputParameters["Target"];
                Entity vudautu = null, vuThuHoach = null;
                Entity HDMia = null;
                Entity HDTD = null;
                Entity HDTTB = null;
                string vuMua = "";
                if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000)
                {
                    // lay ra phieu DN giai ngan hien tai
                    Entity PDNGN = service.Retrieve("new_phieudenghigiaingan", target.Id, new ColumnSet(true));
                    if (PDNGN.Contains("new_vudautu") && ((EntityReference)PDNGN["new_vudautu"]).Id != null && ((EntityReference)PDNGN["new_vudautu"]).Id.ToString() != "undefined")
                    {
                        vudautu = service.Retrieve("new_vudautu", ((EntityReference)PDNGN["new_vudautu"]).Id, new ColumnSet(new string[] { "new_mavudautu" }));
                        var lsVuThuHoach = RetrieveMultiRecord(service, "new_vuthuhoach", new ColumnSet(true), "new_vudautu", vudautu.Id);
                        if (lsVuThuHoach.Count > 0)
                        {
                            vuThuHoach = lsVuThuHoach[0];
                            vuMua = ((DateTime)vuThuHoach["new_tungay"]).ToString("yyyy") + "-" + ((DateTime)vuThuHoach["new_denngay"]).ToString("yyyy");
                        }
                    }
                    if (PDNGN.Contains("new_hopdongdautumia") && ((EntityReference)PDNGN["new_hopdongdautumia"]).Id != null && ((EntityReference)PDNGN["new_hopdongdautumia"]).Id.ToString() != "undefined")
                    {
                        HDMia = service.Retrieve("new_hopdongdautumia", ((EntityReference)PDNGN["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_masohopdong" }));
                    }
                    if (PDNGN.Contains("new_hopdongdaututhuedat") && ((EntityReference)PDNGN["new_hopdongdaututhuedat"]).Id != null && ((EntityReference)PDNGN["new_hopdongdaututhuedat"]).Id.ToString() != "undefined")
                    {
                        HDTD = service.Retrieve("new_hopdongthuedat", ((EntityReference)PDNGN["new_hopdongdaututhuedat"]).Id, new ColumnSet(new string[] { "new_mahopdong" }));
                    }
                    if (PDNGN.Contains("new_hopdongdautummtb") && ((EntityReference)PDNGN["new_hopdongdautummtb"]).Id != null && ((EntityReference)PDNGN["new_hopdongdautummtb"]).Id.ToString() != "undefined")
                    {
                        HDTTB = service.Retrieve("new_hopdongdaututrangthietbi", ((EntityReference)PDNGN["new_hopdongdautummtb"]).Id, new ColumnSet(new string[] { "new_sohopdong" }));
                    }
                    if (!PDNGN.Contains("new_ngayduyet"))
                    {
                        throw new Exception("Chưa có ngày duyệt!");
                    }
                    Entity KH = null;
                    if (PDNGN.Contains("new_khachhang"))
                        KH = service.Retrieve("contact", ((EntityReference)PDNGN["new_khachhang"]).Id, new ColumnSet(true));
                    else
                        KH = service.Retrieve("account", ((EntityReference)PDNGN["new_khachhangdoanhnghiep"]).Id, new ColumnSet(true));
                    trace.Trace("begin");
                    #region begin
                    // TH so tien hoan lai > 0 PRE => GEN ETL
                    if (((Money)PDNGN["new_sotiendthoanlai"]).Value > 0)
                    {
                        trace.Trace("so tien hoan lai");
                        //traceService.Trace("1");
                        Entity etl_ND = new Entity("new_etltransaction");
                        if (PDNGN.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)PDNGN["new_phuongthucthanhtoan"]).Value.ToString() == "100000000")
                        {
                            etl_ND["new_paymenttype"] = "TM";
                        }
                        else if (PDNGN.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)PDNGN["new_phuongthucthanhtoan"]).Value.ToString() == "100000001")
                        {
                            etl_ND["new_paymenttype"] = "CK";
                        }
                        etl_ND["new_name"] = PDNGN["new_masophieu"].ToString() + "_PRE_HL";
                        //traceService.Trace("2");
                        etl_ND["new_vouchernumber"] = "DTND";
                        etl_ND["new_transactiontype"] = "1.2.5.a";
                        etl_ND["new_customertype"] = new OptionSetValue(PDNGN.Contains("new_khachhang") ? 1 : 2); // ???
                        etl_ND["new_season"] = vuMua;//vudautu["new_mavudautu"].ToString();
                        etl_ND["new_sochungtu"] = PDNGN["new_masophieu"].ToString();
                        etl_ND["new_lannhan"] = PDNGN["new_langiaingan"];
                        trace.Trace("begin2");
                        if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000000")
                        { // Mía
                            etl_ND["new_contractnumber"] = HDMia["new_masohopdong"].ToString();
                            etl_ND["new_descriptionheader"] = "Giải ngân hợp đồng mía_vụ_" + vuMua;
                        }
                        else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000001")
                        { // Thuê đất

                            etl_ND["new_contractnumber"] = HDTD["new_mahopdong"].ToString();
                            etl_ND["new_descriptionheader"] = "Giải ngân hợp đồng thuê đất_vụ_" + vuMua;
                        }
                        else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000002")
                        { // MMTTB
                            etl_ND["new_contractnumber"] = HDTTB["new_sohopdong"].ToString();
                            etl_ND["new_descriptionheader"] = "Giải ngân hợp đồng máy móc thiết bị_vụ_" + vuMua;
                        }
                        trace.Trace("begin3");
                        //traceService.Trace("3");
                        etl_ND["new_tradingpartner"] = (KH.LogicalName.ToLower().Trim() == "contact" ?
                            ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : ""))
                            :
                            ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""))
                            );
                        etl_ND["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                        traceService.Trace("4");
                        etl_ND["new_suppliersite"] = "TAY NINH"; // fix cung
                        traceService.Trace("5");
                        etl_ND["new_invoicedate"] = PDNGN["new_ngaylapphieu"];
                        //traceService.Trace("6");
                        etl_ND["new_terms"] = "Tra Ngay";
                        etl_ND["new_taxtype"] = "";
                        trace.Trace("begin4");
                        etl_ND["new_invoiceamount"] = new Money(((Money)PDNGN["new_sotiendthoanlai"]).Value);
                        etl_ND["new_gldate"] = PDNGN["new_ngayduyet"];
                        etl_ND["new_invoicetype"] = "PRE";

                        if (PDNGN.Contains("new_khachhang"))
                            etl_ND["new_khachhang"] = PDNGN["new_khachhang"];
                        else if (PDNGN.Contains("new_khachhangdoanhnghiep"))
                            etl_ND["new_khachhangdoanhnghiep"] = PDNGN["new_khachhangdoanhnghiep"];

                        trace.Trace("2.0");
                        Guid etl_NDID = Guid.Empty;

                        etl_ND["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                        etl_ND["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                        etl_ND["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                        etl_ND["tran_type"] = "PRE";

                        Send(etl_ND);
                        GenPhanBoDauTuHL(target, etl_NDID);
                        trace.Trace("2");
                        if (PDNGN.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)PDNGN["new_phuongthucthanhtoan"]).Value == 100000001) // neu la chuyen khoan
                        {
                            #region Pay nếu là chuyển khoản
                            Entity paytamung = new Entity("new_applytransaction");
                            //apply_PGNPhanbon["new_documentsequence"] = value++;
                            paytamung["new_suppliersitecode"] = "Tây Ninh";

                            //if (PDNGN.Contains("new_taikhoan"))
                            //{
                            //    Entity taikhoanchinh = service.Retrieve("new_taikhoannganhang", ((EntityReference)PDNGN["new_taikhoan"]).Id, new ColumnSet(true));
                            //    paytamung["new_supplierbankname"] = taikhoanchinh["new_sotaikhoan"];
                            //}

                            if (PDNGN.Contains("new_taikhoannganhangttcs"))
                            {
                                Entity taikhoanchinh = service.Retrieve("new_taikhoannganhangcuattcs", ((EntityReference)PDNGN["new_taikhoannganhangttcs"]).Id, new ColumnSet(true));
                                paytamung["new_bankcccountnum"] = taikhoanchinh["new_name"];
                            }

                            Entity etl_entityCRE = service.Retrieve("new_etltransaction", etl_NDID, new ColumnSet(new string[] { "new_name" }));
                            if (etl_entityCRE != null && etl_entityCRE.Contains("new_name"))
                            {
                                paytamung["new_name"] = (string)etl_entityCRE["new_name"];
                            }

                            paytamung["new_paymentamount"] = PDNGN["new_sotiendthoanlai"];
                            paytamung["new_paymentdate"] = PDNGN["new_ngaydukienchi"];
                            paytamung["new_paymentdocumentname"] = "CANTRU_03";
                            paytamung["new_vouchernumber"] = "BN";
                            paytamung["new_cashflow"] = "25.02";

                            paytamung["new_paymentnum"] = "1";
                            paytamung["new_referencenumber"] = PDNGN["new_masophieu"].ToString() + "_" + paytamung["new_name"];
                            paytamung["new_documentnum"] = PDNGN["new_masophieu"].ToString();

                            if (PDNGN.Contains("new_khachhang"))
                                paytamung["new_khachhang"] = PDNGN["new_khachhang"];
                            else if (PDNGN.Contains("new_khachhangdoanhnghiep"))
                                paytamung["new_khachhangdoanhnghiep"] = PDNGN["new_khachhangdoanhnghiep"];

                            paytamung.Id = service.Create(paytamung);

                            paytamung["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                            paytamung["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                            paytamung["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                            paytamung["new_type"] = "TYPE1";

                            Send(paytamung);
                            #endregion
                        }
                    }
                    trace.Trace("3");
                    // TH so tien khong hoan lai => GEN ETL va PAY
                    if (((Money)PDNGN["new_sotiendtkhonghoanlai"]).Value > 0)
                    {
                        #region GEN ETL
                        Entity etl_ND = new Entity("new_etltransaction");

                        etl_ND["new_name"] = PDNGN["new_masophieu"].ToString() + "_STA_KHL";
                        etl_ND["new_vouchernumber"] = "DTND";
                        etl_ND["new_transactiontype"] = "1.1.3.a";
                        etl_ND["new_customertype"] = new OptionSetValue(PDNGN.Contains("new_khachhang") ? 1 : 2); // ???
                        etl_ND["new_season"] = vuMua;//vudautu["new_mavudautu"].ToString();
                        etl_ND["new_sochungtu"] = PDNGN["new_masophieu"].ToString();
                        etl_ND["new_lannhan"] = PDNGN["new_langiaingan"];

                        if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000000")
                        { // Mía
                            etl_ND["new_contractnumber"] = HDMia["new_masohopdong"].ToString();
                            etl_ND["new_descriptionheader"] = "Giải ngân hợp đồng mía_vụ_" + vuMua;
                        }
                        else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000001")
                        { // Thuê đất
                            etl_ND["new_contractnumber"] = HDTD["new_mahopdong"].ToString();
                            etl_ND["new_descriptionheader"] = "Giải ngân hợp đồng thuê đất_vụ_" + vuMua;
                        }
                        else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000002")
                        { // MMTTB
                            etl_ND["new_contractnumber"] = HDTTB["new_sohopdong"].ToString();
                            etl_ND["new_descriptionheader"] = "Giải ngân hợp đồng máy móc thiết bị_vụ_" + vuMua;
                        }

                        etl_ND["new_tradingpartner"] = (KH.LogicalName.ToLower().Trim() == "contact" ?
                            ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : ""))
                            :
                            ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""))
                            );
                        etl_ND["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                        etl_ND["new_suppliersite"] = "TAY NINH"; // fix cung
                        etl_ND["new_invoicedate"] = PDNGN["new_ngaylapphieu"];
                        etl_ND["new_terms"] = "Tra Ngay";
                        etl_ND["new_taxtype"] = "";
                        etl_ND["new_invoiceamount"] = new Money(((Money)PDNGN["new_sotiendtkhonghoanlai"]).Value);
                        etl_ND["new_gldate"] = PDNGN["new_ngayduyet"];
                        etl_ND["new_invoicetype"] = "STA";

                        if (PDNGN.Contains("new_khachhang"))
                            etl_ND["new_khachhang"] = PDNGN["new_khachhang"];
                        else if (PDNGN.Contains("new_khachhangdoanhnghiep"))
                            etl_ND["new_khachhangdoanhnghiep"] = PDNGN["new_khachhangdoanhnghiep"];

                        Guid etl_NDID = Guid.Empty;

                        etl_ND["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                        etl_ND["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                        etl_ND["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                        etl_ND["tran_type"] = "STA";

                        Send(etl_ND);
                        GenPhanBoDauTuKHL(target, etl_NDID);
                        #endregion

                        if (PDNGN.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)PDNGN["new_phuongthucthanhtoan"]).Value == 100000001) // neu la chuyen khoan
                        {
                            #region Pay nếu là chuyển khoản
                            Entity paytamung = new Entity("new_applytransaction");
                            //apply_PGNPhanbon["new_documentsequence"] = value++;
                            paytamung["new_suppliersitecode"] = "Tây Ninh";

                            //if (PDNGN.Contains("new_taikhoan"))
                            //{
                            //    Entity taikhoanchinh = service.Retrieve("new_taikhoannganhang", ((EntityReference)PDNGN["new_taikhoan"]).Id, new ColumnSet(true));
                            //    paytamung["new_supplierbankname"] = taikhoanchinh["new_sotaikhoan"];
                            //}

                            if (PDNGN.Contains("new_taikhoannganhangttcs"))
                            {
                                Entity taikhoanchinh = service.Retrieve("new_taikhoannganhangcuattcs", ((EntityReference)PDNGN["new_taikhoannganhangttcs"]).Id, new ColumnSet(true));
                                paytamung["new_bankcccountnum"] = taikhoanchinh["new_name"];
                            }

                            Entity etl_entityCRE = service.Retrieve("new_etltransaction", etl_NDID, new ColumnSet(new string[] { "new_name" }));
                            if (etl_entityCRE != null && etl_entityCRE.Contains("new_name"))
                            {
                                paytamung["new_name"] = (string)etl_entityCRE["new_name"];
                            }

                            paytamung["new_paymentamount"] = PDNGN["new_sotiendtkhonghoanlai"];
                            paytamung["new_paymentdate"] = PDNGN["new_ngaydukienchi"];
                            //paytamung["new_paymentdocumentname"] = "CANTRU_03";
                            paytamung["new_vouchernumber"] = "BN";
                            paytamung["new_cashflow"] = "25.02";
                            paytamung["new_paymentnum"] = "1";
                            paytamung["new_referencenumber"] = PDNGN["new_masophieu"].ToString() + "_" + paytamung["new_name"];
                            paytamung["new_documentnum"] = PDNGN["new_masophieu"].ToString();

                            if (PDNGN.Contains("new_khachhang"))
                                paytamung["new_khachhang"] = PDNGN["new_khachhang"];
                            else if (PDNGN.Contains("new_khachhangdoanhnghiep"))
                                paytamung["new_khachhangdoanhnghiep"] = PDNGN["new_khachhangdoanhnghiep"];

                            paytamung.Id = service.Create(paytamung);

                            paytamung["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                            paytamung["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                            paytamung["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                            paytamung["new_type"] = "TYPE5";
                            Send(paytamung);
                            #endregion
                        }
                    }
                    #endregion
                    trace.Trace("4");
                    Send(null);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
        }

        public void GenPhanBoDauTuKHL(Entity target, Guid idSTA)
        {
            int type = 0;
            Entity phieugiaonhan = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_hopdongdautumia", "new_khachhang",
                        "new_khachhangdoanhnghiep", "new_vudautu","new_phieudangkyphanbon",
                        "new_tram","new_canbonongvu","new_ngaynhan","new_masophieu","new_lannhan" }));

            string sophieu = phieugiaonhan.Contains("new_masophieu") ? (string)phieugiaonhan["new_masophieu"] : "";
            int lannhan = phieugiaonhan.Contains("new_lannhan") ? (int)phieugiaonhan["new_lannhan"] : 0;

            if (!phieugiaonhan.Contains("new_vudautu"))
                throw new Exception("Phiếu giao nhận không có vụ đầu tư");

            if (!phieugiaonhan.Contains("new_ngaynhan"))
                throw new Exception("Phiếu giao nhận không có ngày nhận");

            if (!phieugiaonhan.Contains("new_phieudangkyphanbon"))
                throw new Exception("Phiếu giao nhận không có phiếu đăng ký phân bón");

            Entity tram = null;
            Entity cbnv = null;
            Entity vudautu = service.Retrieve("new_vudautu",
                       ((EntityReference)phieugiaonhan["new_vudautu"]).Id, new ColumnSet(new string[] { "new_vudautuid" }));

            if (phieugiaonhan.Contains("new_tram"))
                tram = service.Retrieve("businessunit", ((EntityReference)phieugiaonhan["new_tram"]).Id,
                    new ColumnSet(new string[] { "businessunitid" }));

            if (phieugiaonhan.Contains("new_canbonongvu"))
                cbnv = service.Retrieve("new_kiemsoatvien", ((EntityReference)phieugiaonhan["new_canbonongvu"]).Id,
                    new ColumnSet(new string[] { "new_kiemsoatvienid" }));

            List<Entity> lstChitietPGNHM = RetrieveMultiRecord(service, "new_chitietgiaonhanphanbon",
                    new ColumnSet(new string[] { "new_sotienhl", "new_sotienkhl", "new_ngaynhan" }),
                    "new_phieugiaonhanphanbon", phieugiaonhan.Id);

            EntityCollection entcChitiet = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieugiaonhanphanbon",
                "new_new_pgnphanbon_new_chitiethddtmia",
                new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_phieugiaonhanphanbonid", phieugiaonhan.Id);

            decimal tongdmhl = 0;
            decimal tongdmkhl = 0;

            List<Entity> lstThoai = RetrieveMultiRecord(service, "new_thuadat_pdkphanbon",
                new ColumnSet(true), "new_phieudangky", ((EntityReference)phieugiaonhan["new_phieudangkyphanbon"]).Id);
            trace.Trace("Số lượng entity từ pdk : " + lstThoai.Count.ToString());
            if (entcChitiet.Entities.Count == 0)
                return;

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
                throw new Exception("Phiếu giao nhận không có khách hàng");

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

            trace.Trace("vong lap chi tiet hddt mia");
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
            trace.Trace("Vong lap chi tiet giai ngan : " + lstChitietPGNHM.Count.ToString());

            foreach (Entity giaingan in lstChitietPGNHM) // vong lap giai ngan
            {
                decimal phanbokhonghoanlai = giaingan.Contains("new_sotienkhl") ? ((Money)giaingan["new_sotienkhl"]).Value : new decimal(0);
                decimal phanbohoanlai = giaingan.Contains("new_sotienhl") ? ((Money)giaingan["new_sotienhl"]).Value : new decimal(0);
                DateTime ngaynhan = (DateTime)giaingan["new_ngaynhan"];

                trace.Trace("Không hoàn lại : " + dtDinhMuc.Count.ToString());
                foreach (Guid key in dtDinhMuc.Keys)
                {
                    DinhMuc a = dtDinhMuc[key];
                    if (tongdmkhl == 0)
                        throw new Exception("Tổng định mức không hoàn lại phải khác 0");

                    Entity thuadatcanhtac = service.Retrieve("new_thuadatcanhtac", key,
                        new ColumnSet(new string[] { "new_laisuat", "new_name", "new_loailaisuat",
                            "new_dachikhonghoanlai_phanbon"}));

                    decimal sotien = phanbokhonghoanlai * a.dinhMucKHL / tongdmkhl;
                    trace.Trace("Số tiền :" + sotien.ToString() +
                        "phân bổ không hoàn lai:" + phanbokhonghoanlai.ToString()
                        + "Định mực KHL: " + a.dinhMucKHL.ToString() + "Tổng định mức: " + tongdmkhl.ToString());

                    CreatePBDT(hddtmia, KH, thuadatcanhtac, vudautu.ToEntityReference(), vudautu.ToEntityReference(), sotien,
                        idSTA, type = 1, tram, cbnv, ngaynhan, phieugiaonhan, sophieu, lannhan);
                }
                trace.Trace("End KHL");
            }
            trace.Trace("end phan bo dau tu");
        }

        public void GenPhanBoDauTuHL(Entity target, Guid idCRE)
        {
            int type = 0;
            Entity phieugiaonhan = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_hopdongdautumia", "new_khachhang",
                        "new_khachhangdoanhnghiep", "new_vudautu","new_phieudangkyphanbon",
                        "new_tram","new_canbonongvu","new_ngaynhan","new_masophieu","new_lannhan" }));

            string sophieu = phieugiaonhan.Contains("new_masophieu") ? (string)phieugiaonhan["new_masophieu"] : "";
            int lannhan = phieugiaonhan.Contains("new_lannhan") ? (int)phieugiaonhan["new_lannhan"] : 0;

            if (!phieugiaonhan.Contains("new_ngaynhan"))
                throw new Exception("Phiếu giao nhận không có ngày lập phiếu");

            if (!phieugiaonhan.Contains("new_phieudangkyphanbon"))
                throw new Exception("Phiếu giao nhận không có phiếu đăng ký phân bón");

            Entity tram = null;
            Entity cbnv = null;
            Entity vudautu = service.Retrieve("new_vudautu",
                        ((EntityReference)phieugiaonhan["new_vudautu"]).Id, new ColumnSet(new string[] { "new_vudautuid" }));

            if (phieugiaonhan.Contains("new_tram"))
                tram = service.Retrieve("businessunit", ((EntityReference)phieugiaonhan["new_tram"]).Id,
                    new ColumnSet(new string[] { "businessunitid" }));

            if (phieugiaonhan.Contains("new_canbonongvu"))
                cbnv = service.Retrieve("new_kiemsoatvien", ((EntityReference)phieugiaonhan["new_canbonongvu"]).Id,
                    new ColumnSet(new string[] { "new_kiemsoatvienid" }));

            List<Entity> lstChitietPGNHM = RetrieveMultiRecord(service, "new_chitietgiaonhanphanbon",
                    new ColumnSet(new string[] { "new_sotienhl", "new_sotienkhl", "new_ngaynhan" }),
                    "new_phieugiaonhanphanbon", phieugiaonhan.Id);

            EntityCollection entcChitiet = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieugiaonhanphanbon",
                "new_new_pgnphanbon_new_chitiethddtmia",
                new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_phieugiaonhanphanbonid", phieugiaonhan.Id);

            decimal tongdmhl = 0;
            decimal tongdmkhl = 0;

            List<Entity> lstThoai = RetrieveMultiRecord(service, "new_thuadat_pdkphanbon",
                new ColumnSet(true), "new_phieudangky", ((EntityReference)phieugiaonhan["new_phieudangkyphanbon"]).Id);
            trace.Trace("Số lượng entity từ pdk : " + lstThoai.Count.ToString());
            if (entcChitiet.Entities.Count == 0)
                return;

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
                throw new Exception("Phiếu giao nhận không có khách hàng");


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

            trace.Trace("vong lap chi tiet hddt mia");
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
            trace.Trace("Vong lap chi tiet giai ngan : " + lstChitietPGNHM.Count.ToString());
            foreach (Entity giaingan in lstChitietPGNHM) // vong lap giai ngan
            {
                decimal phanbokhonghoanlai = giaingan.Contains("new_sotienkhl") ? ((Money)giaingan["new_sotienkhl"]).Value : new decimal(0);
                decimal phanbohoanlai = giaingan.Contains("new_sotienhl") ? ((Money)giaingan["new_sotienhl"]).Value : new decimal(0);
                DateTime ngaynhan = (DateTime)giaingan["new_ngaynhan"];

                Dictionary<Guid, decimal> dtTungthua = new Dictionary<Guid, decimal>();

                foreach (Guid key in dtDinhMuc.Keys)
                {
                    DinhMuc a = dtDinhMuc[key];
                    if (tongdmhl == 0)
                        throw new Exception("Tổng định mức hoàn lại phải khác 0");

                    decimal sotien = phanbohoanlai * a.dinhMucHL / tongdmhl;

                    if (!dtTungthua.ContainsKey(key))
                    {
                        dtTungthua.Add(key, sotien);
                    }
                }

                foreach (Guid key in dtTungthua.Keys)
                {
                    List<Tylethuhoivon> lstTylethuhoivon = dtTyleThuhoi[key];
                    decimal dinhmuc = dtTungthua[key];

                    Entity thuadatcanhtac = service.Retrieve("new_thuadatcanhtac", key,
                        new ColumnSet(new string[] { "new_laisuat", "new_name", "new_loailaisuat", "new_dachihoanlai_phanbon" }));

                    foreach (Tylethuhoivon a in lstTylethuhoivon)
                    {
                        Entity tilethuhoivon = service.Retrieve("new_tylethuhoivondukien", a.tylethuhoiid,
                                new ColumnSet(new string[] { "new_sotienthuhoi", "new_tiendaphanbo" }));
                        decimal tiendaphanbo = tilethuhoivon.Contains("new_tiendaphanbo") ?
                             ((Money)tilethuhoivon["new_tiendaphanbo"]).Value : new decimal(0);

                        if (dinhmuc < a.sotien - a.daphanbo)
                        {
                            CreatePBDT(hddtmia, KH, thuadatcanhtac, vudautu.ToEntityReference(), a.vuthuhoi, dinhmuc, idCRE, type = 2,
                                tram, cbnv, ngaynhan, phieugiaonhan, sophieu, lannhan);
                            tiendaphanbo = tiendaphanbo + dinhmuc;
                            tilethuhoivon["new_tiendaphanbo"] = new Money(tiendaphanbo);
                            service.Update(tilethuhoivon);
                            break;
                        }
                        else if (dinhmuc > a.sotien - a.daphanbo)
                        {
                            CreatePBDT(hddtmia, KH, thuadatcanhtac, vudautu.ToEntityReference(), a.vuthuhoi, a.sotien - a.daphanbo, idCRE,
                                type = 2, tram, cbnv, ngaynhan, phieugiaonhan, sophieu, lannhan);
                            tiendaphanbo = tiendaphanbo + (a.sotien - a.daphanbo);
                            dinhmuc = dinhmuc - (a.sotien + a.daphanbo);
                            tilethuhoivon["new_tiendaphanbo"] = new Money(tiendaphanbo);
                            service.Update(tilethuhoivon);
                        }
                    }
                }
            }
            trace.Trace("end phan bo dau tu");
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

        public void CreatePBDT(Entity hddtmia, Entity KH, Entity thuadatcanhtac, EntityReference vudautu, EntityReference vuthanhtoan,
            decimal sotien, Guid etlID, int type, Entity tram, Entity cbnv, DateTime ngaygiaonhan, Entity pgnpb, string sophieu, int lannhan)
        {
            trace.Trace("Tạo phiếu phân bổ");
            bool colai = false;
            int loailaisuat = ((OptionSetValue)thuadatcanhtac["new_loailaisuat"]).Value;

            if (loailaisuat == 100000000 && thuadatcanhtac.Contains("new_laisuat"))
                throw new Exception(thuadatcanhtac["new_name"].ToString() + " không có lãi suất");

            if (loailaisuat == 100000000) // cố định
                colai = true;

            // type = 1 - khl , type = 2 - hl
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

                phanbodautuKHL["new_name"] = Name.ToString();

                if (KH.LogicalName == "contact")
                    phanbodautuKHL["new_khachhang"] = KH.ToEntityReference();
                else if (KH.LogicalName == "account")
                    phanbodautuKHL["new_khachhangdoanhnghiep"] = KH.ToEntityReference();

                phanbodautuKHL["new_etltransaction"] = new EntityReference("new_etltransaction", etlID);

                if (type == 2)
                {
                    phanbodautuKHL["new_loaidautu"] = new OptionSetValue(100000000); // credit
                    thuadatcanhtac["new_dachihoanlai_phanbon"] = new Money(sotien);
                }
                else if (type == 1)
                {
                    phanbodautuKHL["new_loaidautu"] = new OptionSetValue(100000002); // standard
                    thuadatcanhtac["new_dachikhonghoanlai_phanbon"] = new Money(sotien);
                }

                phanbodautuKHL["new_loaihopdong"] = new OptionSetValue(100000000);
                phanbodautuKHL["new_hopdongdautumia"] = hddtmia.ToEntityReference();
                phanbodautuKHL["new_thuacanhtac"] = thuadatcanhtac.ToEntityReference();
                phanbodautuKHL["new_vuthanhtoan"] = vudautu;
                phanbodautuKHL["new_vudautu"] = vuthanhtoan;
                phanbodautuKHL["new_sotien"] = new Money(sotien);
                phanbodautuKHL["new_sotien_phanbon"] = new Money(sotien);
                phanbodautuKHL["new_conlai"] = new Money(sotien);
                phanbodautuKHL["new_tram"] = tram.ToEntityReference();
                phanbodautuKHL["new_cbnv"] = cbnv.ToEntityReference();
                phanbodautuKHL["new_ngayphatsinh"] = ngaygiaonhan;
                phanbodautuKHL["new_phieugiaonhanphanbon"] = pgnpb.ToEntityReference();
                phanbodautuKHL["new_loailaisuat"] = new OptionSetValue(loailaisuat);
                phanbodautuKHL["new_mucdichdautu"] = new OptionSetValue(100000000);
                phanbodautuKHL["new_sophieu"] = sophieu;
                phanbodautuKHL["new_lanthucnhan"] = lannhan;

                if (colai == true)
                    phanbodautuKHL["new_laisuat"] = thuadatcanhtac["new_laisuat"];
                else
                    phanbodautuKHL["new_laisuat"] = Getlaisuat(vudautu, 100000000, ngaygiaonhan);

                service.Create(phanbodautuKHL);
                service.Update(thuadatcanhtac);
                #endregion
            }
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
            Entity kq = null;
            decimal result = 0;

            for (int i = 0; i < bls.Entities.Count; i++)
            {
                Entity q = bls[i];

                if (CompareDate(ngaygiaonhan, (DateTime)q["new_ngayapdung"]) < 0)
                {
                    kq = bls[i - 1];
                    result = (decimal)kq["new_phantramlaisuat"];
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

        List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>();
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
    }
}
