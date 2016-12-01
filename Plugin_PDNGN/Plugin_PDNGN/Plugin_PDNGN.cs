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
        public ITracingService traceService;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            try
            {
                IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

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
                    if (!PDNGN.Contains("new_ngaydukienchi"))
                    {
                        throw new Exception("Chưa có ngày dự kiến chi!");
                    }
                    Entity KH = null;
                    if (PDNGN.Contains("new_khachhang"))
                        KH = service.Retrieve("contact", ((EntityReference)PDNGN["new_khachhang"]).Id, new ColumnSet(true));
                    else
                        KH = service.Retrieve("account", ((EntityReference)PDNGN["new_khachhangdoanhnghiep"]).Id, new ColumnSet(true));
                    traceService.Trace("begin");
                    #region begin
                    // TH so tien hoan lai > 0 PRE => GEN ETL
                    if (((Money)PDNGN["new_sotiendthoanlai"]).Value > 0)
                    {
                        traceService.Trace("begin1");
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
                        traceService.Trace("begin2");
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
                        traceService.Trace("begin3");
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
                        traceService.Trace("begin4");
                        etl_ND["new_invoiceamount"] = new Money(((Money)PDNGN["new_sotiendthoanlai"]).Value);
                        etl_ND["new_gldate"] = PDNGN["new_ngaydukienchi"];
                        etl_ND["new_invoicetype"] = "PRE";

                        if (PDNGN.Contains("new_khachhang"))
                            etl_ND["new_khachhang"] = PDNGN["new_khachhang"];
                        else if (PDNGN.Contains("new_khachhangdoanhnghiep"))
                            etl_ND["new_khachhangdoanhnghiep"] = PDNGN["new_khachhangdoanhnghiep"];

                        traceService.Trace("2.0");
                        Guid etl_NDID = service.Create(etl_ND);

                        etl_ND["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                        etl_ND["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                        etl_ND["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                        etl_ND["tran_type"] = "PRE";

                        Send(etl_ND);
                        //GenPhanBoDauTuHL(target, etl_NDID);
                        traceService.Trace("2");
                        if (PDNGN.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)PDNGN["new_phuongthucthanhtoan"]).Value == 100000001) // neu la chuyen khoan
                        {
                            #region Pay nếu là chuyển khoản
                            Entity paytamung = new Entity("new_applytransaction");
                            //apply_PGNPhanbon["new_documentsequence"] = value++;
                            paytamung["new_suppliersitecode"] = "Tây Ninh";

                            if (PDNGN.Contains("new_taikhoan"))
                            {
                                Entity taikhoanchinh = service.Retrieve("new_taikhoannganhang", ((EntityReference)PDNGN["new_taikhoan"]).Id, new ColumnSet(true));
                                paytamung["new_bankcccountnum"] = taikhoanchinh["new_sotaikhoan"];
                            }

                            if (PDNGN.Contains("new_taikhoannganhangttcs"))
                            {
                                Entity taikhoanchinh = service.Retrieve("new_taikhoannganhangcuattcs", ((EntityReference)PDNGN["new_taikhoannganhangttcs"]).Id, new ColumnSet(true));
                                paytamung["new_supplierbankname"] = taikhoanchinh["new_name"];
                            }

                            Entity etl_entityCRE = service.Retrieve("new_etltransaction", etl_NDID, new ColumnSet(new string[] { "new_name" }));
                            if (etl_entityCRE != null && etl_entityCRE.Contains("new_name"))
                            {
                                paytamung["new_name"] = (string)etl_entityCRE["new_name"];
                            }

                            paytamung["new_paymentamount"] = PDNGN["new_sotiendthoanlai"];
                            paytamung["new_paymentdate"] = PDNGN["new_ngayduyet"];
                            paytamung["new_paymentdocumentname"] = "CANTRU_03";
                            paytamung["new_vouchernumber"] = "BN";
                            paytamung["new_cashflow"] = "00.00";

                            paytamung["new_paymentnum"] = 1;
                            paytamung["new_referencenumber"] = PDNGN["new_masophieu"].ToString();
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
                    traceService.Trace("3");
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
                        etl_ND["new_gldate"] = PDNGN["new_ngaydukienchi"];
                        etl_ND["new_invoicetype"] = "STA";

                        if (PDNGN.Contains("new_khachhang"))
                            etl_ND["new_khachhang"] = PDNGN["new_khachhang"];
                        else if (PDNGN.Contains("new_khachhangdoanhnghiep"))
                            etl_ND["new_khachhangdoanhnghiep"] = PDNGN["new_khachhangdoanhnghiep"];

                        Guid etl_NDID = service.Create(etl_ND);

                        etl_ND["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                        etl_ND["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                        etl_ND["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                        etl_ND["tran_type"] = "STA";

                        Send(etl_ND);
                        //GenPhanBoDauTuKHL(target, etl_NDID);
                        #endregion

                        if (PDNGN.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)PDNGN["new_phuongthucthanhtoan"]).Value == 100000001) // neu la chuyen khoan
                        {
                            #region Pay nếu là chuyển khoản
                            Entity paytamung = new Entity("new_applytransaction");
                            //apply_PGNPhanbon["new_documentsequence"] = value++;
                            paytamung["new_suppliersitecode"] = "Tây Ninh";

                            if (PDNGN.Contains("new_taikhoan"))
                            {
                                Entity taikhoanchinh = service.Retrieve("new_taikhoannganhang", ((EntityReference)PDNGN["new_taikhoan"]).Id, new ColumnSet(true));
                                paytamung["new_bankcccountnum"] = taikhoanchinh["new_sotaikhoan"];
                            }

                            if (PDNGN.Contains("new_taikhoannganhangttcs"))
                            {
                                Entity taikhoanchinh = service.Retrieve("new_taikhoannganhangcuattcs", ((EntityReference)PDNGN["new_taikhoannganhangttcs"]).Id, new ColumnSet(true));
                                paytamung["new_supplierbankname"] = taikhoanchinh["new_name"];
                            }

                            Entity etl_entityCRE = service.Retrieve("new_etltransaction", etl_NDID, new ColumnSet(new string[] { "new_name" }));
                            if (etl_entityCRE != null && etl_entityCRE.Contains("new_name"))
                            {
                                paytamung["new_name"] = (string)etl_entityCRE["new_name"];
                            }

                            paytamung["new_paymentamount"] = PDNGN["new_sotiendtkhonghoanlai"];
                            paytamung["new_paymentdate"] = PDNGN["new_ngayduyet"];
                            paytamung["new_paymentdocumentname"] = "CANTRU_03";
                            paytamung["new_vouchernumber"] = "BN";
                            paytamung["new_cashflow"] = "00.00";
                            paytamung["new_paymentnum"] = 1;
                            paytamung["new_referencenumber"] = PDNGN["new_masophieu"].ToString();
                            paytamung["new_documentnum"] = PDNGN["new_masophieu"].ToString();

                            if (PDNGN.Contains("new_khachhang"))
                                paytamung["new_khachhang"] = PDNGN["new_khachhang"];
                            else if (PDNGN.Contains("new_khachhangdoanhnghiep"))
                                paytamung["new_khachhangdoanhnghiep"] = PDNGN["new_khachhangdoanhnghiep"];

                            paytamung.Id = service.Create(paytamung);

                            paytamung["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                            paytamung["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                            paytamung["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                            paytamung["new_type"] = "TYPE4";
                            Send(paytamung);
                            #endregion
                        }
                    }
                    #endregion
                    traceService.Trace("4");
                    Send(null);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
        }
        public void CreatePBDT_Mia(Entity hddtmia, Entity KH, Guid tdct, EntityReference vudautu,
            decimal sotien, Entity tram, Entity cbnv, DateTime ngaylapphieu,
            Entity giaingan, EntityReference thuadat)
        {
            traceService.Trace("Tạo phiếu phân bổ");
            bool colai = false;
            Entity thuadatcanhtac = service.Retrieve("new_thuadatcanhtac", tdct,
                new ColumnSet(new string[] { "new_laisuat", "new_name", "new_loailaisuat", "new_dachikhonghoanlai_phanbon", "new_dachihoanlai_phanbon" }));

            if (((OptionSetValue)thuadatcanhtac["new_loailaisuat"]).Value == 100000000)
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
                //phanbodautu["new_etltransaction"] =
                phanbodautuKHL["new_name"] = Name.ToString();
                traceService.Trace("Name");
                if (KH.LogicalName == "contact")
                    phanbodautuKHL["new_khachhang"] = KH.ToEntityReference();
                else if (KH.LogicalName == "account")
                    phanbodautuKHL["new_khachhangdoanhnghiep"] = KH.ToEntityReference();

                traceService.Trace("update tdct");
                phanbodautuKHL["new_loaihopdong"] = new OptionSetValue(100000000);
                phanbodautuKHL["new_hopdongdautumia"] = hddtmia.ToEntityReference();
                phanbodautuKHL["new_thuacanhtac"] = new EntityReference("new_thuadatcanhtac", tdct);
                phanbodautuKHL["new_vudautu"] = vudautu;
                phanbodautuKHL["new_sotien"] = new Money(sotien);
                phanbodautuKHL["new_conlai"] = new Money(sotien);
                phanbodautuKHL["new_tram"] = tram.ToEntityReference();
                phanbodautuKHL["new_vbnv"] = cbnv.ToEntityReference();
                phanbodautuKHL["new_ngayphatsinh"] = ngaylapphieu;
                phanbodautuKHL["new_phieudenghigiaingan"] = giaingan.ToEntityReference();

                if (thuadat != null)
                    phanbodautuKHL["new_thuadat"] = thuadat;

                if (colai == true)
                    phanbodautuKHL["new_laisuat"] = thuadatcanhtac["new_laisuat"];

                service.Create(phanbodautuKHL);
                traceService.Trace("End tao phan bo dau tu");
                #endregion
            }
        }
        public void CreatePBDT_Thuedat(Entity hdthuedat, Entity KH, EntityReference chitietthuedat, EntityReference vudautu,
            decimal sotien, Entity tram, Entity cbnv, DateTime ngaylapphieu,
            Entity giaingan, EntityReference thuadat)
        {
            traceService.Trace("Tạo phiếu phân bổ");

            // type = 1 - khl , type = 2 - hl
            if (sotien > 0)
            {
                StringBuilder Name = new StringBuilder();
                Name.Append("PBDT");

                if (hdthuedat.Contains("new_masohopdong"))
                    Name.Append("-" + hdthuedat["new_masohopdong"].ToString());

                if (KH.Contains("fullname"))
                    Name.Append("-" + KH["fullname"]);

                else if (KH.Contains("name"))
                    Name.Append("-" + KH["name"]);

                #region phan bo KHL
                Entity phanbodautuKHL = new Entity("new_phanbodautu");

                phanbodautuKHL["new_name"] = Name.ToString();
                traceService.Trace("Name");
                if (KH.LogicalName == "contact")
                    phanbodautuKHL["new_khachhang"] = KH.ToEntityReference();
                else if (KH.LogicalName == "account")
                    phanbodautuKHL["new_khachhangdoanhnghiep"] = KH.ToEntityReference();

                traceService.Trace("update tdct");
                phanbodautuKHL["new_loaihopdong"] = new OptionSetValue(100000000);
                phanbodautuKHL["new_hopdaudaututhuedat"] = hdthuedat.ToEntityReference();
                phanbodautuKHL["new_chitiethddtthuedat"] = chitietthuedat;
                phanbodautuKHL["new_vudautu"] = vudautu;
                phanbodautuKHL["new_sotien"] = new Money(sotien);
                phanbodautuKHL["new_conlai"] = new Money(sotien);
                phanbodautuKHL["new_tram"] = tram.ToEntityReference();
                phanbodautuKHL["new_vbnv"] = cbnv.ToEntityReference();
                phanbodautuKHL["new_ngayphatsinh"] = ngaylapphieu;
                phanbodautuKHL["new_phieudenghigiaingan"] = giaingan.ToEntityReference();

                if (thuadat != null)
                    phanbodautuKHL["new_thuadat"] = thuadat;

                service.Create(phanbodautuKHL);
                traceService.Trace("End tao phan bo dau tu");
                #endregion
            }
        }

        public void GenPhanBoDauTu(Entity target, Guid etlID)
        {
            Entity pdngn = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(true));

            Entity tram = null;
            Entity cbnv = null;
            Entity KH = null;
            EntityReference vudautu = null;
            Entity hdmia = null;
            Entity hdthuedat = null;
            Entity hdtrangthietbi = null;
            EntityReference chitiethdthuedat = null;

            DateTime ngayduyet = ((DateTime)pdngn["new_ngayduyet"]);
            int loaihopdong = ((OptionSetValue)pdngn["new_loaihopdong"]).Value;
            string sophieu = (string)pdngn["new_masophieu"];

            if (pdngn.Contains("new_khachhang"))
                KH = service.Retrieve("contact", ((EntityReference)pdngn["new_khachhang"]).Id,
                    new ColumnSet(new string[] { "fullname" }));

            else if (pdngn.Contains("new_khachhangdoanhnghiep"))
                KH = service.Retrieve("account", ((EntityReference)pdngn["new_khachhangdoanhnghiep"]).Id,
                    new ColumnSet(new string[] { "name" }));

            if (pdngn.Contains("new_tram"))
                tram = service.Retrieve("businessunit", ((EntityReference)pdngn["new_tram"]).Id,
                    new ColumnSet(new string[] { "businessunitid" }));

            if (pdngn.Contains("new_canbonongvu"))
                cbnv = service.Retrieve("new_kiemsoatvien", ((EntityReference)pdngn["new_canbonongvu"]).Id,
                    new ColumnSet(new string[] { "new_kiemsoatvienid" }));

            List<Entity> lstChitietgiaingan = RetrieveMultiRecord(service, "new_chitietphieudenghigiaingan",
                new ColumnSet(true), "new_phieudenghigiaingan", pdngn.Id);

            vudautu = (EntityReference)pdngn["new_vudautu"];

            foreach (Entity ct in lstChitietgiaingan)
            {
                if (loaihopdong == 100000000) // hd mía
                {
                    int noidunggiaingan = ((OptionSetValue)ct["new_noidunggiaingan"]).Value;
                    hdmia = service.Retrieve("new_hopdongdautumia", ((EntityReference)pdngn["new_hopdongdautumia"]).Id,
                new ColumnSet(new string[] { "new_masohopdong" }));

                    switch (noidunggiaingan)
                    {
                        case 100000000:
                            {

                            }
                            break;
                        case 100000001: // tuoi mia
                            {
                                EntityReference nttuoimia = (EntityReference)ct["new_nghiemthutuoimia"];

                                List<Entity> lstchitiet = RetrieveMultiRecord(service, "new_chitietnghiemthutuoimia",
                                    new ColumnSet(new string[] { "new_tongtiendautu", "new_thuadat" }), "new_nghiemthutuoimia", nttuoimia.Id);

                                foreach (Entity chitiet in lstchitiet)
                                {
                                    decimal tongtiendautu = ((Money)chitiet["new_tongtiendautu"]).Value;
                                    EntityReference thuadat = (EntityReference)chitiet["new_thuadat"];
                                    Entity thuadatcanhtac = GetThuadatcanhtacfromthuadat(thuadat, hdmia.ToEntityReference());

                                    CreatePBDT_Mia(hdmia, KH, thuadatcanhtac.Id, vudautu, tongtiendautu, tram, cbnv, ngayduyet, pdngn, thuadat);
                                }
                            }
                            break;
                        case 100000002: // boc la mia
                            {
                                EntityReference ntboclamia = (EntityReference)ct["new_nghiemthuboclamia"];

                                List<Entity> lstchitiet = RetrieveMultiRecord(service, "new_chitietnghiemthuboclamia",
                                    new ColumnSet(new string[] { "new_sotien", "new_chitiethddtmia" }), "new_nghiemthuboclamia", ntboclamia.Id);

                                foreach (Entity chitiet in lstchitiet)
                                {
                                    decimal tongtiendautu = ((Money)chitiet["new_sotien"]).Value;
                                    EntityReference thuadatcanhtac = (EntityReference)chitiet["new_chitiethddtmia"];
                                    EntityReference thuadat = null;

                                    CreatePBDT_Mia(hdmia, KH, thuadatcanhtac.Id, vudautu, tongtiendautu, tram, cbnv, ngayduyet, pdngn, thuadat);
                                }
                            }
                            break;
                        case 100000003: // nt dau tu bo sung von
                            {
                                EntityReference ntdautubosungvon = (EntityReference)ct["new_danhgianangsuat"];

                                List<Entity> lstchitiet = RetrieveMultiRecord(service, "new_chitietnhiemthudautubosungvon",
                                    new ColumnSet(new string[] { "new_denghihoanlaitienmat", "new_thuadat" }),
                                    "new_nghiemthudautubosungvon", ntdautubosungvon.Id);

                                foreach (Entity chitiet in lstchitiet)
                                {
                                    decimal sotien = ((Money)chitiet["new_denghihoanlaitienmat"]).Value;
                                    EntityReference thuadat = (EntityReference)chitiet["new_thuadat"];
                                    Entity thuadatcanhtac = GetThuadatcanhtacfromthuadat(thuadat, hdmia.ToEntityReference());

                                    CreatePBDT_Mia(hdmia, KH, thuadatcanhtac.Id, vudautu, sotien, tram, cbnv, ngayduyet, pdngn, thuadat);
                                }
                            }
                            break;
                    }
                }
                else if (loaihopdong == 100000001) // hd thue dat
                {
                    EntityReference ntthuedat = (EntityReference)ct["new_nghiemthuthuedat"];

                    hdthuedat = service.Retrieve("new_hopdongthuedat", ((EntityReference)pdngn["new_hopdongdaututhuedat"]).Id,
                        new ColumnSet(new string[] { "new_mahopdong" }));
                    chitiethdthuedat = (EntityReference)pdngn["new_chitiethdthuedat"];

                    List<Entity> lstChitiet = RetrieveMultiRecord(service, "new_chitietnghiemthuthuedat",
                        new ColumnSet(new string[] { "new_sotiendautu" }), "new_nghiemthuthuedat", ntthuedat.Id);

                    foreach (Entity chitiet in lstChitiet)
                    {
                        decimal sotien = ((Money)chitiet["new_sotiendautu"]).Value;
                        EntityReference thuadat = (EntityReference)chitiet["new_thuadat"];

                        CreatePBDT_Thuedat(hdthuedat, KH, chitiethdthuedat, vudautu, sotien, tram, cbnv, ngayduyet, pdngn, thuadat);
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

        private Entity GetThuadatcanhtacfromthuadat(EntityReference thuadat, EntityReference hdmia)
        {
            Entity rs = null;

            QueryExpression q = new QueryExpression("new_thuadatcanhtac");
            q.ColumnSet = new ColumnSet(new string[] { "" });
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, thuadat.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, hdmia.Id));
            q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));

            EntityCollection entc = service.RetrieveMultiple(q);
            if (entc.Entities.Count > 0)
                rs = entc.Entities[0];

            return rs;
        }
    }
}
