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
        ITracingService traceService;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            try
            {
                IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);

                Entity target = (Entity)context.InputParameters["Target"];
                Entity vudautu = null;
                Entity HDMia = null;
                Entity HDTD = null;
                Entity HDTTB = null;

                if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000) // da duyet
                {
                    // lay ra phieu DN giai ngan hien tai
                    Entity PDNGN = service.Retrieve("new_phieudenghigiaingan", target.Id, new ColumnSet(true));
                    int loaihopdong = ((OptionSetValue)PDNGN["new_loaihopdong"]).Value;

                    if (PDNGN.Contains("new_vudautu") && ((EntityReference)PDNGN["new_vudautu"]).Id != null && ((EntityReference)PDNGN["new_vudautu"]).Id.ToString() != "undefined")
                        vudautu = service.Retrieve("new_vudautu", ((EntityReference)PDNGN["new_vudautu"]).Id, new ColumnSet(new string[] { "new_mavudautu" }));

                    if (loaihopdong == 100000000)
                    {
                        if (!PDNGN.Contains("new_hopdongdautumia"))
                            throw new Exception("Không có hợp đồng đầu tư mía");

                        HDMia = service.Retrieve("new_hopdongdautumia", ((EntityReference)PDNGN["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_masohopdong" }));
                    }
                    if (loaihopdong == 100000001)
                    {
                        if (!PDNGN.Contains("new_hopdongdaututhuedat"))
                            throw new Exception("Không có hợp đồng đầu tư thuê đất");

                        HDTD = service.Retrieve("new_hopdongthuedat", ((EntityReference)PDNGN["new_hopdongdaututhuedat"]).Id, new ColumnSet(new string[] { "new_mahopdong" }));
                    }
                    if (loaihopdong == 100000002)
                    {
                        if (!PDNGN.Contains("new_hopdongdautummtb"))
                            throw new Exception("Không có hợp đồng đầu tư trang thiết bị");

                        HDTTB = service.Retrieve("new_hopdongdaututrangthietbi", ((EntityReference)PDNGN["new_hopdongdautummtb"]).Id, new ColumnSet(new string[] { "new_sohopdong" }));
                    }
                    if (!PDNGN.Contains("new_ngaydukienchi"))
                        throw new Exception("Phiếu đề nghị giải ngân không có ngày dự kiến chi");

                    if (!PDNGN.Contains("new_ngayduyet"))
                        throw new Exception("Phiếu đề nghị giải ngân không có ngày duyet");

                    Entity KH = null;

                    if (PDNGN.Contains("new_khachhang"))
                        KH = service.Retrieve("contact", ((EntityReference)PDNGN["new_khachhang"]).Id,
                            new ColumnSet(new string[] { "new_makhachhang", "new_socmnd" }));
                    else
                        KH = service.Retrieve("account", ((EntityReference)PDNGN["new_khachhangdoanhnghiep"]).Id,
                            new ColumnSet(new string[] { "new_makhachhang", "new_masothue" }));

                    #region begin
                    // TH so tien hoan lai > 0 PRE => GEN ETL
                    if (PDNGN.Contains("new_sotiendthoanlai") && ((Money)PDNGN["new_sotiendthoanlai"]).Value > 0)
                    {
                        traceService.Trace("so tien hoan lai");
                        Entity etl_ND = new Entity("new_etltransaction");
                        if (((OptionSetValue)PDNGN["new_phuongthucthanhtoan"]).Value.ToString() == "100000000")
                            etl_ND["new_name"] = PDNGN["new_masophieu"].ToString() + "_PRE";

                        etl_ND["new_vouchernumber"] = "DTND";
                        etl_ND["new_transactiontype"] = "1.2.5.a";
                        //etl_ND["new_customertype"] = new OptionSetValue(PDNGN.Contains("new_khachhang") ? 1 : 2); // ???
                        etl_ND["new_season"] = vudautu["new_mavudautu"].ToString();
                        etl_ND["new_sochungtu"] = PDNGN["new_masophieu"].ToString();
                        etl_ND["new_lannhan"] = PDNGN["new_langiaingan"];

                        if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000000")
                        { // Mía
                            etl_ND["new_contractnumber"] = HDMia["new_masohopdong"].ToString();
                            etl_ND["new_descriptionheader"] = "Giải ngân hợp đồng mía";
                        }
                        else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000001")
                        { // Thuê đất
                            etl_ND["new_contractnumber"] = HDTD["new_mahopdong"].ToString();
                            etl_ND["new_descriptionheader"] = "Giải ngân hợp đồng thuê đất";
                        }
                        else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000002")
                        { // MMTTB
                            etl_ND["new_contractnumber"] = HDTTB["new_sohopdong"].ToString();
                            etl_ND["new_descriptionheader"] = "Giải ngân hợp đồng máy móc thiết bị";
                        }

                        etl_ND["new_tradingpartner"] = (KH.LogicalName.ToLower().Trim() == "contact" ?
                            ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : ""))
                            :
                            ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""))
                            );

                        etl_ND["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                        traceService.Trace("4");
                        etl_ND["new_suppliersite"] = "TAY NINH"; // fix cung
                        traceService.Trace("5");
                        etl_ND["new_invoicedate"] = PDNGN["new_ngaydukienchi"];
                        traceService.Trace("6");
                        etl_ND["new_terms"] = "Tra Ngay";
                        etl_ND["new_taxtype"] = "";
                        etl_ND["new_invoiceamount"] = new Money(((Money)PDNGN["new_sotiendthoanlai"]).Value);
                        etl_ND["new_gldate"] = PDNGN["new_ngayduyet"];
                        etl_ND["new_invoicetype"] = "PRE";
                        etl_ND["new_paymenttype"] = "TM";
                        if (PDNGN.Contains("new_khachhang"))
                            etl_ND["new_khachhang"] = PDNGN["new_khachhang"];
                        else if (PDNGN.Contains("new_khachhangdoanhnghiep"))
                            etl_ND["new_khachhangdoanhnghiep"] = PDNGN["new_khachhangdoanhnghiep"];
                        Guid etl_NDID = Guid.Empty;
                        GenPhanBoDauTu_HL(target, etl_NDID);

                        if (((OptionSetValue)PDNGN["new_phuongthucthanhtoan"]).Value == 100000001) // neu la chuyen khoan
                        {
                            #region Pay nếu là chuyển khoản
                            Entity paytamung = new Entity("new_applytransaction");
                            //apply_PGNPhanbon["new_documentsequence"] = value++;
                            paytamung["new_suppliersitecode"] = "Tây Ninh";


                            List<Entity> taikhoannganhang = RetrieveMultiRecord(service, "new_taikhoannganhang",
                                new ColumnSet(new string[] { "new_sotaikhoan", "new_giaodichchinh" }),
                                KH.LogicalName == "contact" ? "new_khachhang" : "new_khachhangdoanhnghiep", KH.Id);

                            Entity taikhoanchinh = null;

                            foreach (Entity en in taikhoannganhang)
                            {
                                if ((bool)en["new_giaodichchinh"] == true)
                                    taikhoanchinh = en;
                            }

                            paytamung["new_supplierbankname"] = (taikhoanchinh == null ? "CTXL-VND-0" : taikhoanchinh["new_sotaikhoan"]);
                            paytamung["new_paymentamount"] = PDNGN["new_sotienung"];
                            paytamung["new_paymentdate"] = PDNGN["new_ngayduyet"];
                            paytamung["new_paymentdocumentname"] = "CANTRU_03";
                            paytamung["new_vouchernumber"] = "BN";
                            paytamung["new_cashflow"] = "00.00";
                            paytamung["new_paymentnum"] = 1;
                            paytamung["new_documentnum"] = PDNGN["new_masophieutamung"].ToString();

                            if (PDNGN.Contains("new_khachhang"))
                                paytamung["new_khachhang"] = PDNGN["new_khachhang"];
                            else if (PDNGN.Contains("new_khachhangdoanhnghiep"))
                                paytamung["new_khachhangdoanhnghiep"] = PDNGN["new_khachhangdoanhnghiep"];

                            service.Create(paytamung);
                            Send(paytamung);
                            #endregion
                        }
                    }

                    // TH so tien khong hoan lai => GEN ETL va PAY
                    if (PDNGN.Contains("new_sotiendtkhonghoanlai") && ((Money)PDNGN["new_sotiendtkhonghoanlai"]).Value > 0)
                    {
                        traceService.Trace("so tien khong hoan lai");
                        #region GEN ETL
                        Entity etl_ND = new Entity("new_etltransaction");
                        if (((OptionSetValue)PDNGN["new_phuongthucthanhtoan"]).Value.ToString() == "100000000")
                        {
                            etl_ND["new_name"] = PDNGN["new_masophieu"].ToString() + "_STA";
                        }
                        etl_ND["new_vouchernumber"] = "DTND";
                        etl_ND["new_transactiontype"] = "1.1.3.a";
                        etl_ND["new_customertype"] = new OptionSetValue(PDNGN.Contains("new_khachhang") ? 1 : 2); // ???
                        etl_ND["new_season"] = vudautu["new_mavudautu"].ToString();
                        etl_ND["new_sochungtu"] = PDNGN["new_masophieu"].ToString();
                        etl_ND["new_lannhan"] = PDNGN["new_langiaingan"];

                        if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000000")
                        { // Mía
                            etl_ND["new_contractnumber"] = HDMia["new_masohopdong"].ToString();
                            etl_ND["new_descriptionheader"] = "Giải ngân hợp đồng mía";
                        }
                        else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000001")
                        { // Thuê đất
                            etl_ND["new_contractnumber"] = HDTD["new_mahopdong"].ToString();
                            etl_ND["new_descriptionheader"] = "Giải ngân hợp đồng thuê đất";
                        }
                        else if (((OptionSetValue)PDNGN["new_loaihopdong"]).Value.ToString() == "100000002")
                        { // MMTTB
                            etl_ND["new_contractnumber"] = HDTTB["new_sohopdong"].ToString();
                            etl_ND["new_descriptionheader"] = "Giải ngân hợp đồng máy móc thiết bị";
                        }

                        etl_ND["new_tradingpartner"] = (KH.LogicalName.ToLower().Trim() == "contact" ?
                            ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : ""))
                            :
                            ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""))
                            );
                        etl_ND["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                        etl_ND["new_suppliersite"] = "TAY NINH"; // fix cung
                        etl_ND["new_invoicedate"] = PDNGN["new_ngaydukienchi"];
                        etl_ND["new_terms"] = "Tra Ngay";
                        etl_ND["new_taxtype"] = "";
                        etl_ND["new_invoiceamount"] = new Money(((Money)PDNGN["new_sotiendtkhonghoanlai"]).Value);
                        etl_ND["new_gldate"] = PDNGN["new_ngayduyet"];
                        etl_ND["new_invoicetype"] = "STA";
                        etl_ND["new_paymenttype"] = "TM";

                        if (PDNGN.Contains("new_khachhang"))
                            etl_ND["new_khachhang"] = PDNGN["new_khachhang"];
                        else if (PDNGN.Contains("new_khachhangdoanhnghiep"))
                            etl_ND["new_khachhangdoanhnghiep"] = PDNGN["new_khachhangdoanhnghiep"];

                        Guid etl_NDID = Guid.Empty;
                        GenPhanBoDauTu_KHL(target, etl_NDID);
                        #endregion                        

                        if (((OptionSetValue)PDNGN["new_phuongthucthanhtoan"]).Value == 100000001) // neu la chuyen khoan
                        {
                            #region Pay nếu là chuyển khoản
                            Entity paytamung = new Entity("new_applytransaction");
                            //apply_PGNPhanbon["new_documentsequence"] = value++;
                            paytamung["new_suppliersitecode"] = "Tây Ninh";


                            List<Entity> taikhoannganhang = RetrieveMultiRecord(service, "new_taikhoannganhang",
                                new ColumnSet(new string[] { "new_sotaikhoan", "new_giaodichchinh" }),
                                KH.LogicalName == "contact" ? "new_khachhang" : "new_khachhangdoanhnghiep", KH.Id);

                            Entity taikhoanchinh = null;

                            foreach (Entity en in taikhoannganhang)
                            {
                                if ((bool)en["new_giaodichchinh"] == true)
                                    taikhoanchinh = en;
                            }

                            paytamung["new_supplierbankname"] = (taikhoanchinh == null ? "CTXL-VND-0" : taikhoanchinh["new_sotaikhoan"]);

                            paytamung["new_paymentamount"] = PDNGN["new_sotiendtkhonghoanlai"];
                            paytamung["new_paymentdate"] = PDNGN["new_ngayduyet"];
                            paytamung["new_paymentdocumentname"] = "CANTRU_03";
                            paytamung["new_vouchernumber"] = "BN";
                            paytamung["new_cashflow"] = "00.00";
                            paytamung["new_paymentnum"] = 1;
                            paytamung["new_documentnum"] = PDNGN["new_masophieutamung"].ToString();

                            if (PDNGN.Contains("new_khachhang"))
                                paytamung["new_khachhang"] = PDNGN["new_khachhang"];
                            else if (PDNGN.Contains("new_khachhangdoanhnghiep"))
                                paytamung["new_khachhangdoanhnghiep"] = PDNGN["new_khachhangdoanhnghiep"];

                            service.Create(paytamung);
                            Send(paytamung);
                            #endregion
                        }
                    }
                    #endregion

                }
            }
            catch (Exception ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
        }

        public void CreatePBDT_Mia(Entity hddtmia, Entity KH, Guid tdct, EntityReference vudautu, EntityReference vuthuhoi,
            decimal sotien, Entity tram, Entity cbnv, DateTime ngaylapphieu,
            Entity giaingan, EntityReference thuadat, int mucdichdautu, int type, string sophieu)
        {
            traceService.Trace("Tạo phiếu phân bổ");

            Entity thuadatcanhtac = service.Retrieve("new_thuadatcanhtac", tdct,
                new ColumnSet(new string[] { "new_laisuat", "new_name", "new_loailaisuat",
                    "new_dachikhonghoanlai_tienmat", "new_dachihoanlai_tienmat" }));

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

                decimal dachihoanlai = thuadatcanhtac.Contains("new_dachihoanlai_tienmat") ? ((Money)thuadatcanhtac["new_dachihoanlai_tienmat"]).Value : new decimal(0);
                decimal dachikhonghoanlai = thuadatcanhtac.Contains("new_dachikhonghoanlai_tienmat") ? ((Money)thuadatcanhtac["new_dachikhonghoanlai_tienmat"]).Value : new decimal(0);

                if (type == 2) // hl
                {
                    phanbodautuKHL["new_loaidautu"] = new OptionSetValue(100000000); // credit
                    thuadatcanhtac["new_dachihoanlai_tienmat"] = new Money(sotien + dachihoanlai);
                }
                else if (type == 1) // khl
                {
                    phanbodautuKHL["new_loaidautu"] = new OptionSetValue(100000002); // standard
                    thuadatcanhtac["new_dachikhonghoanlai_tienmat"] = new Money(sotien + dachikhonghoanlai);
                }

                traceService.Trace("update tdct");
                phanbodautuKHL["new_loaihopdong"] = new OptionSetValue(100000000);
                phanbodautuKHL["new_hopdongdautumia"] = hddtmia.ToEntityReference();
                phanbodautuKHL["new_thuacanhtac"] = new EntityReference("new_thuadatcanhtac", tdct);
                phanbodautuKHL["new_vudautu"] = vudautu;
                phanbodautuKHL["new_vuthanhtoan"] = vuthuhoi;
                phanbodautuKHL["new_sotien"] = new Money(sotien);
                phanbodautuKHL["new_conlai"] = new Money(sotien);
                phanbodautuKHL["new_tram"] = tram.ToEntityReference();
                phanbodautuKHL["new_cbnv"] = cbnv.ToEntityReference();
                phanbodautuKHL["new_ngayphatsinh"] = ngaylapphieu;
                phanbodautuKHL["new_phieudenghigiaingan"] = giaingan.ToEntityReference();
                phanbodautuKHL["new_sophieu"] = sophieu;
                phanbodautuKHL["new_mucdichdautu"] = new OptionSetValue(mucdichdautu);

                if (thuadat != null)
                    phanbodautuKHL["new_thuadat"] = thuadat;

                phanbodautuKHL["new_laisuat"] = Getlaisuat(vudautu, mucdichdautu, ngaylapphieu);

                service.Create(phanbodautuKHL);
                traceService.Trace("End tao phan bo dau tu");
                #endregion
            }
        }

        public void CreatePBDT_Thuedat(Entity hdthuedat, Entity KH, EntityReference chitietthuedat, EntityReference vudautu, EntityReference vuthuhoi,
            decimal sotien, Entity tram, Entity cbnv, DateTime ngaylapphieu,
            Entity giaingan, EntityReference thuadat, int mucdichdautu, int type, string sophieu)
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

                //decimal dachihoanlai = thuadatcanhtac.Contains("new_dachihoanlai_tienmat") ? ((Money)thuadatcanhtac["new_dachihoanlai_tienmat"]).Value : new decimal(0);
                //decimal dachikhonghoanlai = thuadatcanhtac.Contains("new_dachikhonghoanlai_tienmat") ? ((Money)thuadatcanhtac["new_dachikhonghoanlai_tienmat"]).Value : new decimal(0);

                if (type == 2)
                {
                    phanbodautuKHL["new_loaidautu"] = new OptionSetValue(100000000); // credit
                    //thuadatcanhtac["new_dachihoanlai_tienmat"] = new Money(sotien + dachihoanlai);
                }
                else if (type == 1)
                {
                    phanbodautuKHL["new_loaidautu"] = new OptionSetValue(100000002); // standard
                    //thuadatcanhtac["new_dachikhonghoanlai_tienmat"] = new Money(sotien + dachikhonghoanlai);
                }

                traceService.Trace("update tdct");
                phanbodautuKHL["new_loaihopdong"] = new OptionSetValue(100000000);
                phanbodautuKHL["new_hopdaudaututhuedat"] = hdthuedat.ToEntityReference();
                phanbodautuKHL["new_chitiethddtthuedat"] = chitietthuedat;
                phanbodautuKHL["new_vudautu"] = vudautu;
                phanbodautuKHL["new_vuthanhtoan"] = vuthuhoi;
                phanbodautuKHL["new_sotien"] = new Money(sotien);
                phanbodautuKHL["new_conlai"] = new Money(sotien);
                phanbodautuKHL["new_tram"] = tram.ToEntityReference();
                phanbodautuKHL["new_vbnv"] = cbnv.ToEntityReference();
                phanbodautuKHL["new_ngayphatsinh"] = ngaylapphieu;
                phanbodautuKHL["new_phieudenghigiaingan"] = giaingan.ToEntityReference();
                phanbodautuKHL["new_mucdichdautu"] = new OptionSetValue(mucdichdautu);
                phanbodautuKHL["new_sophieu"] = sophieu;

                if (thuadat != null)
                    phanbodautuKHL["new_thuadat"] = thuadat;

                phanbodautuKHL["new_laisuat"] = Getlaisuat(vudautu, mucdichdautu, ngaylapphieu);


                service.Create(phanbodautuKHL);
                traceService.Trace("End tao phan bo dau tu");
                #endregion
            }
        }

        public void GenPhanBoDauTu_HL(Entity target, Guid etlID)
        {
            traceService.Trace("Start phan bo");
            Entity pdngn = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(true));
            int type = 2;
            Entity tram = null;
            Entity cbnv = null;
            Entity KH = null;
            EntityReference vudautu = null;
            Entity hdmia = null;
            Entity hdthuedat = null;
            //Entity hdtrangthietbi = null;
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
            traceService.Trace("so luong chi tiet giai ngan " + lstChitietgiaingan.Count.ToString());

            foreach (Entity ct in lstChitietgiaingan)
            {
                #region mia
                if (loaihopdong == 100000000) // hd mía
                {
                    int noidunggiaingan = ((OptionSetValue)ct["new_noidunggiaingan"]).Value;
                    hdmia = service.Retrieve("new_hopdongdautumia", ((EntityReference)pdngn["new_hopdongdautumia"]).Id,
                new ColumnSet(new string[] { "new_masohopdong" }));

                    switch (noidunggiaingan)
                    {
                        case 100000000: // trong va cham soc mia 
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
                                    decimal sotien = ((Money)chitiet["new_tongtiendautu"]).Value;
                                    EntityReference thuadat = (EntityReference)chitiet["new_thuadat"];
                                    Entity thuadatcanhtac = GetThuadatcanhtacfromthuadat(thuadat, hdmia.ToEntityReference());

                                    List<Entity> lstTylethuhoivon = RetrieveMultiRecord(service, "new_tylethuhoivon",
                                        new ColumnSet(true), "new_thuadatcanhtac", thuadatcanhtac.Id);

                                    foreach (Entity tlth in lstTylethuhoivon)
                                    {
                                        EntityReference vuthuhoi = (EntityReference)tlth["new_vudautu"];
                                        decimal dinhmuc = sotien * (decimal)tlth["new_tylephantram"] / 100;

                                        decimal tiendaphanbo = tlth.Contains("new_tiendaphanbo") ?
                             ((Money)tlth["new_tiendaphanbo"]).Value : new decimal(0);
                                        decimal sotienthuhoi = tlth.Contains("new_sotienthuhoi") ?
                            ((Money)tlth["new_sotienthuhoi"]).Value : 0;

                                        decimal sotienphanbo = sotienthuhoi - tiendaphanbo;

                                        while (true)
                                        {
                                            if (dinhmuc < sotienphanbo)
                                            {
                                                CreatePBDT_Mia(hdmia, KH, thuadatcanhtac.Id, vudautu, vuthuhoi, dinhmuc, tram,
                                        cbnv, ngayduyet, pdngn, thuadat, 100000001, type, sophieu);
                                                tiendaphanbo = tiendaphanbo + dinhmuc;

                                                break;
                                            }
                                            else if (dinhmuc > sotienphanbo)
                                            {
                                                CreatePBDT_Mia(hdmia, KH, thuadatcanhtac.Id, vudautu, vuthuhoi, sotienphanbo, tram,
                                        cbnv, ngayduyet, pdngn, thuadat, 100000001, type, sophieu);
                                                tiendaphanbo = tiendaphanbo + sotienphanbo;
                                                dinhmuc = dinhmuc - sotienphanbo;
                                            }

                                            tlth["new_tiendaphanbo"] = new Money(tiendaphanbo);
                                            //service.Update(tlth);
                                        }
                                    }
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
                                    decimal sotien = ((Money)chitiet["new_sotien"]).Value;
                                    EntityReference thuadatcanhtac = (EntityReference)chitiet["new_chitiethddtmia"];
                                    EntityReference thuadat = null;

                                    List<Entity> lstTylethuhoi = RetrieveMultiRecord(service, "new_tylethuhoivondukien",
                    new ColumnSet(new string[] { "new_sotienthuhoi", "new_tiendaphanbo", "new_vudautu", "new_tylephantram" }),
                    "new_chitiethddtmia", thuadatcanhtac.Id);

                                    foreach (Entity tlth in lstTylethuhoi)
                                    {
                                        EntityReference vuthuhoi = (EntityReference)tlth["new_vudautu"];
                                        decimal dinhmuc = sotien * (decimal)tlth["new_tylephantram"] / 100;

                                        decimal tiendaphanbo = tlth.Contains("new_tiendaphanbo") ?
                             ((Money)tlth["new_tiendaphanbo"]).Value : new decimal(0);
                                        decimal sotienthuhoi = tlth.Contains("new_sotienthuhoi") ?
                            ((Money)tlth["new_sotienthuhoi"]).Value : 0;

                                        decimal sotienphanbo = sotienthuhoi - tiendaphanbo;

                                        while (true)
                                        {
                                            if (dinhmuc < sotienphanbo)
                                            {
                                                CreatePBDT_Mia(hdmia, KH, thuadatcanhtac.Id, vudautu, vuthuhoi, dinhmuc,
                                        tram, cbnv, ngayduyet, pdngn, thuadat, 100000003, type, sophieu);
                                                tiendaphanbo = tiendaphanbo + dinhmuc;

                                                break;
                                            }
                                            else if (dinhmuc > sotienphanbo)
                                            {
                                                CreatePBDT_Mia(hdmia, KH, thuadatcanhtac.Id, vudautu, vuthuhoi, sotienphanbo,
                                        tram, cbnv, ngayduyet, pdngn, thuadat, 100000003, type, sophieu);
                                                tiendaphanbo = tiendaphanbo + sotienphanbo;
                                                dinhmuc = dinhmuc - sotienphanbo;
                                            }

                                            tlth["new_tiendaphanbo"] = new Money(tiendaphanbo);
                                            //service.Update(tlth);
                                        }
                                    }
                                }
                            }
                            break;
                        case 100000003: // nt dau tu bo sung von
                            {
                                traceService.Trace("nt dau tu bo sung von");
                                EntityReference ntdautubosungvon = (EntityReference)ct["new_danhgianangsuat"];

                                List<Entity> lstchitiet = RetrieveMultiRecord(service, "new_chitietnhiemthudautubosungvon",
                                    new ColumnSet(new string[] { "new_denghihoanlaitienmat", "new_thuadat" }),
                                    "new_nghiemthudautubosungvon", ntdautubosungvon.Id);
                                traceService.Trace("So luong chi tiet :  " + lstchitiet.Count.ToString());
                                foreach (Entity chitiet in lstchitiet)
                                {
                                    decimal sotien = ((Money)chitiet["new_denghihoanlaitienmat"]).Value;
                                    traceService.Trace(sotien.ToString());
                                    EntityReference thuadat = (EntityReference)chitiet["new_thuadat"];
                                    Entity thuadatcanhtac = GetThuadatcanhtacfromthuadat(thuadat, hdmia.ToEntityReference());
                                    traceService.Trace(thuadatcanhtac["new_name"].ToString());
                                    List<Entity> lstTylethuhoi = RetrieveMultiRecord(service, "new_tylethuhoivondukien",
                    new ColumnSet(new string[] { "new_sotienthuhoi", "new_tiendaphanbo", "new_vudautu", "new_tylephantram" }),
                    "new_chitiethddtmia", thuadatcanhtac.Id);

                                    foreach (Entity tlth in lstTylethuhoi)
                                    {
                                        EntityReference vuthuhoi = (EntityReference)tlth["new_vudautu"];
                                        decimal dinhmuc = sotien * (decimal)tlth["new_tylephantram"] / 100;
                                        traceService.Trace(dinhmuc.ToString());
                                        decimal tiendaphanbo = tlth.Contains("new_tiendaphanbo") ?
                             ((Money)tlth["new_tiendaphanbo"]).Value : new decimal(0);
                                        decimal sotienthuhoi = tlth.Contains("new_sotienthuhoi") ?
                            ((Money)tlth["new_sotienthuhoi"]).Value : 0;

                                        decimal sotienphanbo = sotienthuhoi - tiendaphanbo;
                                        traceService.Trace("so tien phan bo : " + sotienphanbo.ToString());
                                        while (true)
                                        {
                                            if (dinhmuc < sotienphanbo)
                                            {
                                                CreatePBDT_Mia(hdmia, KH, thuadatcanhtac.Id, vudautu, vuthuhoi, dinhmuc, tram,
                                        cbnv, ngayduyet, pdngn, thuadat, 100000001, type, sophieu);
                                                tiendaphanbo = tiendaphanbo + dinhmuc;

                                                break;
                                            }
                                            else if (dinhmuc > sotienphanbo)
                                            {
                                                CreatePBDT_Mia(hdmia, KH, thuadatcanhtac.Id, vudautu, vuthuhoi, sotienphanbo, tram,
                                        cbnv, ngayduyet, pdngn, thuadat, 100000001, type, sophieu);
                                                tiendaphanbo = tiendaphanbo + sotienphanbo;
                                                dinhmuc = dinhmuc - sotienphanbo;
                                            }

                                            tlth["new_tiendaphanbo"] = new Money(tiendaphanbo);
                                            //service.Update(tlth);
                                        }
                                    }
                                }
                            }

                            break;
                    }
                }
                #endregion

                #region thue dat
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

                        CreatePBDT_Thuedat(hdthuedat, KH, chitiethdthuedat, vudautu, vudautu, sotien,
                            tram, cbnv, ngayduyet, pdngn, thuadat, 100000005, type, sophieu);
                    }
                }
                #endregion

                #region mmtb
                else if (loaihopdong == 100000002) // mmtb
                {

                }
                #endregion
            }
        }

        public void GenPhanBoDauTu_KHL(Entity target, Guid etlID)
        {
            traceService.Trace("Start phan bo");
            Entity pdngn = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(true));

            int type = 1;
            Entity tram = null;
            Entity cbnv = null;
            Entity KH = null;
            EntityReference vudautu = null;
            Entity hdmia = null;
            //Entity hdthuedat = null;
            //Entity hdtrangthietbi = null;
            //EntityReference chitiethdthuedat = null;

            DateTime ngayduyet = ((DateTime)pdngn["new_ngayduyet"]);
            int loaihopdong = ((OptionSetValue)pdngn["new_loaihopdong"]).Value;

            if (loaihopdong != 100000000) // hd mía
                return;

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
            traceService.Trace("so luong chi tiet giai ngan " + lstChitietgiaingan.Count.ToString());

            foreach (Entity ct in lstChitietgiaingan)
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

                                if (!thuadatcanhtac.Contains("new_chinhsachdautu"))
                                    throw new Exception(thuadatcanhtac["new_name"].ToString() + " không có chính sách đầu tư");

                                Entity CSDT = service.Retrieve("new_chinhsachdautu", ((EntityReference)thuadatcanhtac["new_chinhsachdautu"]).Id,
                        new ColumnSet(new string[] { "new_new_thoihanthuhoivon_khl", "new_machinhsach" }));

                                int sonamthuhoiKHL = CSDT.Contains("new_new_thoihanthuhoivon_khl") ? (int)CSDT["new_new_thoihanthuhoivon_khl"] : 0;
                                decimal sotienphanboKHL = 0;

                                if (sonamthuhoiKHL != 0)
                                    sotienphanboKHL = tongtiendautu / sonamthuhoiKHL;
                                else
                                    return;

                                List<Entity> lst = RetrieveVudautu().Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList<Entity>();
                                int curr = lst.FindIndex(p => p.Id == vudautu.Id);

                                for (int k = 0; k < sonamthuhoiKHL; k++)
                                {
                                    Entity vudaututhuhoi = lst[++curr];
                                    traceService.Trace(vudaututhuhoi["new_mavudautu"].ToString());
                                    CreatePBDT_Mia(hdmia, KH, thuadatcanhtac.Id, vudautu, vudaututhuhoi.ToEntityReference(), tongtiendautu, tram,
                                        cbnv, ngayduyet, pdngn, thuadat, 100000001, type, sophieu);

                                    if (curr > lst.Count - 1)
                                        throw new Exception("Phân bổ không hoàn lại thất bại. Vui lòng tạo thêm vụ đầu tư mới để phân bổ");
                                }
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
                                EntityReference thuadatcanhtacEnf = (EntityReference)chitiet["new_chitiethddtmia"];
                                Entity thuadatcanhtac = service.Retrieve(thuadatcanhtacEnf.LogicalName, thuadatcanhtacEnf.Id,
                                    new ColumnSet(new string[] { "new_chinhsachdautu", "new_name" }));

                                EntityReference thuadat = null;

                                if (!thuadatcanhtac.Contains("new_chinhsachdautu"))
                                    throw new Exception(thuadatcanhtac["new_name"].ToString() + " không có chính sách đầu tư");

                                Entity CSDT = service.Retrieve("new_chinhsachdautu", ((EntityReference)thuadatcanhtac["new_chinhsachdautu"]).Id,
                        new ColumnSet(new string[] { "new_new_thoihanthuhoivon_khl", "new_machinhsach" }));

                                int sonamthuhoiKHL = CSDT.Contains("new_new_thoihanthuhoivon_khl") ? (int)CSDT["new_new_thoihanthuhoivon_khl"] : 0;
                                decimal sotienphanboKHL = 0;

                                if (sonamthuhoiKHL != 0)
                                    sotienphanboKHL = tongtiendautu / sonamthuhoiKHL;
                                else
                                    return;

                                List<Entity> lst = RetrieveVudautu().Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList<Entity>();
                                int curr = lst.FindIndex(p => p.Id == vudautu.Id);

                                for (int k = 0; k < sonamthuhoiKHL; k++)
                                {
                                    Entity vudaututhuhoi = lst[++curr];
                                    traceService.Trace(vudaututhuhoi["new_mavudautu"].ToString());
                                    CreatePBDT_Mia(hdmia, KH, thuadatcanhtacEnf.Id, vudautu, vudaututhuhoi.ToEntityReference(), tongtiendautu,
                                        tram, cbnv, ngayduyet, pdngn, thuadat, 100000003, type, sophieu);

                                    if (curr > lst.Count - 1)
                                        throw new Exception("Phân bổ không hoàn lại thất bại. Vui lòng tạo thêm vụ đầu tư mới để phân bổ");
                                }
                            }
                        }
                        break;
                    case 100000003: // nt dau tu bo sung von
                        {
                            EntityReference ntdautubosungvon = (EntityReference)ct["new_danhgianangsuat"];

                            List<Entity> lstchitiet = RetrieveMultiRecord(service, "new_chitietnhiemthudautubosungvon",
                                new ColumnSet(new string[] { "new_denghikhl", "new_thuadat" }),
                                "new_nghiemthudautubosungvon", ntdautubosungvon.Id);

                            foreach (Entity chitiet in lstchitiet)
                            {
                                decimal sotien = ((Money)chitiet["new_denghikhl"]).Value;
                                EntityReference thuadat = (EntityReference)chitiet["new_thuadat"];
                                Entity thuadatcanhtac = GetThuadatcanhtacfromthuadat(thuadat, hdmia.ToEntityReference());

                                if (!thuadatcanhtac.Contains("new_chinhsachdautu"))
                                    throw new Exception(thuadatcanhtac["new_name"].ToString() + " không có chính sách đầu tư");

                                Entity CSDT = service.Retrieve("new_chinhsachdautu", ((EntityReference)thuadatcanhtac["new_chinhsachdautu"]).Id,
                        new ColumnSet(new string[] { "new_new_thoihanthuhoivon_khl", "new_machinhsach" }));

                                int sonamthuhoiKHL = CSDT.Contains("new_new_thoihanthuhoivon_khl") ? (int)CSDT["new_new_thoihanthuhoivon_khl"] : 0;
                                decimal sotienphanboKHL = 0;

                                if (sonamthuhoiKHL != 0)
                                    sotienphanboKHL = sotien / sonamthuhoiKHL;
                                else
                                    return;

                                List<Entity> lst = RetrieveVudautu().Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList<Entity>();
                                int curr = lst.FindIndex(p => p.Id == vudautu.Id);
                                traceService.Trace("c");
                                for (int k = 0; k < sonamthuhoiKHL; k++)
                                {
                                    Entity vudaututhuhoi = lst[++curr];
                                    traceService.Trace(vudaututhuhoi["new_mavudautu"].ToString());
                                    CreatePBDT_Mia(hdmia, KH, thuadatcanhtac.Id, vudautu, vudaututhuhoi.ToEntityReference(), sotien, tram,
                                        cbnv, ngayduyet, pdngn, thuadat, 100000001, type, sophieu);
                                    traceService.Trace("d");
                                    if (curr > lst.Count - 1)
                                        throw new Exception("Phân bổ không hoàn lại thất bại. Vui lòng tạo thêm vụ đầu tư mới để phân bổ");
                                }
                            }
                        }
                        break;
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
            traceService.Trace("Start get thua dat");
            QueryExpression q = new QueryExpression("new_thuadatcanhtac");
            q.ColumnSet = new ColumnSet(new string[] { "new_thuadatcanhtacid", "new_name", "new_chinhsachdautu" });
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

            for (int i = 0; i < n; i++)
            {
                Entity q = bls[i];

                DateTime dt = (DateTime)q["new_ngayapdung"];
                if (n == 1 && CompareDate(ngaygiaonhan, dt) == 0)
                {
                    result = (decimal)q["new_phantramlaisuat"];
                    break;
                }
                else if (n > 1 && CompareDate(ngaygiaonhan, dt) < 0)
                {
                    result = (decimal)bls[i - 1]["new_phantramlaisuat"];
                    break;
                }
                else if (i == n - 1)
                {
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
