using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Messaging;

namespace PDNTamUng_ETLTranSaction
{
    public class PDNTamUng_ETLTranSaction : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        ITracingService trace = null;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];
            
            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000) // da duyet
            {                              
                Entity fullEntity = (Entity)context.PostEntityImages["PostImg"];
                
                string masohopdong = null;
                Entity HDMia = null;
                Entity Hdthuhoach = null;
                Entity Hdvanchuyen = null;
                Entity Hdmuabanmiangoai = null;
                
                trace.Trace("a");
                Entity Vudautu = service.Retrieve("new_vudautu", ((EntityReference)fullEntity["new_vudautu"]).Id, new ColumnSet(new string[] { "new_mavudautu" }));
                var listVuThuHoach = RetrieveMultiRecord(service, "new_vuthuhoach", new ColumnSet(true), "new_vudautu", ((EntityReference)fullEntity["new_vudautu"]).Id);
                Entity vuThuHoach = listVuThuHoach.Count > 0 ? listVuThuHoach[0] : null;
                string vuMua = "";
                if (vuThuHoach != null)
                {
                    vuMua = ((DateTime)vuThuHoach["new_tungay"]).ToString("yyyy") + "-" + ((DateTime)vuThuHoach["new_denngay"]).ToString("yyyy");
                }

                if (!fullEntity.Contains("new_loaihopdong"))
                    throw new Exception("Phiếu đề nghị tạm ứng không có loại hợp đồng");

                int loaihopdong = ((OptionSetValue)fullEntity["new_loaihopdong"]).Value;

                if (!fullEntity.Contains("new_vudautu"))
                    throw new Exception("Phiếu đề nghị tạm ứng không có vụ đầu tư");

                if (!fullEntity.Contains("new_ngayduyet"))
                    throw new Exception("Phiếu đề nghị tạm ứng không có ngày duyệt");

                if (loaihopdong == 100000000) // hd mia
                {
                    HDMia = service.Retrieve("new_hopdongdautumia", ((EntityReference)fullEntity["new_hopdongdautumia"]).Id,
                    new ColumnSet(new string[] { "new_masohopdong", "new_tram", "new_canbonongvu" }));
                    masohopdong = HDMia["new_masohopdong"].ToString();                    
                }
                else if (loaihopdong == 100000004) // hd van chuyen
                {
                    Hdvanchuyen = service.Retrieve("new_hopdongvanchuyen", ((EntityReference)fullEntity["new_hopdongvanchuyen"]).Id,
                    new ColumnSet(new string[] { "new_sohopdongvanchuyen", "new_tram", "new_canbonongvu" }));
                    masohopdong = Hdvanchuyen["new_sohopdongvanchuyen"].ToString();
                }
                else if (loaihopdong == 100000005) // hd thu hoach
                {
                    Hdthuhoach = service.Retrieve("new_hopdongthuhoach", ((EntityReference)fullEntity["new_hopdongthuhoach"]).Id,
                                        new ColumnSet(new string[] { "new_sohopdong", "new_tram", "new_canbonongvu" }));
                    masohopdong = Hdthuhoach["new_sohopdong"].ToString();
                }
                else if (loaihopdong == 100000006) // hd mua ban mia ngoai
                {
                    Hdmuabanmiangoai = service.Retrieve("new_hopdongmuabanmiangoai", ((EntityReference)fullEntity["new_hdmuabanmiangoai"]).Id,
                    new ColumnSet(new string[] { "new_masohopdong", "new_tram", "new_canbonongvu" }));

                    masohopdong = Hdmuabanmiangoai["new_masohopdong"].ToString();
                }

                DateTime ngayduyet = (DateTime)fullEntity["new_ngayduyet"];
                string sophieu = (string)fullEntity["new_masophieutamung"];
                trace.Trace("b");
                Entity KH = null;

                if (fullEntity.Contains("new_khachhang"))
                    KH = service.Retrieve("contact", ((EntityReference)fullEntity["new_khachhang"]).Id,
                        new ColumnSet(new string[] { "new_makhachhang", "new_socmnd", "new_phuongthucthanhtoan" }));
                else
                    KH = service.Retrieve("account", ((EntityReference)fullEntity["new_khachhangdoanhnghiep"]).Id,
                        new ColumnSet(new string[] { "new_makhachhang", "new_masothue", "new_phuongthucthanhtoan" }));
                trace.Trace("A");
                if (fullEntity.Contains("new_sotienung") && ((Money)fullEntity["new_sotienung"]).Value > 0)
                {
                    #region begin

                    Entity etl_ND = new Entity("new_etltransaction");
                    etl_ND["new_name"] = fullEntity["new_masophieutamung"].ToString() + "_PRE";
                    trace.Trace("B");
                    etl_ND["new_vouchernumber"] = "DTND";
                    etl_ND["new_transactiontype"] = "1.2.5.a";
                    etl_ND["new_customertype"] = new OptionSetValue(fullEntity.Contains("new_khachhang") ? 1 : 2); // ???
                    etl_ND["new_season"] = vuMua;//Vudautu["new_mavudautu"].ToString();
                    etl_ND["new_sochungtu"] = fullEntity["new_masophieutamung"].ToString();
                    etl_ND["new_contractnumber"] = masohopdong;
                    etl_ND["new_tradingpartner"] = (KH.LogicalName.ToLower().Trim() == "contact" ?
                        ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : ""))
                        :
                        ((KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "") + "_" + (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""))
                        );
                    etl_ND["new_suppliernumber"] = KH["new_makhachhang"].ToString();
                    etl_ND["new_suppliersite"] = "TAY NINH";
                    etl_ND["new_invoicedate"] = fullEntity["new_ngaylapphieu"];
                    etl_ND["new_descriptionheader"] = "Tạm ứng tiền mặt cho nông dân";
                    etl_ND["new_terms"] = "Tra Ngay";
                    etl_ND["new_taxtype"] = "";
                    trace.Trace("C");
                    decimal sotien = ((Money)fullEntity["new_sotienung"]).Value;
                    etl_ND["new_invoiceamount"] = new Money(sotien);
                    etl_ND["new_gldate"] = fullEntity["new_ngayduyet"];
                    etl_ND["new_invoicetype"] = "PRE";
                    etl_ND["new_paymenttype"] = "TM";
                    trace.Trace("D");
                    if (fullEntity.Contains("new_khachhang"))
                        etl_ND["new_khachhang"] = fullEntity["new_khachhang"];
                    else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                        etl_ND["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];
                    trace.Trace("E");
                    Guid etl_NDID = service.Create(etl_ND);

                    etl_ND["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                    etl_ND["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                    etl_ND["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                    etl_ND["new_descriptionlines"] = fullEntity["new_name"].ToString();
                    etl_ND["tran_type"] = "PRE";

                    Send(etl_ND);
                    CreatePBDT(target, KH, Vudautu.ToEntityReference(), loaihopdong, HDMia, Hdmuabanmiangoai, Hdthuhoach,
                        Hdvanchuyen, sotien, etl_NDID, ngayduyet, sophieu, masohopdong);
                    #endregion

                    if (fullEntity.Contains("new_phuongthucthanhtoan") && ((OptionSetValue)fullEntity["new_phuongthucthanhtoan"]).Value == 100000001)
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

                        Entity etl_entity = service.Retrieve("new_etltransaction", etl_NDID, new ColumnSet(new string[] { "new_name" }));
                        if (etl_entity != null && etl_entity.Contains("new_name"))
                        {
                            paytamung["new_name"] = (string)etl_entity["new_name"];
                        }

                        paytamung["new_supplierbankname"] = (taikhoanchinh == null ? "" : taikhoanchinh["new_sotaikhoan"]);
                        paytamung["new_bankcccountnum"] = "CTXL-VND-0";
                        paytamung["new_paymentamount"] = fullEntity["new_sotienung"];
                        paytamung["new_paymentdate"] = fullEntity["new_ngayduyet"];
                        paytamung["new_paymentdocumentname"] = "CANTRU_03";
                        paytamung["new_vouchernumber"] = "BN";
                        paytamung["new_cashflow"] = "00.00";
                        paytamung["new_paymentnum"] = 1;
                        paytamung["new_documentnum"] = fullEntity["new_masophieutamung"].ToString();

                        if (fullEntity.Contains("new_khachhang"))
                            paytamung["new_khachhang"] = fullEntity["new_khachhang"];
                        else if (fullEntity.Contains("new_khachhangdoanhnghiep"))
                            paytamung["new_khachhangdoanhnghiep"] = fullEntity["new_khachhangdoanhnghiep"];

                        paytamung.Id = service.Create(paytamung);

                        paytamung["new_makhachhang"] = KH.Contains("new_makhachhang") ? KH["new_makhachhang"].ToString() : "";
                        paytamung["name"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("fullname") ? KH["fullname"].ToString() : "") : (KH.Contains("name") ? KH["name"].ToString() : ""));
                        paytamung["new_socmnd"] = (KH.LogicalName.ToLower() == "contact" ? (KH.Contains("new_socmnd") ? KH["new_socmnd"].ToString() : "") : (KH.Contains("new_masothue") ? KH["new_masothue"].ToString() : ""));
                        paytamung["new_descriptionlines"] = fullEntity["new_name"].ToString();
                        paytamung["new_type"] = "TYPE1";

                        Send(paytamung);
                        #endregion
                    }
                }
            }
        }

        public void CreatePBDT(Entity target, Entity KH, EntityReference vudautu, int loaihd, Entity hddtmia,
            Entity muabanmiangoai, Entity hdthuhoach, Entity hdvanchuyen, decimal sotien, Guid etlID, DateTime ngayduyet, string sophieu, string masohopdong)
        {
            trace.Trace("Start");
            EntityReference tram = null;
            EntityReference cbnv = null;

            Entity pdntamung = service.Retrieve(target.LogicalName, target.Id,
                new ColumnSet(new string[] { "new_hopdongdautumia", "new_hopdongvanchuyen", "new_hdmuabanmiangoai", "new_hopdongthuhoach" }));

            if (loaihd == 100000000)
            {
                List<Entity> lstChitietphieutamung = RetrieveMultiRecord(service, "new_chitietphieudenghitamung",
                new ColumnSet(new string[] { "new_chitiethddtmia" }), "new_phieudenghitamung", pdntamung.Id);

                foreach (Entity en in lstChitietphieutamung)
                {
                    #region tao pbdt
                    if (!en.Contains("new_chitiethddtmia"))
                        throw new Exception("Chi tiết phiếu đề nghị tạm ứng không có chi tiết HĐĐT mía");
                    trace.Trace("a");
                    Entity thuadatcanhtac = service.Retrieve("new_thuadatcanhtac", ((EntityReference)en["new_chitiethddtmia"]).Id,
                        new ColumnSet(new string[] { "new_dachihoanlai_tienmat", "new_loailaisuat", "new_laisuat" }));

                    int loailaisuat = ((OptionSetValue)thuadatcanhtac["new_loailaisuat"]).Value;

                    if (loailaisuat == 100000000 && !thuadatcanhtac.Contains("new_laisuat"))
                        throw new Exception("Thửa đất canh tác không có lãi suất");
                    trace.Trace("b");
                    StringBuilder Name = new StringBuilder();
                    Name.Append("PBDT");

                    if (hddtmia.Contains("new_masohopdong"))
                        Name.Append("-" + masohopdong);

                    if (KH.Contains("fullname"))
                        Name.Append("-" + KH["fullname"]);

                    else if (KH.Contains("name"))
                        Name.Append("-" + KH["name"]);

                    Entity phanbodautuKHL = new Entity("new_phanbodautu");

                    phanbodautuKHL["new_name"] = Name.ToString();

                    if (KH.LogicalName == "contact")
                        phanbodautuKHL["new_khachhang"] = KH.ToEntityReference();
                    else if (KH.LogicalName == "account")
                        phanbodautuKHL["new_khachhangdoanhnghiep"] = KH.ToEntityReference();

                    trace.Trace("c");
                    phanbodautuKHL["new_etltransaction"] = new EntityReference("new_etltransaction", etlID);
                    phanbodautuKHL["new_loaidautu"] = new OptionSetValue(100000000); // credit
                    thuadatcanhtac["new_dachihoanlai_tienmat"] = new Money(sotien);
                    trace.Trace("d");
                    phanbodautuKHL["new_loaihopdong"] = new OptionSetValue(100000000);
                    phanbodautuKHL["new_hopdongdautumia"] = hddtmia.ToEntityReference();
                    phanbodautuKHL["new_thuacanhtac"] = thuadatcanhtac.ToEntityReference();
                    phanbodautuKHL["new_vudautu"] = vudautu;
                    phanbodautuKHL["new_vuthanhtoan"] = GetVuthanhtoan(vudautu);
                    phanbodautuKHL["new_sotien"] = new Money(sotien);
                    phanbodautuKHL["new_conlai"] = new Money(sotien);
                    phanbodautuKHL["new_tram"] = tram;
                    phanbodautuKHL["new_cbnv"] = cbnv;
                    phanbodautuKHL["new_ngayphatsinh"] = ngayduyet;
                    phanbodautuKHL["new_phieutamung"] = target.ToEntityReference();
                    phanbodautuKHL["new_loailaisuat"] = new OptionSetValue(loailaisuat);
                    phanbodautuKHL["new_mucdichdautu"] = new OptionSetValue(100000004);
                    phanbodautuKHL["new_sophieu"] = sophieu;
                    trace.Trace("e");
                    if (loailaisuat == 100000000)
                        phanbodautuKHL["new_laisuat"] = thuadatcanhtac["new_laisuat"];
                    else
                        phanbodautuKHL["new_laisuat"] = Getlaisuat(vudautu, 100000000, ngayduyet);
                    trace.Trace("f");
                    service.Create(phanbodautuKHL);
                    trace.Trace("Đã tạo pbdt");
                    #endregion
                }
            }
            else if (loaihd == 100000004)
            {
                trace.Trace("hd van chuyen");
                #region tao pbdt         

                StringBuilder Name = new StringBuilder();
                Name.Append("PBDT");

                Name.Append("-" + masohopdong);

                if (KH.Contains("fullname"))
                    Name.Append("-" + KH["fullname"]);

                else if (KH.Contains("name"))
                    Name.Append("-" + KH["name"]);
                trace.Trace("a");
                Entity phanbodautuKHL = new Entity("new_phanbodautu");

                phanbodautuKHL["new_name"] = Name.ToString();

                if (KH.LogicalName == "contact")
                    phanbodautuKHL["new_khachhang"] = KH.ToEntityReference();
                else if (KH.LogicalName == "account")
                    phanbodautuKHL["new_khachhangdoanhnghiep"] = KH.ToEntityReference();

                phanbodautuKHL["new_etltransaction"] = new EntityReference("new_etltransaction", etlID);
                phanbodautuKHL["new_loaidautu"] = new OptionSetValue(100000000); // credit           
                phanbodautuKHL["new_loaihopdong"] = new OptionSetValue(100000000);
                phanbodautuKHL["new_hopdongvanchuyen"] = hdvanchuyen.ToEntityReference();
                phanbodautuKHL["new_vudautu"] = vudautu;
                phanbodautuKHL["new_vuthanhtoan"] = GetVuthanhtoan(vudautu);
                phanbodautuKHL["new_sotien"] = new Money(sotien);
                phanbodautuKHL["new_conlai"] = new Money(sotien);
                phanbodautuKHL["new_tram"] = tram;
                phanbodautuKHL["new_cbnv"] = cbnv;
                phanbodautuKHL["new_ngayphatsinh"] = ngayduyet;
                phanbodautuKHL["new_phieutamung"] = target.ToEntityReference();
                phanbodautuKHL["new_mucdichdautu"] = new OptionSetValue(100000004);
                phanbodautuKHL["new_sophieu"] = sophieu;
                phanbodautuKHL["new_laisuat"] = Getlaisuat(vudautu, 100000000, ngayduyet);
                trace.Trace("f");
                service.Create(phanbodautuKHL);
                trace.Trace("Đã tạo pbdt");
                #endregion
            }
            else if (loaihd == 100000005)
            {
                #region tao pbdt         

                StringBuilder Name = new StringBuilder();
                Name.Append("PBDT");

                Name.Append("-" + masohopdong);

                if (KH.Contains("fullname"))
                    Name.Append("-" + KH["fullname"]);

                else if (KH.Contains("name"))
                    Name.Append("-" + KH["name"]);

                Entity phanbodautuKHL = new Entity("new_phanbodautu");

                phanbodautuKHL["new_name"] = Name.ToString();

                if (KH.LogicalName == "contact")
                    phanbodautuKHL["new_khachhang"] = KH.ToEntityReference();
                else if (KH.LogicalName == "account")
                    phanbodautuKHL["new_khachhangdoanhnghiep"] = KH.ToEntityReference();

                phanbodautuKHL["new_etltransaction"] = new EntityReference("new_etltransaction", etlID);
                phanbodautuKHL["new_loaidautu"] = new OptionSetValue(100000000); // credit           
                phanbodautuKHL["new_loaihopdong"] = new OptionSetValue(100000000);
                phanbodautuKHL["new_hopdongthuhoach"] = hdthuhoach.ToEntityReference();
                phanbodautuKHL["new_vudautu"] = vudautu;
                phanbodautuKHL["new_vuthanhtoan"] = GetVuthanhtoan(vudautu);
                phanbodautuKHL["new_sotien"] = new Money(sotien);
                phanbodautuKHL["new_conlai"] = new Money(sotien);
                phanbodautuKHL["new_tram"] = tram;
                phanbodautuKHL["new_cbnv"] = cbnv;
                phanbodautuKHL["new_ngayphatsinh"] = ngayduyet;
                phanbodautuKHL["new_phieutamung"] = target.ToEntityReference();
                phanbodautuKHL["new_mucdichdautu"] = new OptionSetValue(100000004);
                phanbodautuKHL["new_sophieu"] = sophieu;
                phanbodautuKHL["new_laisuat"] = Getlaisuat(vudautu, 100000000, ngayduyet);
                trace.Trace("f");
                service.Create(phanbodautuKHL);
                trace.Trace("Đã tạo pbdt");
                #endregion
            }
            else if (loaihd == 100000006)
            {
                #region tao pbdt         

                StringBuilder Name = new StringBuilder();
                Name.Append("PBDT");
                Name.Append("-" + masohopdong);

                if (KH.Contains("fullname"))
                    Name.Append("-" + KH["fullname"]);

                else if (KH.Contains("name"))
                    Name.Append("-" + KH["name"]);

                Entity phanbodautuKHL = new Entity("new_phanbodautu");

                phanbodautuKHL["new_name"] = Name.ToString();

                if (KH.LogicalName == "contact")
                    phanbodautuKHL["new_khachhang"] = KH.ToEntityReference();
                else if (KH.LogicalName == "account")
                    phanbodautuKHL["new_khachhangdoanhnghiep"] = KH.ToEntityReference();

                phanbodautuKHL["new_etltransaction"] = new EntityReference("new_etltransaction", etlID);
                phanbodautuKHL["new_loaidautu"] = new OptionSetValue(100000000); // credit           
                phanbodautuKHL["new_loaihopdong"] = new OptionSetValue(100000000);
                phanbodautuKHL["new_hopdongmuabanmiangoai"] = muabanmiangoai.ToEntityReference();
                phanbodautuKHL["new_vudautu"] = vudautu;
                phanbodautuKHL["new_vuthanhtoan"] = GetVuthanhtoan(vudautu);
                phanbodautuKHL["new_sotien"] = new Money(sotien);
                phanbodautuKHL["new_conlai"] = new Money(sotien);
                phanbodautuKHL["new_tram"] = tram;
                phanbodautuKHL["new_cbnv"] = cbnv;
                phanbodautuKHL["new_ngayphatsinh"] = ngayduyet;
                phanbodautuKHL["new_phieutamung"] = target.ToEntityReference();
                phanbodautuKHL["new_mucdichdautu"] = new OptionSetValue(100000004);
                phanbodautuKHL["new_sophieu"] = sophieu;
                phanbodautuKHL["new_laisuat"] = Getlaisuat(vudautu, 100000000, ngayduyet);
                trace.Trace("f");
                service.Create(phanbodautuKHL);
                trace.Trace("Đã tạo pbdt");
                #endregion
            }
        }

        private EntityReference GetVuthanhtoan(EntityReference vudautu)
        {
            QueryExpression q = new QueryExpression(vudautu.LogicalName);
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            EntityCollection a = service.RetrieveMultiple(q);

            List<Entity> lst = a.Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList<Entity>();
            int curr = lst.FindIndex(p => p.Id == vudautu.Id);

            return lst[curr + 1].ToEntityReference();
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
            if (tmp != null)
            {
                m.Body = Serialize(tmp);
                m.Label = "invo";
            }
            else
                m.Label = "brek";
            mq.Send(m);
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
            int n = bls.Entities.Count;

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
