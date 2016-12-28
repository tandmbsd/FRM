using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.Discovery;
using Microsoft.Xrm.Sdk.XmlNamespaces;
using Microsoft.Xrm.Client.Services;
using System.Configuration;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.WebServiceClient;
using System.ServiceModel;
using System.ServiceModel.Security;

namespace ConsoleAppTest
{
    class Program
    {
        static void Main(string[] args)
        {
            OrganizationService service;
            var connectionstring = GetWindowsIntegratedSecurityConnectionString();
            var serverConnection = new ServerConnection(connectionstring);

            using (service = new OrganizationService(serverConnection.CRMConnection))
            {
                Guid entityId = new Guid("8BCBB3AC-00E8-E511-93F6-9ABE942A7E29");//{472AA958-BFB5-E511-93F1-9ABE942A7E29} 24EB7CBB-B5B5-E511-93F1-9ABE942A7E29 

                Entity CSDT = service.Retrieve("new_chinhsachdautu", entityId, new ColumnSet(true));
                Entity newCSDT = new Entity("new_chinhsachdautu");

                //Thông tin chính sách
                EntityReference vudautuRef = CSDT.GetAttributeValue<EntityReference>("new_vudautu");
                Guid vuDTId = vudautuRef.Id;
                Entity vuDT = service.Retrieve("new_vudautu", vuDTId, new ColumnSet(new string[] { "new_name", "new_ngaybatdau" }));

                Entity newvuDT = FindvuDT(service, vuDT);
                if (newvuDT != null && newvuDT.Id != Guid.Empty)
                {
                    newCSDT["new_vudautu"] = newvuDT.ToEntityReference();

                    string newName = CSDT["new_name"].ToString() + " vụ " + newvuDT["new_name"].ToString();
                    newCSDT["new_name"] = newName;

                    int loaihd = ((OptionSetValue)CSDT["new_loaihopdong"]).Value;
                    newCSDT["new_loaihopdong"] = new OptionSetValue(loaihd);

                    int mucdichdt = ((OptionSetValue)CSDT["new_mucdichdautu"]).Value;
                    newCSDT["new_mucdichdautu"] = new OptionSetValue(mucdichdt);

                    DateTime ngayapdung = DateTime.Now;
                    newCSDT["new_ngayapdung"] = ngayapdung;

                    //newCSDT["statecode"] = 1;

                    // Chi tiết
                    if (CSDT.Attributes.Contains("new_vutrong_vl"))   // vu trong
                    {
                        string vutrong = CSDT["new_vutrong_vl"].ToString();
                        newCSDT.Attributes["new_vutrong_vl"] = vutrong;
                    }
                    if (CSDT.Attributes.Contains("new_mucdichsanxuatmia_vl"))   // muc dich sx mia
                    {
                        string mucdichsxmia = CSDT["new_mucdichsanxuatmia_vl"].ToString();
                        newCSDT.Attributes["new_mucdichsanxuatmia_vl"] = mucdichsxmia;
                    }
                    if (CSDT.Attributes.Contains("new_loaisohuudat_vl"))   // loai so huu dat
                    {
                        string loaishdat = CSDT["new_loaisohuudat_vl"].ToString();
                        newCSDT.Attributes["new_loaisohuudat_vl"] = loaishdat;
                    }
                    if (CSDT.Attributes.Contains("new_loaigocmia_vl"))   // loai goc mia
                    {
                        string loaigocmia = CSDT["new_loaigocmia_vl"].ToString();
                        newCSDT.Attributes["new_loaigocmia_vl"] = loaigocmia;
                    }
                    if (CSDT.Attributes.Contains("new_nhomgiongmia_vl"))   // nhom giong mia
                    {
                        string nhomgiongmia = CSDT["new_nhomgiongmia_vl"].ToString();
                        newCSDT.Attributes["new_nhomgiongmia_vl"] = nhomgiongmia;
                    }
                    if (CSDT.Attributes.Contains("new_nhomdat_vl"))   // nhom dat
                    {
                        string nhomdat = CSDT["new_nhomdat_vl"].ToString();
                        newCSDT.Attributes["new_nhomdat_vl"] = nhomdat;
                    }
                    if (CSDT.Attributes.Contains("new_nhomphanbon_vl"))   // nhom phan bon
                    {
                        string nhomphanbon = CSDT["new_nhomphanbon_vl"].ToString();
                        newCSDT.Attributes["new_nhomphanbon_vl"] = nhomphanbon;
                    }
                    if (CSDT.Attributes.Contains("new_mucdichtuoi_vl"))   // muc dich tuoi
                    {
                        string nhomphanbon = CSDT["new_mucdichtuoi_vl"].ToString();
                        newCSDT.Attributes["new_mucdichtuoi_vl"] = nhomphanbon;
                    }
                    if (CSDT.Attributes.Contains("new_phuongphaptuoi_vl"))   // phuong phap tuoi
                    {
                        string nhomphanbon = CSDT["new_phuongphaptuoi_vl"].ToString();
                        newCSDT.Attributes["new_phuongphaptuoi_vl"] = nhomphanbon;
                    }

                    // Cơ giới hóa

                    newCSDT["new_lamdat"] = (CSDT.Contains("new_lamdat") ? (bool)CSDT["new_lamdat"] : false);     // lam dat
                    newCSDT["new_bonphan"] = (bool)CSDT["new_bonphan"];   // bon phan
                    newCSDT["new_trongmia"] = (bool)CSDT["new_trongmia"]; // trong mia
                    newCSDT["new_tuoi"] = (bool)CSDT["new_tuoi"];         // tuoi  
                    newCSDT["new_thuhoach"] = (bool)CSDT["new_thuhoach"]; // thu hoach
                    newCSDT["new_chamsoc"] = (bool)CSDT["new_chamsoc"];   // cham soc

                    // Giá trị

                    if (CSDT.Attributes.Contains("new_dinhmucdautukhonghoanlai"))   // Dinh muc DT khong hoan lai
                    {
                        Money MdmDTkhongHL = (Money)CSDT["new_dinhmucdautukhonghoanlai"];
                        newCSDT["new_dinhmucdautukhonghoanlai"] = MdmDTkhongHL;
                    }
                    if (CSDT.Attributes.Contains("new_dinhmuctamung"))   // Dinh muc tam ung
                    {
                        newCSDT["new_dinhmuctamung"] = (decimal)CSDT["new_dinhmuctamung"];
                    }

                    newCSDT.Attributes["new_loailaisuatcodinhthaydoi"] = (bool)CSDT["new_loailaisuatcodinhthaydoi"];

                    if (CSDT.Attributes.Contains("new_muclaisuatdautu"))   // Muc lai suat
                    {
                        newCSDT["new_muclaisuatdautu"] = (decimal)CSDT["new_muclaisuatdautu"];
                    }

                    if (CSDT.Attributes.Contains("new_cachtinhlai"))   //Cach tinh lai
                    {
                        int cachtinhlai = ((OptionSetValue)CSDT["new_cachtinhlai"]).Value;
                        newCSDT["new_cachtinhlai"] = new OptionSetValue(cachtinhlai);
                    }
                    if (CSDT.Attributes.Contains("new_thoihanthuhoivon"))   // Thoi han thu hoi von
                    {
                        int thoihanTHV = (int)CSDT["new_thoihanthuhoivon"];
                        newCSDT["new_thoihanthuhoivon"] = thoihanTHV;
                    }
                    if (CSDT.Attributes.Contains("new_vubatdauthuhoi"))   // Vu bat dau thu hoi
                    {
                        newCSDT["new_vubatdauthuhoi"] = newvuDT.ToEntityReference();
                    }

                    if (CSDT.Attributes.Contains("new_dinhmucdautuhoanlai"))   // Dinh muc DT hoan lai
                    {
                        Money MdmDTHL = (Money)CSDT["new_dinhmucdautuhoanlai"];
                        newCSDT["new_dinhmucdautuhoanlai"] = MdmDTHL;
                    }

                    if (CSDT.Attributes.Contains("new_dinhmucphanbontoithieu"))   // Dinh muc phan bon toi thieu
                    {
                        Money MphanbonTT = (Money)CSDT["new_dinhmucphanbontoithieu"];
                        newCSDT["new_dinhmucphanbontoithieu"] = MphanbonTT;
                    }

                    Guid newCSDTID = service.Create(newCSDT);

                    // Nhóm khách hàng

                    EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", CSDT.Id);
                    EntityReferenceCollection listNhomKH = new EntityReferenceCollection();

                    if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                    {
                        foreach (Entity nhomKH in dsNhomKH.Entities)
                        {
                            listNhomKH.Add(nhomKH.ToEntityReference());
                        }

                        //service.Associate(CSDT.LogicalName, CSDT.Id, new Relationship(relationshipName), new EntityReferenceCollection() { nhomkhachhangRef });
                        service.Associate("new_chinhsachdautu", newCSDTID, new Relationship("new_new_chinhsachdautu_new_nhomkhachhang"), listNhomKH);
                    }

                    // Vùng địa lý
                    EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachdautu", "new_new_chinhsachdautu_new_vung", new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuid", CSDT.Id);
                    EntityReferenceCollection listVungDl = new EntityReferenceCollection();
                    if (dsVungDL != null && dsVungDL.Entities.Count > 0)
                    {
                        foreach (Entity vungDL in dsVungDL.Entities)
                        {
                            listVungDl.Add(vungDL.ToEntityReference());
                        }
                        service.Associate("new_chinhsachdautu", newCSDTID, new Relationship("new_new_chinhsachdautu_new_vung"), listVungDl);
                    }

                    // Nhóm năng suất
                    EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid" }), "new_chinhsachdautuid", CSDT.Id);
                    EntityReferenceCollection listNhomNS = new EntityReferenceCollection();
                    if (dsNhomNS != null && dsNhomNS.Entities.Count > 0)
                    {
                        foreach (Entity nhomNS in dsNhomNS.Entities)
                        {
                            listNhomNS.Add(nhomNS.ToEntityReference());
                        }
                        service.Associate("new_chinhsachdautu", newCSDTID, new Relationship("new_new_chinhsachdautu_new_nhomnangsuat"), listNhomNS);
                    }

                    // Giống mía
                    EntityCollection dsGiongmia = RetrieveNNRecord(service, "new_giongmia", "new_chinhsachdautu", "new_new_chinhsachdautu_new_giongmia", new ColumnSet(new string[] { "new_giongmiaid" }), "new_chinhsachdautuid", CSDT.Id);
                    EntityReferenceCollection listGiongmia = new EntityReferenceCollection();
                    if (dsGiongmia != null && dsGiongmia.Entities.Count > 0)
                    {
                        foreach (Entity giongmia in dsGiongmia.Entities)
                        {
                            listGiongmia.Add(giongmia.ToEntityReference());
                        }
                        service.Associate("new_chinhsachdautu", newCSDTID, new Relationship("new_new_chinhsachdautu_new_giongmia"), listGiongmia);
                    }

                    // Khuyến khích phát triển
                    EntityCollection dsKKPTCSDT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_chinhsachdautu", "new_new_chinhsachdautu_new_khuyenkhichphatt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_chinhsachdautuid", CSDT.Id);
                    EntityReferenceCollection listKKPT = new EntityReferenceCollection();
                    if (dsKKPTCSDT != null && dsKKPTCSDT.Entities.Count > 0)
                    {
                        foreach (Entity kkpt in dsKKPTCSDT.Entities)
                        {
                            listKKPT.Add(kkpt.ToEntityReference());
                        }
                        service.Associate("new_chinhsachdautu", newCSDTID, new Relationship("new_new_chinhsachdautu_new_khuyenkhichphattri"), listKKPT);
                    }

                    // Nhóm cự ly
                    EntityCollection dsNHomCL = RetrieveNNRecord(service, "new_nhomculy", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomculy", new ColumnSet(new string[] { "new_nhomculyid" }), "new_chinhsachdautuid", CSDT.Id);
                    EntityReferenceCollection listNhomcl = new EntityReferenceCollection();
                    if (dsNHomCL != null && dsNHomCL.Entities.Count > 0)
                    {
                        foreach (Entity nhomcl in dsNHomCL.Entities)
                        {
                            listNhomcl.Add(nhomcl.ToEntityReference());
                        }
                        service.Associate("new_chinhsachdautu", newCSDTID, new Relationship("new_new_chinhsachdautu_new_nhomculy"), listNhomcl);
                    }

                    // Mô hình khuyến nông
                    EntityCollection dsMHKN = RetrieveNNRecord(service, "new_mohinhkhuyennong", "new_chinhsachdautu", "new_new_chinhsachdautu_new_mohinhkhuyennong", new ColumnSet(new string[] { "new_mohinhkhuyennongid" }), "new_chinhsachdautuid", CSDT.Id);
                    EntityReferenceCollection listMHKN = new EntityReferenceCollection();
                    if (dsMHKN != null && dsMHKN.Entities.Count > 0)
                    {
                        foreach (Entity mhkn in dsMHKN.Entities)
                        {
                            listMHKN.Add(mhkn.ToEntityReference());
                        }
                        service.Associate("new_chinhsachdautu", newCSDTID, new Relationship("new_new_chinhsachdautu_new_mohinhkhuyennong"), listMHKN);
                    }

                    EntityReference csdtEntityRef = new EntityReference("new_chinhsachdautu", newCSDTID);

                    // Lần giải ngân
                    EntityCollection LangiainganCol = FindLanGiaiNgan(service, CSDT);
                    if (LangiainganCol != null && LangiainganCol.Entities.Count > 0)
                    {
                        foreach (Entity a in LangiainganCol.Entities)
                        {
                            Entity langiaingan = new Entity("new_dinhmucdautu");
                            EntityReference currencyRef = a.GetAttributeValue<EntityReference>("transactioncurrencyid");

                            string tenLGN = (a.Contains("new_name") ? (string)a["new_name"] : "");
                            int lanGN = (a.Contains("new_langiaingan") ? (int)a["new_langiaingan"] : 0);
                            decimal tyleGN = (a.Contains("new_phantramtilegiaingan") ? (decimal)a["new_phantramtilegiaingan"] : 0);
                            decimal sotienGN = (a.Contains("new_sotien") ? ((Money)a["new_sotien"]).Value : 0);
                            Money MsotienGN = new Money(sotienGN);


                            langiaingan["new_name"] = tenLGN;
                            langiaingan["new_langiaingan"] = lanGN;
                            langiaingan["new_chinhsachdautu"] = csdtEntityRef;
                            langiaingan["new_phantramtilegiaingan"] = tyleGN;
                            langiaingan["new_sotien"] = MsotienGN;
                            langiaingan["new_yeucauconghiemthu"] = (bool)a["new_yeucauconghiemthu"];
                            langiaingan["transactioncurrencyid"] = currencyRef;

                            service.Create(langiaingan);
                        }
                    }

                    //Ti le thu hoi von
                    EntityCollection TileTHVCol = FindTiLeTHV(service, CSDT);
                    if (TileTHVCol != null && TileTHVCol.Entities.Count > 0)
                    {
                        foreach (Entity a in TileTHVCol.Entities)
                        {
                            Entity tileTHV = new Entity("new_tilethuhoivon");
                            EntityReference currencyRef = a.GetAttributeValue<EntityReference>("transactioncurrencyid");

                            string tenTL = (a.Contains("new_name") ? (string)a["new_name"] : "");
                            int nam = (a.Contains("new_nam") ? (int)a["new_nam"] : 0);
                            decimal tyleTH = (a.Contains("new_phantramtilethuhoi") ? (decimal)a["new_phantramtilethuhoi"] : 0);
                            string diengiai = (a.Contains("new_diengiai") ? ((string)a["new_diengiai"]) : "");


                            tileTHV["new_name"] = tenTL;
                            tileTHV["new_nam"] = nam;
                            tileTHV["new_chinhsachdautu"] = csdtEntityRef;
                            tileTHV["new_phantramtilethuhoi"] = tyleTH;
                            tileTHV["new_diengiai"] = diengiai;
                            tileTHV["transactioncurrencyid"] = currencyRef;

                            service.Create(tileTHV);
                        }
                    }

                    //Bang lai suat thay doi
                    EntityCollection BangLSTDCol = FindBangLSTD(service, CSDT);
                    if (BangLSTDCol != null && BangLSTDCol.Entities.Count > 0)
                    {
                        foreach (Entity a in BangLSTDCol.Entities)
                        {
                            Entity bangLSTD = new Entity("new_banglaisuatthaydoi");

                            string ma = (a.Contains("new_ma") ? (string)a["new_ma"] : "");
                            string ten = (a.Contains("new_name") ? (string)a["new_name"] : "");
                            decimal phantram = (a.Contains("new_phantramlaisuat") ? (decimal)a["new_phantramlaisuat"] : 0);
                            DateTime apdung = DateTime.Now;

                            bangLSTD["new_ma"] = ma;
                            bangLSTD["new_name"] = ten;
                            bangLSTD["new_chinhsachdautu"] = csdtEntityRef;
                            bangLSTD["new_phantramlaisuat"] = phantram;
                            bangLSTD["new_ngayapdung"] = apdung;

                            service.Create(bangLSTD);
                        }
                    }

                    // Định mức MMTB cho chính sách MMTB
                    EntityCollection dmMMTBcol = FinddmMMTB(service, CSDT);
                    if (dmMMTBcol != null && dmMMTBcol.Entities.Count > 0)
                    {
                        foreach (Entity a in dmMMTBcol.Entities)
                        {
                            Entity dmMMTB = new Entity("new_dinhmucungvondautummtb");

                            string ten = (a.Contains("new_name") ? (string)a["new_name"] : "Định mức");
                            decimal phantram = (a.Contains("new_phantramgiatri") ? (decimal)a["new_phantramgiatri"] : 0);

                            if(a.Contains("new_phuongthuctinhtu"))
                            {
                                int pttu = ((OptionSetValue)a["new_phuongthuctinhtu"]).Value;
                                dmMMTB["new_phuongthuctinhtu"] = new OptionSetValue(pttu);
                            }
                            if (a.Contains("new_phuongthuctinhden"))
                            {
                                int ptden = ((OptionSetValue)a["new_phuongthuctinhden"]).Value;
                                dmMMTB["new_phuongthuctinhden"] = new OptionSetValue(ptden);
                            }

                            decimal giatritu = (a.Contains("new_giatritu") ? (decimal)a["new_giatritu"] : 0);
                            decimal giatriden = (a.Contains("new_giatriden") ? (decimal)a["new_giatriden"] : 0);

                            dmMMTB["new_name"] = ten;
                            dmMMTB["new_chinhsachdautu"] = csdtEntityRef;
                            dmMMTB["new_phantramgiatri"] = phantram;
                                  
                            dmMMTB["new_giatritu"] = giatritu;
                            dmMMTB["new_giatriden"] = giatriden;

                            service.Create(dmMMTB);
                        }
                    }

                } //if(newvuDT !=null && newvuDT.Id != Guid.Empty)
                else
                {
                    throw new InvalidPluginExecutionException("Chưa có vụ đầu tư mới");
                }

                //traceService.Trace("ID là " + newCSDTID.ToString());

            }//using
        }

        public static string GetWindowsIntegratedSecurityConnectionString()
        {
            int count = ConfigurationManager.ConnectionStrings.Count;
            if (count == 0)
                throw new Exception("Could not find ConnectionString");

            return ConfigurationManager.ConnectionStrings[0].ConnectionString;
        }

        public static Entity FindvuDT(IOrganizationService crmservices, Entity vudautu)
        {
            string fetchXml = @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                      <entity name='new_vudautu'>
                                        <attribute name='new_name' />
                                        <attribute name='new_mavudautu' />
                                        <attribute name='new_ngaybatdau' />
                                        <attribute name='new_ngayketthuc' />
                                        <attribute name='new_dactinh' />
                                        <attribute name='new_danghoatdong' />
                                        <attribute name='new_vudautuid' />
                                        <order attribute='new_ngaybatdau' descending='false' />
                                        <filter type='and'>
                                          <condition attribute='statecode' operator='eq' value='0' />
                                          <condition attribute='new_ngaybatdau' operator='on-or-after' value='{0}' />
                                        </filter>
                                      </entity>
                                    </fetch>";

            DateTime ngaybatdau = (DateTime)vudautu["new_ngaybatdau"];
            fetchXml = string.Format(fetchXml, ngaybatdau);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            if (entc.Entities.Count() >= 2)
            {
                return entc.Entities[1];
            }
            else
            {
                return null;
            }
        }

        public static EntityCollection FindLanGiaiNgan(IOrganizationService crmservices, Entity CSDT)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                       <entity name='new_dinhmucdautu'>
                            <attribute name='new_name' />
                            <attribute name='new_chinhsachdautu' />
                            <attribute name='new_sotien' />
                            <attribute name='new_langiaingan' />
                            <attribute name='new_phantramtilegiaingan' />
                            <attribute name='new_yeucauconghiemthu' />
                            <attribute name='transactioncurrencyid' />
                            <attribute name='createdon' />
                            <attribute name='new_dinhmucdautuid' />
                            <order attribute='createdon' descending='false' />
                                 <filter type='and'>
                                     <condition attribute='new_chinhsachdautu' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                                 </filter>
                         </entity>
                     </fetch>";
            fetchXml = string.Format(fetchXml, CSDT.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }

        public static EntityCollection FindTiLeTHV(IOrganizationService crmservices, Entity CSDT)
        {
            string fetchXml =
                            @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                  <entity name='new_tilethuhoivon'>
                                    <attribute name='new_name' />
                                    <attribute name='new_phantramtilethuhoi' />
                                    <attribute name='new_nam' />
                                    <attribute name='new_chinhsachdautu' />
                                    <attribute name='new_diengiai' />
                                    <attribute name='new_tilethuhoivonid' />
                                    <order attribute='new_nam' descending='false' />
                                    <filter type='and'>
                                      <condition attribute='new_chinhsachdautu' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                                    </filter>
                                  </entity>
                                </fetch>";
            fetchXml = string.Format(fetchXml, CSDT.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }

        public static EntityCollection FindBangLSTD(IOrganizationService crmservices, Entity CSDT)
        {
            string fetchXml =
                @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_banglaisuatthaydoi'>
                        <attribute name='new_name' />
                        <attribute name='createdon' />
                        <attribute name='new_ngayapdung' />
                        <attribute name='new_ma' />
                        <attribute name='new_phantramlaisuat' />
                        <attribute name='new_chinhsachdautu' />
                        <attribute name='new_banglaisuatthaydoiid' />
                        <order attribute='createdon' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_chinhsachdautu' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, CSDT.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }

        public static EntityCollection FinddmMMTB(IOrganizationService crmservices, Entity CSDT)
        {
            string fetchXml =
                @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_dinhmucungvondautummtb'>
                    <attribute name='new_name' />
                    <attribute name='createdon' />
                    <attribute name='new_phuongthuctinhtu' />
                    <attribute name='new_phuongthuctinhden' />
                    <attribute name='new_phantramgiatri' />
                    <attribute name='new_giatritu' />
                    <attribute name='new_giatriden' />
                    <attribute name='new_dinhmucungvondautummtbid' />
                    <order attribute='createdon' descending='true' />
                    <filter type='and'>
                      <condition attribute='new_chinhsachdautu' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, CSDT.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }
        public static EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
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
            EntityCollection collRecords = crmservices.RetrieveMultiple(query);

            return collRecords;
        }
    }
}


//StringBuilder xml = new StringBuilder();
// xml.AppendLine("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
// xml.AppendLine()
//stringA.AppendLine(stringB) nghia la stringA = stringA + stringB

// muốn check field có value không thì dùng service.Trace(entityA.Attributes.Contains("fieldxyz")); 
// sau đó dùng hàm Contains() để check coi nó có value ko
// hoặc dùng if (bien == null) Trace("A") else Trace("B");

//Logger.Write("Phonecall PostCreate", "Begin");
//throw new InvalidPluginExecutionException("End");
//Logger.Write("entity Id", entity.Id.ToString());

//Convert.ChangeType(mCSTM["new_dongiatang1ccs"]), decimal);
//giá trị mới = Convert.ChangeType(val, pd.PropertyType);
//service.Trace(entityA.Attributes.Contains("fieldxyz"));
//service.Trace("vi tri 1");