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
using System.ServiceModel.Description;


namespace HDThueDatConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            //OrganizationService service;
            //var connectionstring = GetWindowsIntegratedSecurityConnectionString();
            //var serverConnection = new ServerConnection(connectionstring);

            var credentials = new ClientCredentials();
            credentials.UserName.UserName = @"dev2\crmservices";
            credentials.UserName.Password = @"P@ssword";

            Uri OrganizationUri = new Uri("http://10.33.1.93/TEST/XRMServices/2011/Organization.svc");
            Uri HomeRealmUri = null;

            using (OrganizationServiceProxy serviceProxy = new OrganizationServiceProxy(OrganizationUri, HomeRealmUri, credentials, null))
            //using (service = new OrganizationService(serverConnection.CRMConnection))
            {
                IOrganizationService service = (IOrganizationService)serviceProxy;

                Guid entityId = new Guid("91870CC8-4635-E611-93EF-98BE942A7E2D");

                Entity ChiTietHDThueDatThuaDat = service.Retrieve("new_chitiethdthuedat_thuadat", entityId, new ColumnSet(new string[] { "createdon", "new_chitiethdthuedat", "new_thuadat", "new_dinhmuc", "new_chinhsachdautu", "new_sonamthuedat", "new_dientichthucthue" }));

                DateTime ngaytao = ChiTietHDThueDatThuaDat.GetAttributeValue<DateTime>("createdon");
                //if (!ChiTietHDThueDatThuaDat.Contains("new_chinhsachdautu"))
                //{
                if (!ChiTietHDThueDatThuaDat.Contains("new_chitiethdthuedat"))
                {
                    throw new InvalidPluginExecutionException("Thiếu thông tin bắt buộc về thửa đất / vụ đầu tư");
                }
                else
                {

                    EntityReference thuadatEntityRef = ChiTietHDThueDatThuaDat.GetAttributeValue<EntityReference>("new_thuadat");
                    Guid thuadatId = thuadatEntityRef.Id;
                    Entity thuadatObj = service.Retrieve("new_thuadat", thuadatId, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy", "new_diachi" }));

                    EntityReference ctHDDTThuedatRef = ChiTietHDThueDatThuaDat.GetAttributeValue<EntityReference>("new_chitiethdthuedat");
                    Guid ctDHDTThuedatId = ctHDDTThuedatRef.Id;
                    Entity ChiTietHDThueDat = service.Retrieve("new_datthue", ctDHDTThuedatId, new ColumnSet(new string[] { "new_hopdongthuedat", "new_benchothuedatkh", "new_benchothuedatkhdn" }));

                    EntityReference HDDTThuedatRef = ChiTietHDThueDat.GetAttributeValue<EntityReference>("new_hopdongthuedat");
                    Guid DHDTThuedatId = HDDTThuedatRef.Id;
                    Entity HDDTThuedat = service.Retrieve("new_hopdongthuedat", DHDTThuedatId, new ColumnSet(new string[] { "new_vudautu", "new_khachhang", "new_khachhangdoanhnghiep" }));

                    EntityReference vudautuRef = HDDTThuedat.GetAttributeValue<EntityReference>("new_vudautu");
                    Guid vuDTId = vudautuRef.Id;

                    string fetchXml =
                              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                      <entity name='new_chinhsachdautu'>
                                        <attribute name='new_name' />
                                        <attribute name='new_vudautu' />
                                        <attribute name='new_ngayapdung' />
                                        <attribute name='new_mucdichdautu' />
                                        <attribute name='new_loaihopdong' />
                                        <attribute name='new_nhomdat_vl' />
                                        <attribute name='new_dinhmucdautukhonghoanlai' />
                                        <attribute name='new_dinhmucdautuhoanlai' />
                                        <attribute name='new_sonamthue' />
                                        <attribute name='new_chinhsachdautuid' />
                                        <order attribute='new_ngayapdung' descending='true' />
                                        <filter type='and'>
                                          <condition attribute='new_loaihopdong' operator='eq' value='100000001' />
                                          <condition attribute='new_mucdichdautu' operator='eq' value='100000005' />
                                          <condition attribute='new_ngayapdung' operator='on-or-before' value='{0}' />
                                          <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{1}' />                                        
                                        </filter>
                                      </entity>
                                    </fetch>";

                    fetchXml = string.Format(fetchXml, ngaytao, vuDTId);
                    EntityCollection result = service.RetrieveMultiple(new FetchExpression(fetchXml));

                    Entity mCSDT = new Entity();
                    Entity en = new Entity(ChiTietHDThueDatThuaDat.LogicalName);
                    en.Id = ChiTietHDThueDatThuaDat.Id;

                    if (result != null && result.Entities.Count > 0)
                    {
                        foreach (Entity a in result.Entities)
                        {
                           // traceService.Trace("Nhom dat ngoai if");
                            if (a.Contains("new_nhomdat_vl"))  // Nhom dat
                            {
                               // traceService.Trace("Nhom dat trong if");
                                //traceService.Trace(a["new_nhomdat_vl"].ToString());
                                //traceService.Trace(((OptionSetValue)thuadatObj["new_nhomdat"]).Value.ToString());
                                if (thuadatObj.Attributes.Contains("new_nhomdat"))
                                {
                                    if (a["new_nhomdat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_nhomdat"]).Value.ToString()) == -1)
                                        continue;
                                }
                                else
                                    continue;
                            }
                            //if (a.Contains("new_loaisohuudat_vl"))  // Loai chu so huu
                            //{
                            //    if (thuadatObj.Attributes.Contains("new_loaisohuudat"))
                            //    {
                            //        if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_loaisohuudat"]).Value.ToString()) == -1)
                            //            continue;
                            //    }
                            //    else
                            //    {
                            //        continue;
                            //    }
                            //}

                            if (a.Contains("new_sonamthue"))  // So nam thue
                            {
                                if (ChiTietHDThueDatThuaDat.Contains("new_sonamthuedat"))
                                {
                                    int sonamthueCSDT = (int)a["new_sonamthue"];
                                    int sonamthueCTHDTD = (int)ChiTietHDThueDatThuaDat["new_sonamthuedat"];
                                    if (sonamthueCSDT != sonamthueCTHDTD)
                                        continue;
                                }
                                else
                                    continue;
                            }

                            // NHom khach hang
                            bool co = false;

                            if (HDDTThuedat.Attributes.Contains("new_khachhang"))
                            {
                                Guid khId = HDDTThuedat.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang", "new_nangsuatbinhquan" }));

                                EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                {
                                    EntityReference nhomkhEntityRef = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang");
                                    Guid nhomkhId = nhomkhEntityRef.Id;
                                    Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                    if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                    {
                                        foreach (Entity nhomKH in dsNhomKH.Entities)
                                        {
                                            if (nhomKHHDCT.Id == nhomKH.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        co = true;
                                    }
                                }
                                else   //neu khong co NHomKH trong CTHD
                                {
                                    if (dsNhomKH == null || dsNhomKH.Entities.Count == 0)
                                    {
                                        co = true;
                                    }
                                }
                            }

                            if (HDDTThuedat.Attributes.Contains("new_khachhangdoanhnghiep"))
                            {
                                Guid khId = HDDTThuedat.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

                                EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                {
                                    Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;
                                    Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang", nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                    if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                    {
                                        foreach (Entity nhomKH in dsNhomKH.Entities)
                                        {
                                            if (nhomKHHDCT.Id == nhomKH.Id)
                                            {
                                                co = true;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        co = true;
                                    }
                                }
                                else   //neu khong co NHomKH trong CTHD
                                {
                                    if (dsNhomKH == null || dsNhomKH.Entities.Count == 0)
                                    {
                                        co = true;
                                    }
                                }
                            }

                            if (co == false)
                                continue;
                           // traceService.Trace("DK nhom KH");

                            //Vung dia ly
                            EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachdautu", "new_new_chinhsachdautu_new_vung", new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuid", a.Id);

                            if (dsVungDL.Entities.Count > 0)
                            {
                                co = false;

                                List<Guid> dsvung = new List<Guid>();
                                foreach (Entity n in dsVungDL.Entities)
                                    dsvung.Add(n.Id);
                                if (thuadatObj.Attributes.Contains("new_diachi"))
                                {
                                    Entity diachi = service.Retrieve("new_diachi", ((EntityReference)thuadatObj["new_diachi"]).Id, new ColumnSet(new string[] { "new_path" }));

                                    QueryExpression qe = new QueryExpression("new_vungdialy_hanhchinh");
                                    qe.ColumnSet = new ColumnSet(new string[] { "new_vungdialy_hanhchinhid", "new_vungdialy", "new_path" });
                                    qe.Criteria.AddCondition(new ConditionExpression("new_vungdialy", ConditionOperator.In, dsvung.ToArray()));

                                    foreach (Entity n in service.RetrieveMultiple(qe).Entities)
                                    {
                                        if (diachi["new_path"].ToString().Contains(n["new_path"].ToString()))
                                        {
                                            co = true;
                                            break;
                                        }
                                    }
                                }
                                if (co == false)
                                    continue;
                            }

                            // Nhom cu ly
                            co = false;

                            EntityCollection dsNHomCL = RetrieveNNRecord(service, "new_nhomculy", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomculy", new ColumnSet(new string[] { "new_nhomculyid" }), "new_chinhsachdautuid", a.Id);
                            if (thuadatObj.Attributes.Contains("new_nhomculy"))
                            {
                                EntityReference nhomclEntityRef = thuadatObj.GetAttributeValue<EntityReference>("new_nhomculy");
                                Guid nhomclId = nhomclEntityRef.Id;
                                Entity nhomCL = service.Retrieve("new_nhomculy", nhomclId, new ColumnSet(new string[] { "new_name" }));

                                if (dsNHomCL != null && dsNHomCL.Entities.Count > 0)
                                {
                                    foreach (Entity nhomCL1 in dsNHomCL.Entities)
                                    {
                                        if (nhomCL.Id == nhomCL1.Id)
                                        {
                                            co = true;
                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    co = true;
                                }
                            }
                            else   //neu khong co NHomCL trong CTHD
                            {

                                if (dsNHomCL == null || dsNHomCL.Entities.Count == 0)
                                {
                                    co = true;
                                }
                            }
                            if (co == false)
                                continue;

                            // NHom nang suat
                            co = false;

                            if (HDDTThuedat.Attributes.Contains("new_khachhang"))
                            {
                                EntityReference khEntityRef = HDDTThuedat.GetAttributeValue<EntityReference>("new_khachhang");
                                Guid khId = khEntityRef.Id;
                                Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nangsuatbinhquan" }));

                                EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

                                if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                {
                                    decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                    if (dsNhomNS != null && dsNhomNS.Entities.Count > 0)
                                    {
                                        foreach (Entity mhkn1 in dsNhomNS.Entities)
                                        {
                                            if (mhkn1.Attributes.Contains("new_nangsuattu") && mhkn1.Attributes.Contains("new_nangsuatden"))
                                            {
                                                decimal nangsuattu = mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                decimal nangsuatden = mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                if ((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden))
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    else
                                    {
                                        co = true;
                                    }
                                }
                                else
                                {
                                    if (dsNhomNS == null || dsNhomNS.Entities.Count == 0)
                                    {
                                        co = true;
                                    }
                                }
                            }
                            if (HDDTThuedat.Attributes.Contains("new_khachhangdoanhnghiep"))
                            {
                                Guid khId = HDDTThuedat.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));

                                EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

                                if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                {
                                    decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                    if (dsNhomNS != null && dsNhomNS.Entities.Count > 0)
                                    {
                                        foreach (Entity mhkn1 in dsNhomNS.Entities)
                                        {
                                            if (mhkn1.Attributes.Contains("new_nangsuattu") && mhkn1.Attributes.Contains("new_nangsuatden"))
                                            {
                                                decimal nangsuattu = mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                decimal nangsuatden = mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                if ((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden))
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    else
                                    {
                                        co = true;
                                    }
                                }
                                else
                                {
                                    if (dsNhomNS == null || dsNhomNS.Entities.Count == 0)
                                    {
                                        co = true;
                                    }
                                }
                            }

                            if (co == false)
                                continue;
                           // traceService.Trace("DK nhom KH");

                            mCSDT = a;
                            break;
                        }
                    }
                    if (mCSDT != null && mCSDT.Id != Guid.Empty)
                    {
                        //traceService.Trace("Tim duoc CSDT");
                        // ------Gan vao Chi tiet HDDT mia
                        EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                        en.Attributes.Add("new_chinhsachdautu", csdtRef);

                        EntityCollection oldlTLTHVDK = FindTLTHVDK(service, ChiTietHDThueDatThuaDat);
                        if (oldlTLTHVDK != null && oldlTLTHVDK.Entities.Count > 0)
                        {
                            foreach (Entity a in oldlTLTHVDK.Entities)
                            {
                                service.Delete("new_tylethuhoivondukien", a.Id);
                            }
                        }

                        // -------Gan ty le thu hoi von du kien
                        // Lay nhung tylethuhoivon trong chinh sach dau tu
                        string fetchTLTHV =
                        @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                          <entity name='new_tilethuhoivon'>
                                               <attribute name='new_name' />
                                               <attribute name='new_phantramtilethuhoi' />
                                               <attribute name='new_nam' />
                                               <attribute name='new_chinhsachdautu' />
                                               <attribute name='new_tilethuhoivonid' />
                                               <order attribute='new_nam' descending='false' />
                                               <link-entity name='new_chinhsachdautu' from='new_chinhsachdautuid' to='new_chinhsachdautu' alias='ac'>
                                                   <filter type='and'>
                                                         <condition attribute='new_chinhsachdautuid' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                                                   </filter>
                                               </link-entity>
                                          </entity>
                                     </fetch>";

                        Guid csdtKQ = mCSDT.Id;

                        fetchTLTHV = string.Format(fetchTLTHV, csdtKQ);
                        EntityCollection collTLTHV = service.RetrieveMultiple(new FetchExpression(fetchTLTHV));

                        Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name", "new_loailaisuatcodinhthaydoi", "new_muclaisuatdautu", "new_cachtinhlai", "new_dinhmucdautukhonghoanlai" }));

                        foreach (Entity TLTHV in collTLTHV.Entities)
                        {
                            Entity tlthvdkHDCT = new Entity("new_tylethuhoivondukien");

                            EntityReference vudautuEntityRef = HDDTThuedat.GetAttributeValue<EntityReference>("new_vudautu");
                            EntityReference cthdtdEntityRef = new EntityReference("new_chitiethdthuedat_thuadat", entityId);

                            if (TLTHV.Attributes.Contains("new_phantramtilethuhoi") && TLTHV.Attributes.Contains("new_nam") && csdtKQEntity.Attributes.Contains("new_dinhmucdautuhoanlai"))
                            {
                                string tenTLTHVDK = "Năm " + TLTHV.GetAttributeValue<int>("new_nam").ToString();

                                decimal tyle = (TLTHV.Contains("new_phantramtilethuhoi") ? (decimal)TLTHV["new_phantramtilethuhoi"] : 0);
                                decimal dientichtt = (ChiTietHDThueDatThuaDat.Contains("new_dientichthucthue") ? (decimal)ChiTietHDThueDatThuaDat["new_dientichthucthue"] : 0);
                                decimal dinhmucDThl = 0 + csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value;
                                decimal sotien = 0;

                                sotien = (dinhmucDThl * dientichtt * tyle) / 100;

                                Money sotienM = new Money(sotien);

                                tlthvdkHDCT.Attributes.Add("new_name", tenTLTHVDK);
                                tlthvdkHDCT.Attributes.Add("new_loaityle", new OptionSetValue(100000001));
                                tlthvdkHDCT.Attributes.Add("new_chitiethdthuedat_thuadat", cthdtdEntityRef);
                                tlthvdkHDCT.Attributes.Add("new_vudautu", vudautuEntityRef);
                                tlthvdkHDCT.Attributes.Add("new_tylephantram", tyle);
                                tlthvdkHDCT.Attributes.Add("new_sotienthuhoi", sotienM);

                                service.Create(tlthvdkHDCT);
                            }
                        }
                        // ------End Gan vao ty le thu hoi von du kien

                        // Lay thong so vu dau tu
                        string fetchTSVDT =
                           @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                    <entity name='new_thongsotheovudautu'>
                                    <attribute name='new_name' />
                                    <attribute name='createdon' />
                                    <attribute name='new_vudautu' />
                                    <attribute name='new_loai' />
                                    <attribute name='new_giatri' />
                                    <attribute name='new_giatien' />
                                    <attribute name='new_apdungtu' />
                                    <attribute name='new_thongsotheovudautuid' />
                                    <order attribute='new_apdungtu' descending='true' />
                                    <filter type='and'>
                                           <condition attribute='new_loai' operator='eq' value='100000001' />
                                           <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{0}' />
                                    </filter>
                                    </entity>
                                </fetch>";

                        fetchTSVDT = string.Format(fetchTSVDT, vuDTId);
                        EntityCollection collTSVDT = service.RetrieveMultiple(new FetchExpression(fetchTSVDT));
                        Entity TSVDT = collTSVDT.Entities[0];

                        // ------ Gan NHom du lieu Lai suat

                        if (collTSVDT.Entities.Count > 0)
                        {
                            if (csdtKQEntity.Attributes.Contains("new_loailaisuatcodinhthaydoi"))
                            {
                                bool loails = csdtKQEntity.GetAttributeValue<bool>("new_loailaisuatcodinhthaydoi");

                                // Loai lai suat
                                if (loails == false) // ls thay doi
                                    en["new_loailaisuat"] = new OptionSetValue(100000001);

                                else   // ls co dinh
                                    en["new_loailaisuat"] = new OptionSetValue(100000000);
                            }

                            // Muc lai suat
                            if (csdtKQEntity.Attributes.Contains("new_loailaisuatcodinhthaydoi"))
                            {
                                bool loails = csdtKQEntity.GetAttributeValue<bool>("new_loailaisuatcodinhthaydoi");
                                decimal mucls = 0;
                                if (loails == false)   // ls thay doi
                                {
                                    mucls = 0 + csdtKQEntity.GetAttributeValue<decimal>("new_muclaisuatdautu");
                                    en["new_laisuat"] = mucls;
                                }
                                else // ls co dinh
                                {
                                    if (TSVDT.GetAttributeValue<OptionSetValue>("new_loai").Value == 100000001) //100,000,001 : Loai ls
                                    {
                                        mucls = (TSVDT.Contains("new_giatri") ? TSVDT.GetAttributeValue<decimal>("new_giatri") : 0);
                                        en["new_laisuat"] = mucls;
                                    }
                                }
                            }

                            // Cach tinh lai
                            if (csdtKQEntity.Attributes.Contains("new_cachtinhlai"))
                            {
                                OptionSetValue cachlinhlai = csdtKQEntity.GetAttributeValue<OptionSetValue>("new_cachtinhlai");
                                en["new_cachtinhlai"] = cachlinhlai;
                            }

                            //------ End nhom du lieu Gan Lai suat

                            // -------- Gan nhom du lieu Dinh muc
                            if (csdtKQEntity.Attributes.Contains("new_dinhmucdautuhoanlai"))
                            {
                                Money dinhmucDT = csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai");
                                en["new_dinhmuc"] = dinhmucDT;
                            }
                            // -------- End nhom du lieu Dinh muc
                        }
                        service.Update(en);
                        //Logger.Write("PostCreate - Update Lead/Prospect: Success"));  

                    } //if (mCSDT != null && mCSDT.Id != Guid.Empty)
                    else
                    {
                        throw new InvalidPluginExecutionException("Không tìm thấy Chính sách Đầu tư thuê đất phù hợp");
                    }
                } //if (vudautuRef == null && vudautuRef.Id == Guid.Empty)
                //}//

            }//using
        }
        static EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
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

        public static EntityCollection FindTLTHVDK(IOrganizationService crmservices, Entity chitietHDthuedat)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_tylethuhoivondukien'>
                        <attribute name='new_name' />
                        <attribute name='new_vudautu' />
                        <attribute name='new_tylephantram' />
                        <attribute name='new_sotienthuhoi' />
                        <attribute name='new_loaityle' />
                        <attribute name='new_chitiethddttrangthietbi' />
                        <attribute name='new_chitiethddtthuedat' />
                        <attribute name='new_chitiethddtmia' />
                        <attribute name='new_tylethuhoivondukienid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_chitiethddtthuedat' operator='eq' uitype='new_datthue' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, chitietHDthuedat.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static string GetWindowsIntegratedSecurityConnectionString()
        {
            int count = ConfigurationManager.ConnectionStrings.Count;
            if (count == 0)
                throw new Exception("Could not find ConnectionString");

            return ConfigurationManager.ConnectionStrings[0].ConnectionString;
        }
    }
}
