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


namespace TrangThietBiConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            //OrganizationService service;
            //var connectionstring = GetWindowsIntegratedSecurityConnectionString();
            //var serverConnection = new ServerConnection(connectionstring);


            var credentials = new ClientCredentials();
            credentials.UserName.UserName = @"dev1\crmservices";
            credentials.UserName.Password = @"P@ssword";

            Uri OrganizationUri = new Uri("http://10.33.1.92/TTCS/XRMServices/2011/Organization.svc");
            Uri HomeRealmUri = null;

            //using (service = new OrganizationService(serverConnection.CRMConnection))
            using (OrganizationServiceProxy serviceProxy = new OrganizationServiceProxy(OrganizationUri, HomeRealmUri, credentials, null))
            {
                IOrganizationService service = (IOrganizationService)serviceProxy;

                Guid entityId = new Guid("5D31F6D2-DF2E-E611-80C5-9ABE942A7CB1"); //47273D08-4CB3-E511-93EF-9ABE942A7E29
                Entity NTThamcanh = service.Retrieve("new_danhgianangsuat", entityId, new ColumnSet(new string[] { "subject" }));

                NTThamcanh = service.Retrieve("new_danhgianangsuat", entityId, new ColumnSet(new string[] { "subject", "new_vudautu", "new_hopdongdautumia", "new_thuadatcanhtac", "new_loaidanhgia", "new_dinhmuc", "createdon", "new_dientich", "new_dukienno" }));
                DateTime ngaytao = NTThamcanh.GetAttributeValue<DateTime>("createdon");

                //string loaidanhgia = new OptionSetValue(100000001).Value.ToString(); 

                if (NTThamcanh.Attributes.Contains("new_loaidanhgia") && NTThamcanh.GetAttributeValue<OptionSetValue>("new_loaidanhgia").Value.ToString() == "100000001")  // Loai danh gia: Tham canh la 100000001
                {
                    EntityReference ctHDDTmiaRef = NTThamcanh.GetAttributeValue<EntityReference>("new_thuadatcanhtac");
                    Guid ctHDDTmiaId = ctHDDTmiaRef.Id;
                    Entity ctHDDTmia = service.Retrieve("new_thuadatcanhtac", ctHDDTmiaId, new ColumnSet(new string[] { "new_vutrong", "new_loaigocmia", "new_mucdichsanxuatmia", "new_giongmia", "new_thuadat", "createdon", "new_hopdongdautumia", "new_khachhang", "new_khachhangdoanhnghiep", "new_thamgiamohinhkhuyennong", "new_dientichthucte" }));

                    EntityReference thuadatEntityRef = ctHDDTmia.GetAttributeValue<EntityReference>("new_thuadat");
                    EntityReference giongmiaEntityRef = ctHDDTmia.GetAttributeValue<EntityReference>("new_giongmia");
                    EntityReference vudautuRef = NTThamcanh.GetAttributeValue<EntityReference>("new_vudautu");

                    if (thuadatEntityRef == null || thuadatEntityRef.Id == Guid.Empty || giongmiaEntityRef == null || giongmiaEntityRef.Id == Guid.Empty || vudautuRef == null || vudautuRef.Id == Guid.Empty)
                    {
                        throw new InvalidPluginExecutionException("Thiếu thông tin bắt buộc về thửa đất / giống mía / vụ đầu tư");
                    }
                    else
                    {
                        Guid thuadatId = thuadatEntityRef.Id;
                        Entity thuadatObj = service.Retrieve("new_thuadat", thuadatId, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy" }));

                        Guid giongmiaId = giongmiaEntityRef.Id;
                        Entity giongmiaObj = service.Retrieve("new_giongmia", giongmiaId, new ColumnSet(new string[] { "new_nhomgiong", "new_name" }));

                        Guid vuDTId = vudautuRef.Id;

                        string fetchXml =
                                  @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                          <entity name='new_chinhsachdautu'>
                                            <attribute name='new_name' />
                                            <attribute name='new_vudautu' />
                                            <attribute name='new_ngayapdung' />
                                            <attribute name='new_mucdichdautu' />
                                            <attribute name='new_loaihopdong' />
                                            <attribute name='new_dinhmucdautukhonghoanlai' />
                                            <attribute name='new_dinhmucdautuhoanlai' />
                                            <attribute name='new_loaigocmia_vl' />
                                            <attribute name='new_nhomdat_vl' />
                                            <attribute name='new_vutrong_vl' />
                                            <attribute name='new_mucdichsanxuatmia_vl' />
                                            <attribute name='new_nhomgiongmia_vl' />
                                            <attribute name='new_loaisohuudat_vl' />
                                            <attribute name='new_chinhsachdautuid' />
                                            <order attribute='new_ngayapdung' descending='true' />
                                            <filter type='and'>
                                              <condition attribute='statecode' operator='eq' value='0' />
                                              <condition attribute='new_loaihopdong' operator='eq' value='100000000' />
                                              <condition attribute='new_mucdichdautu' operator='eq' value='100000001' />
                                              <condition attribute='new_ngayapdung' operator='on-or-before' value='{0}' />
                                          <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{1}' />                                       
                                        </filter>
                                      </entity>
                                    </fetch>";

                        fetchXml = string.Format(fetchXml, ngaytao, vuDTId);
                        EntityCollection result = service.RetrieveMultiple(new FetchExpression(fetchXml));
                        List<Entity> CSDT = result.Entities.ToList<Entity>();

                        Entity mCSDT = null;

                        if (CSDT != null)
                        {
                            foreach (Entity a in CSDT)
                            {
                                if (a.Contains("new_vutrong_vl"))  // Vu trong
                                {
                                    if (NTThamcanh.Contains("new_vutrong"))
                                    {
                                        if (a["new_vutrong_vl"].ToString().IndexOf(((OptionSetValue)NTThamcanh["new_vutrong"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_loaigocmia_vl"))  // Loai goc mia
                                {
                                    if (NTThamcanh.Contains("new_loaigocmia"))
                                    {
                                        if (a["new_loaigocmia_vl"].ToString().IndexOf(((OptionSetValue)NTThamcanh["new_loaigocmia"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_mucdichsanxuatmia_vl"))  // Muc dich san xuat mia
                                {
                                    if (NTThamcanh.Contains("new_mucdichsanxuatmia"))
                                    {
                                        if (a["new_mucdichsanxuatmia_vl"].ToString().IndexOf(((OptionSetValue)NTThamcanh["new_mucdichsanxuatmia"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_nhomdat_vl"))  // Nhom dat
                                {
                                    if (thuadatObj.Attributes.Contains("new_nhomdat"))
                                    {
                                        if (a["new_nhomdat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_nhomdat"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                    {
                                        continue;
                                    }
                                }

                                if (a.Contains("new_loaisohuudat_vl"))  // Loai chu so huu
                                {
                                    if (thuadatObj.Attributes.Contains("new_loaisohuudat"))
                                    {
                                        if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_loaisohuudat"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                    {
                                        continue;
                                    }
                                }

                                if (a.Contains("new_nhomgiongmia_vl"))  // Nhom giong mia
                                {
                                    if (giongmiaObj.Attributes.Contains("new_nhomgiong"))
                                    {
                                        if (a["new_nhomgiongmia_vl"].ToString().IndexOf(((OptionSetValue)giongmiaObj["new_nhomgiong"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                    {
                                        continue;
                                    }
                                }

                                //if (a.Contains("new_vudautu"))  // Vu dau tu
                                //{

                                //    EntityReference vudautuRefa = a.GetAttributeValue<EntityReference>("new_vudautu");
                                //    Guid vuDTaId = vudautuRefa.Id;
                                //    if (vuDTaId != vuDTId)
                                //        continue;
                                //}

                                // Nhom khach hang
                                bool co = false;
                                if (ctHDDTmia.Attributes.Contains("new_khachhang"))
                                {
                                    Guid khId = ctHDDTmia.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                    Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang", "new_nangsuatbinhquan" }));

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
                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                        {
                                            co = true;
                                        }
                                    }
                                }
                                if (ctHDDTmia.Attributes.Contains("new_khachhangdoanhnghiep"))
                                {
                                    Guid khId = ctHDDTmia.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
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
                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                        {
                                            co = true;
                                        }
                                    }
                                }

                                if (co == false)
                                    continue;

                                //Vung dia ly
                                co = false;

                                EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachdautu", "new_new_chinhsachdautu_new_vung", new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuid", a.Id);

                                if (thuadatObj.Attributes.Contains("new_vungdialy"))
                                {
                                    Guid vungdlId = thuadatObj.GetAttributeValue<EntityReference>("new_vungdialy").Id;
                                    Entity vungDL = service.Retrieve("new_vung", vungdlId, new ColumnSet(new string[] { "new_name" }));

                                    if (dsVungDL != null && dsVungDL.Entities.Count > 0)
                                    {
                                        foreach (Entity vungDL1 in dsVungDL.Entities)
                                        {
                                            if (vungDL.Id == vungDL1.Id)
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
                                else   //neu khong co VungDiaLy trong CTHD
                                {
                                    if (dsVungDL == null || dsVungDL.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                                if (co == false)
                                    continue;

                                // Giong mia
                                co = false;
                                EntityCollection dsGiongmia = RetrieveNNRecord(service, "new_giongmia", "new_chinhsachdautu", "new_new_chinhsachdautu_new_giongmia", new ColumnSet(new string[] { "new_giongmiaid" }), "new_chinhsachdautuid", a.Id);
                                if (dsGiongmia != null && dsGiongmia.Entities.Count > 0)
                                {
                                    foreach (Entity giongmia in dsGiongmia.Entities)
                                    {
                                        if (giongmiaObj.Id == giongmia.Id)
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
                                if (co == false)
                                    continue;

                                // Khuyen khich phat trien
                                co = false;
                                EntityCollection dsKKPTHDCT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_khuyenkhichpt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_thuadatcanhtacid", ctHDDTmia.Id);
                                EntityCollection dsKKPTCSDT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_chinhsachdautu", "new_new_chinhsachdautu_new_khuyenkhichphatt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_chinhsachdautuid", a.Id);

                                if (dsKKPTHDCT != null && dsKKPTHDCT.Entities.Count > 0)
                                {
                                    if (dsKKPTCSDT != null && dsKKPTCSDT.Entities.Count > 0)
                                    {
                                        foreach (Entity kkpt1 in dsKKPTHDCT.Entities)
                                        {
                                            foreach (Entity kkpt2 in dsKKPTCSDT.Entities)
                                            {
                                                //neu tim thay kkpt1 nam trong danh sach dsKKPTCSDT thi thoat khoi for
                                                if (kkpt1.Id == kkpt2.Id)
                                                {
                                                    co = true;
                                                    break;
                                                }
                                            }
                                            if (co)
                                            {
                                                //thoat vong for thu 1
                                                break;
                                            }

                                        }
                                    }
                                    else
                                    {
                                        co = true;
                                    }
                                }
                                else   //neu khong co KKPT trong CTHD
                                {
                                    if (dsKKPTCSDT == null || dsKKPTCSDT.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                                if (co == false)
                                    continue;

                                // Nhom cu ly
                                co = false;

                                EntityCollection dsNHomCL = RetrieveNNRecord(service, "new_nhomculy", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomculy", new ColumnSet(new string[] { "new_nhomculyid" }), "new_chinhsachdautuid", a.Id);
                                if (thuadatObj.Attributes.Contains("new_nhomculy"))
                                {
                                    Guid nhomclId = thuadatObj.GetAttributeValue<EntityReference>("new_nhomculy").Id;
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
                                    if (dsNHomCL == null || dsNHomCL.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                                if (co == false)
                                    continue;

                                // Mo hinh khuyen nong
                                co = false;

                                EntityCollection dsMHKN = RetrieveNNRecord(service, "new_mohinhkhuyennong", "new_chinhsachdautu", "new_new_chinhsachdautu_new_mohinhkhuyennong", new ColumnSet(new string[] { "new_mohinhkhuyennongid" }), "new_chinhsachdautuid", a.Id);

                                if (ctHDDTmia.Attributes.Contains("new_thamgiamohinhkhuyennong"))
                                {
                                    Guid mhknId = ctHDDTmia.GetAttributeValue<EntityReference>("new_thamgiamohinhkhuyennong").Id;
                                    Entity mhkn = service.Retrieve("new_mohinhkhuyennong", mhknId, new ColumnSet(new string[] { "new_name" }));

                                    if (dsMHKN != null && dsMHKN.Entities.Count() > 0)
                                    {
                                        foreach (Entity mhkn1 in dsMHKN.Entities)
                                        {
                                            if (mhkn.Id == mhkn1.Id)
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
                                else   //neu khong co MNKH trong CTHD
                                {
                                    if (dsMHKN == null || dsMHKN.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                                if (co == false)
                                    continue;

                                // NHom nang suat
                                co = false;
                                if (ctHDDTmia.Attributes.Contains("new_khachhang"))
                                {
                                    Guid khId = ctHDDTmia.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                    Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nangsuatbinhquan" }));
                                    EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                    {
                                        decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                        {
                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
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
                                        else
                                        {
                                            co = true;
                                        }
                                    }
                                    else
                                    {
                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                        {
                                            co = true;
                                        }
                                    }
                                }
                                if (ctHDDTmia.Attributes.Contains("new_khachhangdoanhnghiep"))
                                {
                                    Guid khId = ctHDDTmia.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                    Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));
                                    EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                    {
                                        decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                        {
                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
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
                                        else
                                        {
                                            co = true;
                                        }
                                    }
                                    else
                                    {
                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                        {
                                            co = true;
                                        }
                                    }
                                }

                                if (co == false)
                                    continue;

                                mCSDT = a;
                                break;
                            }
                        }
                        if (mCSDT != null && mCSDT.Id != Guid.Empty)
                        {
                            // ------Gan vao Chi tiet HDDT mia
                            EntityReference csdtRef = new EntityReference("new_danhgianangsuat", mCSDT.Id);
                            NTThamcanh.Attributes.Add("new_danhgianangsuat", csdtRef);

                            Guid csdtKQ = mCSDT.Id;
                            Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name" }));

                            // -------- Gan nhom du lieu  Dinh muc

                            if (csdtKQEntity.Attributes.Contains("new_dinhmucdautuhoanlai"))
                            {
                                Money dinhmucDT = csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai");
                                if (NTThamcanh.Attributes.Contains("new_dinhmuc"))  // Dau tu  hoan lai
                                {
                                    NTThamcanh.Attributes["new_dinhmuc"] = dinhmucDT;
                                }
                                else
                                {
                                    NTThamcanh.Attributes.Add("new_dinhmuc", dinhmucDT);
                                }

                                // -------- Gan du kien no
                                decimal dinhmucdt = dinhmucDT.Value;
                                decimal dientichdanhgia = NTThamcanh.GetAttributeValue<decimal>("new_dientich");
                                decimal dukienno = dinhmucdt * dientichdanhgia;
                                Money dukiennoM = new Money(dukienno);

                                if (NTThamcanh.Attributes.Contains("new_dukienno"))  // Dau tu  hoan lai
                                {
                                    NTThamcanh.Attributes["new_dukienno"] = dukiennoM;
                                }
                                else
                                {
                                    NTThamcanh.Attributes.Add("new_dukienno", dukiennoM);
                                }
                                // -------- End Gan du kien no
                            }
                            // -------- End nhom du lieu  Gan Dinh muc



                            service.Update(NTThamcanh);

                        }  //if (mCSDT != null && mCSDT.Id != Guid.Empty)
                        //else
                        //{
                        //    throw new InvalidPluginExecutionException("Không tìm thấy Chính sách Đầu tư thâm canh phù hợp");
                        //}
                    }
                } //if(NTThamcanh.GetAttributeValue<OptionSetValue>("new_loaidanhgia") == loaidanhgia)
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

        public static string GetWindowsIntegratedSecurityConnectionString()
        {
            int count = ConfigurationManager.ConnectionStrings.Count;
            if (count == 0)
                throw new Exception("Could not find ConnectionString");

            return ConfigurationManager.ConnectionStrings[0].ConnectionString;
        }
    }
}
