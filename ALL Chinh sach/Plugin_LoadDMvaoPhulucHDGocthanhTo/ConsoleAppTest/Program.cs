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
                Guid entityId = new Guid("9C50A1F1-DDFB-E511-93F7-9ABE942A7E29");
            

                Entity PhulucHDGocthanhTo = service.Retrieve("new_phuluchopdong_gocsangto", entityId, new ColumnSet(new string[] { "new_phuluchopdong", "new_chitiethopdongdautumia", "new_chinhsachdautu", "createdon" }));
                DateTime ngaytao = DateTime.Now;
                if (PhulucHDGocthanhTo.Contains("createdon"))
                    ngaytao = PhulucHDGocthanhTo.GetAttributeValue<DateTime>("createdon");

                EntityReference ctHDDTmiaRef = PhulucHDGocthanhTo.GetAttributeValue<EntityReference>("new_chitiethopdongdautumia");
                Entity ChiTietHD = service.Retrieve("new_thuadatcanhtac", ctHDDTmiaRef.Id, new ColumnSet(new string[] { "new_vutrong", "new_loaigocmia", "new_mucdichsanxuatmia", "new_giongmia", "new_thuadat", "createdon", "new_hopdongdautumia", "new_khachhang", "new_khachhangdoanhnghiep", "new_thamgiamohinhkhuyennong", "new_dientichthucte", "new_tuoimia", "new_dientichhopdong", "new_dinhmucphanbontoithieu", "new_chinhsachdautu", "new_copytuhddtthuedat", "new_chinhsachdautu", "new_luugoc" }));

                if (ChiTietHD != null)
                {
                    EntityReference thuadatEntityRef = new EntityReference();
                    Guid thuadatId = new Guid();
                    Entity thuadatObj = new Entity();
                    if (ChiTietHD.Attributes.Contains("new_thuadat"))
                    {
                        thuadatEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_thuadat");
                        thuadatId = thuadatEntityRef.Id;
                        thuadatObj = service.Retrieve("new_thuadat", thuadatId, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy" }));
                    }
                    EntityReference giongmiaEntityRef = new EntityReference();
                    Guid giongmiaId = new Guid();
                    Entity giongmiaObj = new Entity();
                    if (ChiTietHD.Attributes.Contains("new_giongmia"))
                    {
                        giongmiaEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_giongmia");
                        giongmiaId = giongmiaEntityRef.Id;
                        giongmiaObj = service.Retrieve("new_giongmia", giongmiaId, new ColumnSet(new string[] { "new_nhomgiong", "new_name" }));
                    }

                    EntityReference HDDTmiaRef = new EntityReference();
                    Guid DHDTmiaId = new Guid();
                    Entity HDDTmia = new Entity();
                    if (ChiTietHD.Attributes.Contains("new_hopdongdautumia"))
                    {
                        HDDTmiaRef = ChiTietHD.GetAttributeValue<EntityReference>("new_hopdongdautumia");
                        DHDTmiaId = HDDTmiaRef.Id;
                        HDDTmia = service.Retrieve("new_hopdongdautumia", DHDTmiaId, new ColumnSet(new string[] { "new_vudautu", "new_chinhantienmat" }));
                    }

                    if (HDDTmia == null || DHDTmiaId == Guid.Empty || !HDDTmia.Attributes.Contains("new_vudautu"))
                    {
                        throw new InvalidPluginExecutionException("Trong chi tiết HĐĐT mía chưa có Vụ đầu tư !");
                    }
                    else
                    {
                        EntityReference vudautuRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");
                        Guid vuDTId = vudautuRef.Id;
                        Entity vuDT = service.Retrieve("new_vudautu", vuDTId, new ColumnSet(new string[] { "new_name" }));
                        EntityCollection listCSDTCol = FindCSDTtrongmia(service, ChiTietHD);
                        Entity mCSDT = null;

                        if (listCSDTCol != null && listCSDTCol.Entities.Count > 0)
                        {
                            foreach (Entity a in listCSDTCol.Entities)
                            {
                                if (a.Contains("new_vutrong_vl"))  // Vu trong
                                {
                                    if (ChiTietHD.Contains("new_vutrong"))
                                    {
                                        if (a["new_vutrong_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_vutrong"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_loaigocmia_vl"))  // Loai goc mia
                                {
                                    if (ChiTietHD.Contains("new_loaigocmia"))
                                    {
                                        if (a["new_loaigocmia_vl"].ToString().IndexOf("100000000") == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_mucdichsanxuatmia_vl"))  // Muc dich san xuat mia
                                {
                                    if (ChiTietHD.Contains("new_mucdichsanxuatmia"))
                                    {
                                        if (a["new_mucdichsanxuatmia_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_mucdichsanxuatmia"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_nhomdat_vl"))  // Nhom dat
                                {
                                    if (ChiTietHD.Attributes.Contains("new_thuadat") && thuadatObj.Attributes.Contains("new_nhomdat"))
                                    {
                                        if (a["new_nhomdat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_nhomdat"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_loaisohuudat_vl"))  // Loai chu so huu
                                {
                                    if (ChiTietHD.Attributes.Contains("new_loaisohuudat"))
                                    {
                                        if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_loaisohuudat"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_nhomgiongmia_vl"))  // Nhom giong mia
                                {
                                    if (ChiTietHD.Attributes.Contains("new_giongmia") && giongmiaObj.Attributes.Contains("new_nhomgiong"))
                                    {
                                        if (a["new_nhomgiongmia_vl"].ToString().IndexOf(((OptionSetValue)giongmiaObj["new_nhomgiong"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_luugoc"))  // Luu goc
                                {
                                    if (ChiTietHD.Attributes.Contains("new_luugoc"))
                                    {
                                        if (((OptionSetValue)a["new_luugoc"]).Value.ToString() != ((OptionSetValue)ChiTietHD["new_luugoc"]).Value.ToString())
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                // NHom khach hang
                                bool co = false;

                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                {
                                    EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                    Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang", "new_nangsuatbinhquan" }));

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

                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                {
                                    EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                    Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

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
                                EntityCollection dsKKPTHDCT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_khuyenkhichpt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_thuadatcanhtacid", ChiTietHD.Id);
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

                                if (ChiTietHD.Attributes.Contains("new_thamgiamohinhkhuyennong"))
                                {
                                    EntityReference mhknEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_thamgiamohinhkhuyennong");
                                    Guid mhknId = mhknEntityRef.Id;
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

                                EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                {
                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                    Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nangsuatbinhquan" }));

                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                    {
                                        decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
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
                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                        {
                                            co = true;
                                        }
                                    }
                                }
                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                {
                                    Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                    Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));

                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                    {
                                        decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
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
                        } // if (listCSDTCol != null && listCSDTCol.Entities.Count > 0)

                        if (mCSDT != null && mCSDT.Id != Guid.Empty)
                        {
                            // ------Gan vao Phụ lục Gốc sang Tơ
                            EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                            Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtRef.Id, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name", "new_loailaisuatcodinhthaydoi", "new_muclaisuatdautu", "new_cachtinhlai", "new_dinhmucdautukhonghoanlai", "new_dinhmucphanbontoithieu" }));

                            Entity en = new Entity(PhulucHDGocthanhTo.LogicalName);
                            en.Id = entityId;

                            en.Attributes.Add("new_chinhsachdautu", csdtRef);

                            decimal dongiabsKHL = 0;
                            decimal dongiabsHL = 0;
                            decimal dongiabsPB = 0;
                            decimal dongiabsTM = 0;

                            EntityCollection listCSDTBS = FindCSDTBS(service, vuDT, ngaytao);
                            if (listCSDTBS != null && listCSDTBS.Entities.Count > 0)
                            {
                                foreach (Entity csdtbs in listCSDTBS.Entities)
                                {
                                    // NHom khach hang
                                    bool phuhop = true;
                                    if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                    {
                                        if (csdtbs.Attributes.Contains("new_nhomkhachhang"))
                                        {
                                            EntityReference nhomkhCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_nhomkhachhang");
                                            Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                            Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang" }));

                                            if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                            {
                                                Guid nhomkhCSDTBSId = nhomkhCSDTBSRef.Id;
                                                Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;

                                                if (nhomkhId != nhomkhCSDTBSId)
                                                {
                                                    phuhop = false;
                                                }
                                            }
                                            else   //neu khong co NHomKH trong CTHD
                                            {
                                                phuhop = false;
                                            }
                                        }
                                    }

                                    if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                    {
                                        if (csdtbs.Attributes.Contains("new_nhomkhachhang"))
                                        {
                                            EntityReference nhomkhCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_nhomkhachhang");
                                            Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                            Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

                                            if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                            {
                                                Guid nhomkhCSDTBSId = nhomkhCSDTBSRef.Id;
                                                Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;

                                                if (nhomkhId != nhomkhCSDTBSId)
                                                {
                                                    phuhop = false;
                                                }
                                            }
                                            else   //neu khong co NHomKH trong CTHD
                                            {
                                                phuhop = false;
                                            }
                                        }
                                    }

                                    if (phuhop == false)
                                        continue;

                                    // Giong mia

                                    phuhop = true;
                                    if (csdtbs.Attributes.Contains("new_giongmia"))
                                    {
                                        EntityReference giongmiaCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_giongmia");
                                        if (giongmiaEntityRef != null && giongmiaEntityRef.Id != Guid.Empty)
                                        {
                                            Guid giongmiaCSDTBSId = giongmiaCSDTBSRef.Id;

                                            if (giongmiaId != giongmiaCSDTBSId)
                                            {
                                                phuhop = false;
                                            }
                                        }
                                        else   //neu khong co Giongmia trong CTHD
                                        {
                                            phuhop = false;
                                        }

                                    }
                                    if (phuhop == false)
                                        continue;

                                    // NHom nang suat

                                    phuhop = true;
                                    if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                    {
                                        if (csdtbs.Attributes.Contains("new_nhomnangsuat"))
                                        {
                                            EntityReference nhomnangsuatCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_nhomnangsuat");
                                            Guid nhomnangsuatCSDTBSRefId = nhomnangsuatCSDTBSRef.Id;
                                            Entity nhomnangsuatCSDTBS = service.Retrieve("new_nhomnangsuat", nhomnangsuatCSDTBSRefId, new ColumnSet(new string[] { "new_nangsuattu", "new_nangsuatden" }));

                                            Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                            Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang", "new_nangsuatbinhquan" }));

                                            if (khObj.Attributes.Contains("new_nangsuatbinhquan") && nhomnangsuatCSDTBS.Attributes.Contains("new_nangsuattu") && nhomnangsuatCSDTBS.Attributes.Contains("new_nangsuatden"))
                                            {
                                                decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                                decimal nangsuattu = nhomnangsuatCSDTBS.GetAttributeValue<decimal>("new_nangsuattu");
                                                decimal nangsuatden = nhomnangsuatCSDTBS.GetAttributeValue<decimal>("new_nangsuatden");

                                                if (!((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden)))
                                                {
                                                    phuhop = false;
                                                }
                                            }
                                            else
                                            {
                                                phuhop = false;
                                            }
                                        }
                                    }

                                    if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                    {
                                        if (csdtbs.Attributes.Contains("new_nhomnangsuat"))
                                        {
                                            EntityReference nhomnangsuatCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_nhomnangsuat");
                                            Guid nhomnangsuatCSDTBSRefId = nhomnangsuatCSDTBSRef.Id;
                                            Entity nhomnangsuatCSDTBS = service.Retrieve("new_nhomnangsuat", nhomnangsuatCSDTBSRefId, new ColumnSet(new string[] { "new_nangsuattu", "new_nangsuatden" }));

                                            Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                            Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));

                                            if (khObj.Attributes.Contains("new_nangsuatbinhquan") && nhomnangsuatCSDTBS.Attributes.Contains("new_nangsuattu") && nhomnangsuatCSDTBS.Attributes.Contains("new_nangsuatden"))
                                            {
                                                decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                                decimal nangsuattu = nhomnangsuatCSDTBS.GetAttributeValue<decimal>("new_nangsuattu");
                                                decimal nangsuatden = nhomnangsuatCSDTBS.GetAttributeValue<decimal>("new_nangsuatden");

                                                if (!((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden)))
                                                {
                                                    phuhop = false;
                                                }
                                            }
                                            else
                                            {
                                                phuhop = false;
                                            }
                                        }
                                    }

                                    if (phuhop == false)
                                        continue;

                                    // Khuyen khich phat trien

                                    phuhop = true;
                                    EntityCollection dsKKPTHDCT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_khuyenkhichpt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_thuadatcanhtacid", ChiTietHD.Id);

                                    if (csdtbs.Attributes.Contains("new_khuyenkhichphattrien"))
                                    {
                                        EntityReference kkptCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_khuyenkhichphattrien");
                                        if (dsKKPTHDCT.Entities.Count > 0)
                                        {
                                            foreach (Entity kkptHDCT in dsKKPTHDCT.Entities)
                                            {
                                                if (kkptHDCT.Id != kkptCSDTBSRef.Id)
                                                {
                                                    phuhop = false;
                                                    break;
                                                }
                                            }
                                        }
                                        else
                                        {
                                            phuhop = false;
                                        }
                                    }

                                    if (phuhop == false)
                                        continue;

                                    // Mo hinh khuyen nong

                                    phuhop = true;

                                    if (csdtbs.Attributes.Contains("new_mohinhkhuyennong"))
                                    {
                                        if (ChiTietHD.Attributes.Contains("new_thamgiamohinhkhuyennong"))
                                        {
                                            EntityReference mhknEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_thamgiamohinhkhuyennong");
                                            EntityReference mhknCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_mohinhkhuyennong");

                                            if (mhknCSDTBSRef.Id != mhknEntityRef.Id)
                                                phuhop = false;
                                        }
                                        else
                                        {
                                            phuhop = false;
                                        }

                                    }
                                    if (phuhop == false)
                                        continue;

                                    // Nhom cu ly

                                    phuhop = true;

                                    if (csdtbs.Attributes.Contains("new_nhomculy"))
                                    {
                                        if (thuadatObj.Attributes.Contains("new_nhomculy"))
                                        {
                                            EntityReference nhomclEntityRef = thuadatObj.GetAttributeValue<EntityReference>("new_nhomculy");
                                            EntityReference nhomclCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_nhomculy");
                                            if (nhomclEntityRef.Id != nhomclCSDTBSRef.Id)
                                            {
                                                phuhop = false;
                                            }
                                        }
                                        else
                                        {
                                            phuhop = false;
                                        }
                                    }
                                    if (phuhop == false)
                                        continue;

                                    //Vung dia ly

                                    phuhop = true;

                                    if (csdtbs.Attributes.Contains("new_vungdialy"))
                                    {
                                        if (thuadatObj.Attributes.Contains("new_vungdialy"))
                                        {
                                            EntityReference vungdlEntityRef = thuadatObj.GetAttributeValue<EntityReference>("new_vungdialy");
                                            EntityReference vungdlCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_vungdialy");
                                            if (vungdlEntityRef.Id != vungdlCSDTBSRef.Id)
                                            {
                                                phuhop = false;
                                            }
                                        }
                                        else
                                        {
                                            phuhop = false;
                                        }
                                    }
                                    if (phuhop == false)
                                        continue;
                                    //break;

                                    dongiabsKHL += (csdtbs.Contains("new_sotienbosung_khl") ? csdtbs.GetAttributeValue<Money>("new_sotienbosung_khl").Value : 0);
                                    dongiabsHL += (csdtbs.Contains("new_sotienbosung") ? csdtbs.GetAttributeValue<Money>("new_sotienbosung").Value : 0);
                                    dongiabsPB += (csdtbs.Contains("new_bosungphanbon") ? csdtbs.GetAttributeValue<Money>("new_bosungphanbon").Value : 0);
                                    dongiabsTM += (csdtbs.Contains("new_bosungtienmat") ? csdtbs.GetAttributeValue<Money>("new_bosungtienmat").Value : 0);

                                } // foreach(Entity csdtbs in listCSDTBS.Entities)     
                            }
                            // ----------------- DINH MUC KHONG HOAN LAI

                            decimal dientichhd = (ChiTietHD.Contains("new_dientichhopdong") ? (decimal)ChiTietHD["new_dientichhopdong"] : 0);
                            decimal dongiaDTKHL = (csdtKQEntity.Contains("new_dinhmucdautukhonghoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautukhonghoanlai").Value : 0);
                            decimal dongiaDTHL = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value : 0);
                            decimal dongiaPhanbon = (csdtKQEntity.Contains("new_dinhmucphanbontoithieu") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucphanbontoithieu").Value : 0);

                            dongiaDTKHL += dongiabsKHL;
                            Money MdongiaDTKHL = new Money(dongiaDTKHL);

                            decimal dinhmucDTKHL = dongiaDTKHL * dientichhd;
                            Money MdinhmucDTKHL = new Money(dinhmucDTKHL);

                            //traceService.Trace("truoc cap nhat " + tongDMDTKHL);

                            en.Attributes.Add("new_dongiadautukhonghoanlai", MdongiaDTKHL);
                            en.Attributes.Add("new_dinhmucdautukhonghoanlai", MdinhmucDTKHL);

                            // ----------END------- DINH MUC KHONG HOAN LAI

                            // ----------------- DINH MUC DAU TU HOAN LAI

                            dongiaDTHL = dongiaDTHL - dongiaPhanbon + dongiabsHL + dongiabsPB + dongiabsTM;

                            decimal dinhmucDTHL = dongiaDTHL * dientichhd;

                            Money MdongiaDT = new Money(dongiaDTHL);
                            Money MdinhmucDT = new Money(dinhmucDTHL);

                            en.Attributes.Add("new_dongiadautuhoanlai", MdongiaDT);
                            en.Attributes.Add("new_dinhmucdautuhoanlai", MdinhmucDT);

                            // -------------------- DINH MUC PHAN BON

                            decimal tongDMPB = 0;
                            if (csdtKQEntity.Attributes.Contains("new_dinhmucphanbontoithieu"))   // don gia phan bon toi thieu
                            {
                                if (!HDDTmia.Contains("new_chinhantienmat") || (HDDTmia.Contains("new_chinhantienmat") && (bool)HDDTmia["new_chinhantienmat"] == false))
                                {
                                    Money MdongiaPhanbon = new Money(dongiaPhanbon);
                                    tongDMPB = dongiaPhanbon * dientichhd;
                                    Money MtongDMDTKHL = new Money(tongDMPB);

                                    en.Attributes.Add("new_dongiaphanbontoithieu", MdongiaPhanbon);
                                    en.Attributes.Add("new_dinhmucphanbontoithieu", MtongDMDTKHL);
                                }
                            }
                            // --------END--------- DINH MUC PHAN BON

                            // -------------------- DINH MUC DAU TU
                            decimal tongDM = dinhmucDTHL + dinhmucDTKHL + tongDMPB;
                            Money MtongDM = new Money(tongDM);

                            en.Attributes.Add("new_dinhmucdautu", MtongDM);

                            // --------END--------- DINH MUC DAU TU

                            service.Update(en);
                        }
                    }
                } // if(ctHDDTmia != null)
            } // using
        }
        public static string GetWindowsIntegratedSecurityConnectionString()
        {
            int count = ConfigurationManager.ConnectionStrings.Count;
            if (count == 0)
                throw new Exception("Could not find ConnectionString");

            return ConfigurationManager.ConnectionStrings[0].ConnectionString;
        }
        public static EntityCollection FindCSDTtrongmia(IOrganizationService crmservices, Entity ctHDDT)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctHDDT["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctHDDT["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000000));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
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

        public static EntityCollection FindCSDTBS(IOrganizationService crmservices, Entity Vudt, DateTime ngaytao)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                          <entity name='new_chinhsachdautuchitiet'>
                                            <attribute name='new_chinhsachdautuchitietid' />
                                            <attribute name='new_name' />
                                            <attribute name='createdon' />
                                            <attribute name='new_nhomkhachhang' />
                                            <attribute name='new_giongmia' />
                                            <attribute name='new_nhomnangsuat' />
                                            <attribute name='new_khuyenkhichphattrien' />
                                            <attribute name='new_mohinhkhuyennong' />
                                            <attribute name='new_nhomculy' />
                                            <attribute name='new_vungdialy' />
                                            <attribute name='new_sotienbosung' />
                                            <attribute name='new_sotienbosung_khl' />
                                            <attribute name='new_bosungphanbon' />
                                            <attribute name='new_bosungtienmat' />
                                            <order attribute='new_name' descending='false' />
                                            <filter type='and'>
                                              <condition attribute='statecode' operator='eq' value='0' />
                                              <condition attribute='new_vudautu' operator='eq' value='{0}' />
                                              <condition attribute='new_tungay' operator='on-or-after' value='{1}' />
                                              <condition attribute='new_denngay' operator='on-or-before' value='{2}' />
                                            </filter>
                                          </entity>
                                        </fetch>";
            fetchXml = string.Format(fetchXml, Vudt.Id, ngaytao, ngaytao);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
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