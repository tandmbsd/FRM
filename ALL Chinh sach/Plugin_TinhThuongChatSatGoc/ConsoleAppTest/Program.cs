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
                Guid entityId = new Guid("B32D832E-1BDA-E511-93F1-9ABE942A7E29"); //  7b25569F14-5DC6-E511-93F1-9ABE942A7E29

                Entity NghiemthusauTh = service.Retrieve("new_nghiemthuchatsatgoc", entityId, new ColumnSet(new string[] { "subject", "new_khachhang", "new_khachhangdoanhnghiep", "new_tinhtrangduyet", "statuscode", "new_hopdongdautumia", "new_ngaynghiemthu" }));
                DateTime ngaytao = NghiemthusauTh.GetAttributeValue<DateTime>("createdon");

                EntityReference HDDTmiaRef = NghiemthusauTh.GetAttributeValue<EntityReference>("new_hopdongdautumia");
                Entity HDDTmia = service.Retrieve("new_hopdongdautumia", HDDTmiaRef.Id, new ColumnSet(new string[] { "new_vudautu" }));

                if (NghiemthusauTh.GetAttributeValue<OptionSetValue>("new_tinhtrangduyet").Value.ToString() == "100000006") // Da duyet : new_tinhtrangduyet 100,000,006  -- // statuscode : 100,000,000
                {
                    // Cập nhật chặt sát gốc == true
                    EntityCollection chitietHDDTmiaCol = FindchitietHDDTmia(service, HDDTmia);
                    foreach (Entity chitietHDDTmia in chitietHDDTmiaCol.Entities)
                    {
                        chitietHDDTmia.Attributes["new_chatsatgoc"] = true;
                        service.Update(chitietHDDTmia);
                    }

                    // Tạo PDN thưởng chặt sát gốc

                    EntityCollection chitietNTsauTHCol = FindchitietNTsauTH(service, NghiemthusauTh);

                    foreach (Entity chitietNT in chitietNTsauTHCol.Entities)
                    {
                        EntityReference thuadatRef = chitietNT.GetAttributeValue<EntityReference>("new_thuadat");
                        Entity thuadat = service.Retrieve("new_thuadat", thuadatRef.Id, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy" }));
                        Entity chitietHDKQ = null;
                        foreach (Entity chitietHDDTmia in chitietHDDTmiaCol.Entities)
                        {
                            Entity ThuaDatCTHD = service.Retrieve("new_thuadat", ((EntityReference)chitietHDDTmia["new_thuadat"]).Id, new ColumnSet(new string[] { "new_nhomdat" }));
                            if (thuadat.Id == ThuaDatCTHD.Id)
                            {
                                chitietHDKQ = chitietHDDTmia;
                                break;
                            }
                        }

                        chitietHDKQ = service.Retrieve("new_thuadatcanhtac", chitietHDKQ.Id, new ColumnSet(new string[] { "new_vutrong", "new_loaigocmia", "new_mucdichsanxuatmia", "new_giongmia", "new_thuadat", "createdon", "new_hopdongdautumia", "new_khachhang", "new_khachhangdoanhnghiep", "new_thamgiamohinhkhuyennong" }));
                        Entity Giongmia = service.Retrieve("new_giongmia", ((EntityReference)chitietHDKQ["new_giongmia"]).Id, new ColumnSet(new string[] { "new_nhomgiong" }));
                        Entity Vudautu = service.Retrieve("new_vudautu", ((EntityReference)HDDTmia["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

                        // Tìm chính sách thu mua
                        EntityCollection Chinhsach = FindChinhSachThuMua(service, NghiemthusauTh);

                        Entity mCSTM = null;

                        if (Chinhsach != null && Chinhsach.Entities.Count > 0)
                        {
                            foreach (Entity a in Chinhsach.Entities)
                            {
                                if (a.Contains("new_vutrong_vl")) // Vụ trồng
                                    if (a["new_vutrong_vl"].ToString().IndexOf(((OptionSetValue)chitietHDKQ["new_vutrong"]).Value.ToString()) == -1)
                                        continue;

                                if (a.Contains("new_loaigocmia_vl")) // Loại gốc mía
                                    if (a["new_loaigocmia_vl"].ToString().IndexOf(((OptionSetValue)chitietHDKQ["new_loaigocmia"]).Value.ToString()) == -1)
                                        continue;

                                if (a.Contains("new_mucdichsanxuatmia_vl"))  // Muc dich san xuat mia
                                    if (a["new_mucdichsanxuatmia_vl"].ToString().IndexOf(((OptionSetValue)chitietHDKQ["new_mucdichsanxuatmia"]).Value.ToString()) == -1)
                                        continue;

                                if (a.Contains("new_nhomdat_vl"))  // Nhom dat
                                {
                                    if (thuadat.Attributes.Contains("new_nhomdat"))
                                    {
                                        if (a["new_nhomdat_vl"].ToString().IndexOf(((OptionSetValue)thuadat["new_nhomdat"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_loaisohuudat_vl"))  // Loai chu so huu dat
                                {
                                    if (thuadat.Attributes.Contains("new_loaisohuudat"))
                                    {
                                        if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)thuadat["new_loaisohuudat"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_nhomgiongmia_vl"))  // Nhom giong mia
                                {
                                    if (Giongmia.Attributes.Contains("new_nhomgiong"))
                                    {
                                        if (a["new_nhomgiongmia_vl"].ToString().IndexOf(((OptionSetValue)Giongmia["new_nhomgiong"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                if (a.Contains("new_tinhtrangmia_vl"))  // Tình trạng mía
                                {
                                    if (a["new_tinhtrangmia_vl"].ToString().IndexOf("100000001") < 0)  // 100000001: mía cháy
                                    {
                                        if (NghiemthusauTh.Contains("new_miachay"))
                                            continue;
                                    }
                                }

                                // NHom khach hang
                                bool co = false;

                                if (chitietHDKQ.Attributes.Contains("new_khachhang"))
                                {
                                    EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachthumua", "new_new_chinhsachthumua_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachthumuaid", a.Id);
                                    Entity KH = service.Retrieve("contact", ((EntityReference)chitietHDKQ["new_khachhang"]).Id, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang" }));

                                    if (KH.Attributes.Contains("new_nhomkhachhang"))
                                    {
                                        Guid nhomkhId = KH.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;
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
                                if (chitietHDKQ.Attributes.Contains("new_khachhangdoanhnghiep"))
                                {
                                    EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachthumua", "new_new_chinhsachthumua_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachthumuaid", a.Id);
                                    Entity KH = service.Retrieve("account", ((EntityReference)chitietHDKQ["new_khachhangdoanhnghiep"]).Id, new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

                                    if (KH.Attributes.Contains("new_nhomkhachhang"))
                                    {
                                        Guid nhomkhId = KH.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;
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

                                EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachthumua", "new_new_chinhsachthumua_new_vung", new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachthumuaid", a.Id);

                                if (thuadat.Attributes.Contains("new_vungdialy"))
                                {
                                    Guid vungdlId = thuadat.GetAttributeValue<EntityReference>("new_vungdialy").Id;
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

                                EntityCollection dsGiongmia = RetrieveNNRecord(service, "new_giongmia", "new_chinhsachthumua", "new_new_chinhsachthumua_new_giongmia", new ColumnSet(new string[] { "new_giongmiaid" }), "new_chinhsachthumuaid", a.Id);
                                if (dsGiongmia != null && dsGiongmia.Entities.Count > 0)
                                {
                                    foreach (Entity giongmia in dsGiongmia.Entities)
                                    {
                                        if (Giongmia.Id == giongmia.Id)
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
                                EntityCollection dsKKPTHDCT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_khuyenkhichpt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_thuadatcanhtacid", chitietHDKQ.Id);
                                EntityCollection dsKKPTCSTM = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_chinhsachthumua", "new_new_csthumua_new_khuyenkhichphattrien", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_chinhsachthumuaid", a.Id);

                                if (dsKKPTHDCT != null && dsKKPTHDCT.Entities.Count > 0)
                                {
                                    if (dsKKPTCSTM != null && dsKKPTCSTM.Entities.Count > 0)
                                    {
                                        foreach (Entity kkpt1 in dsKKPTHDCT.Entities)
                                        {
                                            foreach (Entity kkpt2 in dsKKPTCSTM.Entities)
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
                                    if (dsKKPTCSTM == null || dsKKPTCSTM.Entities.Count() == 0)
                                    {
                                        co = true;
                                    }
                                }
                                if (co == false)
                                    continue;

                                // Nhom cu ly
                                co = false;

                                EntityCollection dsNHomCL = RetrieveNNRecord(service, "new_nhomculy", "new_chinhsachthumua", "new_new_chinhsachthumua_new_nhomculy", new ColumnSet(new string[] { "new_nhomculyid" }), "new_chinhsachthumuaid", a.Id);
                                if (thuadat.Attributes.Contains("new_nhomculy"))
                                {
                                    Guid nhomclId = thuadat.GetAttributeValue<EntityReference>("new_nhomculy").Id;
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
                                EntityCollection dsMHKN = RetrieveNNRecord(service, "new_mohinhkhuyennong", "new_chinhsachthumua", "new_new_chinhsachthumua_new_mohinhkhuyennon", new ColumnSet(new string[] { "new_mohinhkhuyennongid" }), "new_chinhsachthumuaid", a.Id);

                                if (chitietHDKQ.Attributes.Contains("new_thamgiamohinhkhuyennong"))
                                {
                                    Guid mhknId = chitietHDKQ.GetAttributeValue<EntityReference>("new_thamgiamohinhkhuyennong").Id;
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
                                if (chitietHDKQ.Attributes.Contains("new_khachhang"))
                                {
                                    EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachthumua", "new_new_chinhsachthumua_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachthumuaid", a.Id);
                                    Entity KH = service.Retrieve("contact", ((EntityReference)chitietHDKQ["new_khachhang"]).Id, new ColumnSet(new string[] { "fullname", "new_nangsuatbinhquan" }));
                                    if (KH.Attributes.Contains("new_nangsuatbinhquan"))
                                    {
                                        decimal nangsuatbq = KH.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                        {
                                            foreach (Entity nhomns1 in dsNhomNS.Entities)
                                            {
                                                decimal nangsuattu = nhomns1.GetAttributeValue<decimal>("new_nangsuattu");
                                                decimal nangsuatden = nhomns1.GetAttributeValue<decimal>("new_nangsuatden");

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
                                if (chitietHDKQ.Attributes.Contains("new_khachhangdoanhnghiep"))
                                {
                                    Entity khachhang = service.Retrieve("account", ((EntityReference)chitietHDKQ["new_khachhangdoanhnghiep"]).Id, new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));
                                    EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachthumua", "new_new_chinhsachthumua_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachthumuaid", a.Id);

                                    if (khachhang.Attributes.Contains("new_nangsuatbinhquan"))
                                    {
                                        decimal nangsuatbq = khachhang.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                        {
                                            foreach (Entity nhomns1 in dsNhomNS.Entities)
                                            {
                                                decimal nangsuattu = nhomns1.GetAttributeValue<decimal>("new_nangsuattu");
                                                decimal nangsuatden = nhomns1.GetAttributeValue<decimal>("new_nangsuatden");

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

                                mCSTM = a;
                                break;
                            }

                            if (mCSTM != null && mCSTM.Id != Guid.Empty)
                            {

                                Guid kqCSTMid = mCSTM.Id;
                                Entity cstmKQEntity = service.Retrieve("new_chinhsachthumua", kqCSTMid, new ColumnSet(new string[] { "new_hinhthuctinhklthuongchatsatgoc", "new_thuongchatsatgoc" }));

                                // Cập nhật CSTM vào chi tiết nghiệm thu 
                                EntityReference cstmtRef = new EntityReference("new_chinhsachthumua", kqCSTMid);
                                chitietNT.Attributes.Add("new_chinhsachthumua", cstmtRef);
                                service.Update(chitietNT);

                                EntityCollection phieuDNthuongSatGocCol = null;
                                EntityCollection bienbanTTCDcol = FindBBCongDon(service, HDDTmia);
                                EntityCollection chitietBBTTCDcol = null;
                                Entity BBTTCD = bienbanTTCDcol[0];

                                foreach (Entity bienbanTTCD in bienbanTTCDcol.Entities)
                                {
                                    chitietBBTTCDcol = FindChitietBBCongDon(service, bienbanTTCD, thuadat);

                                    if (chitietBBTTCDcol != null && chitietBBTTCDcol.Entities.Count > 0)
                                    {
                                        if (BBTTCD.Attributes.Contains("new_daucongkh"))
                                        {
                                            Entity KH = service.Retrieve("contact", ((EntityReference)BBTTCD["new_daucongkh"]).Id, new ColumnSet(new string[] { "fullname" }));
                                            phieuDNthuongSatGocCol = FindphieuDnThuongSatGoc(service, HDDTmia, KH, thuadat);
                                        }
                                        if (BBTTCD.Attributes.Contains("new_daucongkhdn"))
                                        {
                                            Entity KH = service.Retrieve("account", ((EntityReference)BBTTCD["new_daucongkhdn"]).Id, new ColumnSet(new string[] { "name" }));
                                            phieuDNthuongSatGocCol = FindphieuDnThuongSatGocDN(service, HDDTmia, KH, thuadat);
                                        }

                                        if (cstmKQEntity.Contains("new_hinhthuctinhklthuongchatsatgoc"))
                                        {
                                            if (phieuDNthuongSatGocCol != null && phieuDNthuongSatGocCol.Entities.Count > 0)
                                            {
                                                // Cập nhật phiếu đề nghị thưởng chặt sát gốc
                                                Entity phieuDNthuong = phieuDNthuongSatGocCol[0];
                                                decimal thuongSatgoccstm = (cstmKQEntity.Contains("new_thuongchatsatgoc") ? ((Money)cstmKQEntity["new_thuongchatsatgoc"]).Value : 0);
                                                Money MthuongSatgoccstm = new Money(thuongSatgoccstm);
                                                decimal khoiluongmia = 1;
                                                decimal miatuoi = (chitietNT.Contains("new_miatuoi") ? (decimal)chitietNT["new_miatuoi"] : 0);
                                                decimal miachay = (chitietNT.Contains("new_miachay") ? (decimal)chitietNT["new_miachay"] : 0);

                                                if (cstmKQEntity.GetAttributeValue<OptionSetValue>("new_hinhthuctinhklthuongchatsatgoc").Value.ToString() == "100000000")
                                                    khoiluongmia = miatuoi;
                                                if (cstmKQEntity.GetAttributeValue<OptionSetValue>("new_hinhthuctinhklthuongchatsatgoc").Value.ToString() == "100000001")
                                                    khoiluongmia = miatuoi + miachay;

                                                phieuDNthuong.Attributes["new_dinhmucthuong"] = MthuongSatgoccstm;
                                                phieuDNthuong.Attributes["new_klmiaduocthuong"] = khoiluongmia;

                                                service.Update(phieuDNthuong);
                                            }
                                            else
                                            {
                                                Entity phieuDNthuong = new Entity("new_phieudenghithuong");

                                                string ten = "Thưởng cho đầu công chặt sát gốc";
                                                DateTime ngaylap = DateTime.Now;
                                                decimal thuongSatgoccstm = (cstmKQEntity.Contains("new_thuongchatsatgoc") ? ((Money)cstmKQEntity["new_thuongchatsatgoc"]).Value : 0);
                                                Money MthuongSatgoccstm = new Money(thuongSatgoccstm);
                                                decimal khoiluongmia = 1;
                                                decimal miatuoi = (chitietNT.Contains("new_miatuoi") ? (decimal)chitietNT["new_miatuoi"] : 0);
                                                decimal miachay = (chitietNT.Contains("new_miachay") ? (decimal)chitietNT["new_miachay"] : 0);

                                                if (cstmKQEntity.GetAttributeValue<OptionSetValue>("new_hinhthuctinhklthuongchatsatgoc").Value.ToString() == "100000000")
                                                    khoiluongmia = miatuoi;
                                                if (cstmKQEntity.GetAttributeValue<OptionSetValue>("new_hinhthuctinhklthuongchatsatgoc").Value.ToString() == "100000001")
                                                    khoiluongmia = miatuoi + miachay;

                                                phieuDNthuong.Attributes.Add("new_name", ten);
                                                phieuDNthuong.Attributes.Add("new_ngaylapphieu", ngaylap);

                                                if (BBTTCD.Attributes.Contains("new_daucongkh"))
                                                {
                                                    EntityReference khRef = (EntityReference)BBTTCD["new_daucongkh"];
                                                    phieuDNthuong.Attributes.Add("new_khachhang", khRef);
                                                }
                                                if (BBTTCD.Attributes.Contains("new_daucongkhdn"))
                                                {
                                                    EntityReference khRef = (EntityReference)BBTTCD["new_daucongkhdn"];
                                                    phieuDNthuong.Attributes.Add("new_khachhangdoanhnghiep", khRef);
                                                }

                                                EntityReference hdthRef = chitietNT.GetAttributeValue<EntityReference>("new_hopdongthuhoach");

                                                phieuDNthuong.Attributes.Add("new_vudautu", Vudautu.ToEntityReference());
                                                phieuDNthuong.Attributes.Add("new_hopdongdautumia", HDDTmia.ToEntityReference());
                                                phieuDNthuong.Attributes.Add("new_loaithuong", new OptionSetValue(100000002));
                                                phieuDNthuong.Attributes.Add("new_nghiemthusauthuhoach", NghiemthusauTh.ToEntityReference());
                                                phieuDNthuong.Attributes.Add("new_hopdongthuhoach", hdthRef);
                                                phieuDNthuong.Attributes.Add("new_thuadat", thuadatRef);
                                                phieuDNthuong.Attributes.Add("new_dinhmucthuong", MthuongSatgoccstm);
                                                phieuDNthuong.Attributes.Add("new_klmiaduocthuong", khoiluongmia);

                                                service.Create(phieuDNthuong);
                                            }
                                        } //if (cstmKQEntity.Contains("new_hinhthuctinhklthuongchatsatgoc"))
                                    } // if (chitietBBTTCDcol != null && chitietBBTTCDcol.Entities.Count > 0)
                                }
                            } // Nếu tìm được CSTM
                            else
                            {
                                throw new InvalidPluginExecutionException("Không tìm thấy Chính sách Thu mua phù hợp");
                            }
                        } //if (Chinhsach != null && Chinhsach.Entities.Count > 0)
                    } //foreach (Entity chitietNT in chitietNTsauTHCol.Entities)

                } // Nếu tình trạng đã duyệt
            }//using
        }

        public static string GetWindowsIntegratedSecurityConnectionString()
        {
            int count = ConfigurationManager.ConnectionStrings.Count;
            if (count == 0)
                throw new Exception("Could not find ConnectionString");

            return ConfigurationManager.ConnectionStrings[0].ConnectionString;
        }

        public static EntityCollection FindchitietHDDTmia(IOrganizationService crmservices, Entity HDDTmia)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_thuadatcanhtac'>
                    <attribute name='new_name' />
                    <attribute name='new_dientichthucte' />
                    <attribute name='new_hopdongdautumia' />
                    <attribute name='new_thuadat' />'
                    <attribute name='statuscode' />
                    <attribute name='new_khachhangdoanhnghiep' />
                    <attribute name='new_khachhang' />
                    <attribute name='new_giongmia' />
                    <attribute name='new_chatsatgoc' />
                    <attribute name='new_dinhmucdautu' />
                    <attribute name='new_thuadatcanhtacid' />
                    <order attribute='new_name' descending='false' />
                    <filter type='and'>
                      <condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='{0}' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, HDDTmia.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindchitietNTsauTH(IOrganizationService crmservices, Entity NTsauTH)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_chitietnghiemthusauthuhoach'>
                    <attribute name='new_name' />
                    <attribute name='new_tongsanluong' />
                    <attribute name='new_thuadat' />
                    <attribute name='new_nghiemthusauthuhoach' />
                    <attribute name='new_miatuoi' />
                    <attribute name='new_miachay' />
                    <attribute name='new_dientich' /> 
                    <attribute name='new_hopdongthuhoach' />
                    <attribute name='new_chitietnghiemthusauthuhoachid' />
                    <attribute name='createdon' />
                    <order attribute='createdon' descending='false' />
                    <filter type='and'>
                      <condition attribute='new_nghiemthusauthuhoach' operator='eq' uitype='new_nghiemthuchatsatgoc' value='{0}' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, NTsauTH.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindBBCongDon(IOrganizationService crmservices, Entity hddtMia)
        {
            string fetchXml =
               @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_bienbanthoathuancongdon'>
                    <attribute name='new_name' />
                    <attribute name='createdon' />
                    <attribute name='new_mabienban' />
                    <attribute name='new_vudautu' />
                    <attribute name='new_tram' />
                    <attribute name='new_ngaylapphieu' />
                    <attribute name='new_chumiakhdn' />
                    <attribute name='new_chumiakh' />
                    <attribute name='new_daucongkh' />
                    <attribute name='new_daucongkhdn' />
                    <attribute name='new_canbonongvu' />
                    <attribute name='new_bienbanthoathuancongdonid' />
                    <order attribute='new_ngaylapphieu' descending='false' />
                    <filter type='and'>
                      <condition attribute='statecode' operator='eq' value='0' />
                      <condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='{0}' />
                      <condition attribute='statuscode' operator='eq' value='100000000' />
                    </filter>
                  </entity>
                </fetch>";

            Guid hddtmiaId = hddtMia.Id;
            fetchXml = string.Format(fetchXml, hddtmiaId);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }

        public static EntityCollection FindChitietBBCongDon(IOrganizationService crmservices, Entity BBCongDon, Entity thuadat)
        {
            string fetchCTCongdon =
               @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_chitietbbthoathuancongdon'>
                    <attribute name='new_name' />
                    <attribute name='new_thuadat' />
                    <attribute name='new_sanluonguoctinh' />
                    <attribute name='new_giacongmiachay' />
                    <attribute name='new_dientichthuchien' />
                    <attribute name='new_giatongcong' />
                    <attribute name='new_congdonchatvabocmia' />
                    <attribute name='new_bbthoathuancongdon' />
                    <attribute name='new_chitietbbthoathuancongdonid' />
                    <order attribute='new_dientichthuchien' descending='true' />
                    <filter type='and'>
                      <condition attribute='statecode' operator='eq' value='0' />
                      <condition attribute='new_bbthoathuancongdon' operator='eq' uitype='new_bienbanthoathuancongdon' value='{0}' />
                      <condition attribute='new_thuadat' operator='eq' uitype='new_thuadat' value='{1}' />
                    </filter>
                  </entity>
                </fetch>";

            Guid bbcongdonId = BBCongDon.Id;
            Guid thuadatId = thuadat.Id;
            fetchCTCongdon = string.Format(fetchCTCongdon, bbcongdonId, thuadatId);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchCTCongdon));
            return entc;
        }

        public static EntityCollection FindChinhSachThuMua(IOrganizationService crmservices, Entity NTsauTH)
        {
            string fetchXml =
                @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_chinhsachthumua'>
                    <attribute name='new_name' />
                    <attribute name='new_vudautu' />
                    <attribute name='new_thoidiemapdung' />
                    <attribute name='new_hoatdongapdung' />
                    <attribute name='new_congdieutunoikhac' />   
                    <attribute name='new_machinhsach' />
                    <attribute name='new_loaigocmia_vl' />
                    <attribute name='new_nhomdat_vl' />
                    <attribute name='new_vutrong_vl' />
                    <attribute name='new_mucdichsanxuatmia_vl' />
                    <attribute name='new_nhomgiongmia_vl' />
                    <attribute name='new_loaisohuudat_vl' />
                    <attribute name='new_loaimiachay_vl' />
                    <attribute name='new_tinhtrangmia_vl' />
                    <attribute name='new_tinhtrangmia' />
                    <attribute name='new_chinhsachthumuaid' />
                    <order attribute='new_thoidiemapdung' descending='true' />
                    <filter type='and'>
                      <condition attribute='statecode' operator='eq' value='0' />
                      <condition attribute='new_hoatdongapdung' operator='eq' value='100000001' />
                      <condition attribute='new_thoidiemapdung' operator='on-or-before' value='{0}' />
                      <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{1}' />
                    </filter>
                  </entity>
                </fetch>";

            EntityReference HDDTmiaRef = NTsauTH.GetAttributeValue<EntityReference>("new_hopdongdautumia");
            Entity HDDTmia = crmservices.Retrieve("new_hopdongdautumia", HDDTmiaRef.Id, new ColumnSet(new string[] { "new_vudautu" }));
            Guid vudautuId = (HDDTmia.GetAttributeValue<EntityReference>("new_vudautu")).Id;
            DateTime ngayNT = NTsauTH.GetAttributeValue<DateTime>("new_ngaynghiemthu");

            fetchXml = string.Format(fetchXml, ngayNT, vudautuId);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }

        public static EntityCollection FindphieuDnThuongSatGoc(IOrganizationService crmservices, Entity HD, Entity KH, Entity TD)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_phieudenghithuong'>
                        <attribute name='new_name' />
                        <attribute name='new_tienthuong' />
                        <attribute name='new_ngaylapphieu' />
                        <attribute name='new_masophieu' />
                        <attribute name='new_loaithuong' />
                        <attribute name='new_thuadat' />
                        <attribute name='new_hopdongdautumia' />
                        <attribute name='new_tinhtrangduyet' />
                        <attribute name='statuscode' />
                        <attribute name='new_phieudenghithuongid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='{0}' />
                          <condition attribute='new_khachhang' operator='eq' uitype='contact' value='{1}' />
                          <condition attribute='new_thuadat' operator='eq' uitype='new_thuadat' value='{2}' />
                          <condition attribute='new_loaithuong' operator='eq' value='100000002' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, HD.Id, KH.Id, TD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindphieuDnThuongSatGocDN(IOrganizationService crmservices, Entity HD, Entity KH, Entity TD)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_phieudenghithuong'>
                        <attribute name='new_name' />
                        <attribute name='new_tienthuong' />
                        <attribute name='new_ngaylapphieu' />
                        <attribute name='new_masophieu' />
                        <attribute name='new_loaithuong' />
                        <attribute name='new_thuadat' />
                        <attribute name='new_hopdongdautumia' />
                        <attribute name='new_tinhtrangduyet' />
                        <attribute name='statuscode' />
                        <attribute name='new_phieudenghithuongid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='{0}' />
                          <condition attribute='new_khachhangdoanhnghiep' operator='eq' uitype='account' value='{1}' />
                          <condition attribute='new_thuadat' operator='eq' uitype='new_thuadat' value='{2}' />
                          <condition attribute='new_loaithuong' operator='eq' value='100000002' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, HD.Id, KH.Id, TD.Id);
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