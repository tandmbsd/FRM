using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_Sotiendenghi_PDKThuoc
{
    public class Plugin_Sotiendenghi_PDKThuoc : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));

            #region CREATE
            if (context.MessageName.ToUpper() == "CREATE")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity target = (Entity)context.InputParameters["Target"];

                Entity chitietpdkthuoc = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                if (!chitietpdkthuoc.Contains("new_phieudangkythuoc"))
                {
                    throw new Exception("Chi tiết pdk thuốc không có phiếu đăng ký thuốc !!! ");
                }

                Entity pdkthuoc = service.Retrieve("new_phieudangkythuoc", ((EntityReference)chitietpdkthuoc["new_phieudangkythuoc"]).Id, new ColumnSet(true));

                decimal thanhtien = 0;
                decimal TongDMDTHL = 0;
                decimal TongDMDTHLvattu = 0;
                decimal TongDMDTKHL = 0;

                decimal DMDTHL = 0;
                decimal DMDTHLvattu = 0;
                decimal DMDTKHL = 0;

                decimal daGiainganHLtienmat = 0;
                decimal daGiainganHLvattu = 0;
                decimal daGiainganKHL = 0;

                decimal DenghiHL = 0;
                decimal DenghiHLvattu = 0;
                decimal DenghiKHL = 0;

                List<Entity> listCTPDKT = RetrieveMultiRecord(service, "new_chitietdangkythuoc", new ColumnSet(true), "new_phieudangkythuoc", pdkthuoc.Id);

                foreach (Entity ct in listCTPDKT)
                {
                    decimal dongia = ct.Contains("new_dongia") ? ((Money)ct["new_dongia"]).Value : 0;
                    decimal sluong = ct.Contains("new_soluong") ? (decimal)ct["new_soluong"] : 0;

                    thanhtien += (dongia * sluong);
                }

                EntityCollection listCTHD = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieudangkythuoc", "new_new_pdkthuoc_new_chitiethddtmia", new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_phieudangkythuocid", pdkthuoc.Id);
                if (listCTHD != null && listCTHD.Entities.Count > 0)
                {
                    #region for listCTHD
                    foreach (Entity cthd in listCTHD.Entities)
                    {
                        #region Tim chinh sach dau tu
                        Entity ChiTietHD = service.Retrieve("new_thuadatcanhtac", cthd.Id, new ColumnSet(new string[] { "new_vutrong", "new_loaigocmia", "new_mucdichsanxuatmia", "new_giongmia", "new_thuadat", "createdon", "new_hopdongdautumia", "new_khachhang", "new_khachhangdoanhnghiep", "new_thamgiamohinhkhuyennong", "new_dientichthucte", "new_tuoimia", "new_dientichhopdong", "new_dinhmucphanbontoithieu", "new_copytuhddtthuedat", "new_trangthainghiemthu", "new_dachikhonghoanlai_homgiong", "new_dachihoanlai_homgiong", "new_tongchikhonghoanlai", "new_tongchihoanlai", "new_yeucaudacbiet" }));
                        DateTime ngaytao = chitietpdkthuoc.GetAttributeValue<DateTime>("createdon");

                        EntityReference thuadatEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_thuadat");
                        Entity thuadatObj = service.Retrieve("new_thuadat", thuadatEntityRef.Id, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy" }));

                        EntityReference giongmiaEntityRef = null;
                        Guid giongmiaId = new Guid();
                        Entity giongmiaObj = null;
                        if (ChiTietHD.Attributes.Contains("new_giongmia"))
                        {
                            giongmiaEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_giongmia");
                            giongmiaId = giongmiaEntityRef.Id;
                            giongmiaObj = service.Retrieve("new_giongmia", giongmiaId, new ColumnSet(new string[] { "new_nhomgiong", "new_name" }));
                        }

                        EntityReference HDDTmiaRef = ChiTietHD.GetAttributeValue<EntityReference>("new_hopdongdautumia");
                        Entity HDDTmia = service.Retrieve("new_hopdongdautumia", HDDTmiaRef.Id, new ColumnSet(new string[] { "new_vudautu" }));

                        EntityReference vudautuRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");
                        Entity Vudautu = service.Retrieve("new_vudautu", vudautuRef.Id, new ColumnSet(new string[] { "new_name" }));

                        if (vudautuRef == null || vudautuRef.Id == Guid.Empty)
                        {
                            throw new InvalidPluginExecutionException("Trong HĐĐT mía chưa có Vụ đầu tư !");
                        }
                        else
                        {
                            Guid vuDTId = vudautuRef.Id;
                            EntityCollection resultCol = FindCSDTtrongmia(service, ngaytao, Vudautu);
                            Entity mCSDT = null;

                            if (resultCol != null && resultCol.Entities.Count > 0)
                            {
                                foreach (Entity a in resultCol.Entities)
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
                                            if (a["new_loaigocmia_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_loaigocmia"]).Value.ToString()) == -1)
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
                                        if (thuadatObj.Attributes.Contains("new_nhomdat"))
                                        {
                                            if (a["new_nhomdat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_nhomdat"]).Value.ToString()) == -1)
                                                continue;
                                        }
                                        else
                                            continue;
                                    }

                                    if (a.Contains("new_loaisohuudat_vl"))  // Loai chu so huu
                                    {
                                        if (thuadatObj.Attributes.Contains("new_loaisohuudat"))
                                        {
                                            if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_loaisohuudat"]).Value.ToString()) == -1)
                                                continue;
                                        }
                                        else
                                            continue;
                                    }

                                    if (a.Contains("new_nhomgiongmia_vl"))  // Nhom giong mia
                                    {
                                        if (giongmiaObj.Attributes.Contains("new_nhomgiong"))
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
                            }
                            #endregion
                            if (mCSDT != null && mCSDT.Id != Guid.Empty)
                            {
                                Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", mCSDT.Id, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name", "new_loailaisuatcodinhthaydoi", "new_dinhmucdautukhonghoanlai", "new_dinhmucphanbontoithieu" }));
                                decimal dientichhd = (ChiTietHD.Contains("new_dientichhopdong") ? (decimal)ChiTietHD["new_dientichhopdong"] : 0);
                                string tinhtrangnghiemthu = ((OptionSetValue)ChiTietHD["new_trangthainghiemthu"]).Value.ToString();
                                bool yeucaudacbiet = ChiTietHD.Contains("new_yeucaudacbiet") ? (bool)ChiTietHD["new_yeucaudacbiet"] : false;

                                decimal tyle = 0;
                                int langiaingan = 0;

                                // Tim dinh muc dau tu
                                EntityCollection DinhmucDTTMcol = FindDaututienmat(service, csdtKQEntity);
                                if (DinhmucDTTMcol != null && DinhmucDTTMcol.Entities.Count > 0)
                                {
                                    foreach (Entity dmdttm in DinhmucDTTMcol.Entities)
                                    {
                                        string yeucau = ((OptionSetValue)dmdttm["new_yeucau"]).Value.ToString();

                                        if (yeucaudacbiet == true)
                                        {
                                            DMDTHLvattu += dmdttm.Contains("new_phanbontoithieuyc") ? ((Money)dmdttm["new_phanbontoithieuyc"]).Value : 0;
                                            tyle += dmdttm.Contains("new_tyleyc") ? (decimal)dmdttm["new_tyleyc"] : 0;
                                        }
                                        else
                                        {
                                            DMDTHLvattu += dmdttm.Contains("new_phanbontoithieubt") ? ((Money)dmdttm["new_phanbontoithieubt"]).Value : 0;
                                            tyle += (dmdttm.Contains("new_phantramtilegiaingan") ? (decimal)dmdttm["new_phantramtilegiaingan"] : 0);
                                        }

                                        if (yeucau == tinhtrangnghiemthu)
                                        {
                                            langiaingan = (dmdttm.Contains("new_langiaingan") ? (int)dmdttm["new_langiaingan"] : 0);
                                            break;
                                        }
                                    }
                                }

                                //decimal dongiaDTKHL = (csdtKQEntity.Contains("new_dinhmucdautukhonghoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautukhonghoanlai").Value : 0);
                                decimal dongiaDTKHL = cthd.Contains("new_dautukhonghoanlai") ? ((Money)cthd["new_dautukhonghoanlai"]).Value : 0;
                                DMDTKHL = dongiaDTKHL * tyle / 100;

                                ////decimal dongiaDTHLvattu = (csdtKQEntity.Contains("new_dinhmucphanbontoithieu") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucphanbontoithieu").Value : 0);
                                ////DMDTHLvattu = dongiaDTHLvattu * tyle / 100;

                                //decimal dongiaDTHL = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value : 0);
                                // DMDTHL = ((dongiaDTHL - dongiaDTHLvattu) * dientichhd * tyle) / 100;
                                decimal dongiaDTHL = cthd.Contains("new_dautuhoanlai") ? ((Money)cthd["new_dautuhoanlai"]).Value : 0;
                                DMDTHL = dongiaDTHL * tyle / 100;
                            }

                            List<Entity> lstPhanbodautu = RetrieveMultiRecord(service, "new_phanbodautu", new ColumnSet(true), "new_thuacanhtac", cthd.Id);

                            foreach (Entity en1 in lstPhanbodautu)
                            {
                                daGiainganKHL += en1.Contains("new_khonghoanlai_homgiong") ? ((Money)en1["new_khonghoanlai_homgiong"]).Value : 0;
                                daGiainganHLvattu += en1.Contains("new_hoanlai_homgiong") ? ((Money)en1["new_hoanlai_homgiong"]).Value : 0;
                                daGiainganHLtienmat += en1.Contains("new_hoanlai_tienmat") ? ((Money)en1["new_hoanlai_tienmat"]).Value : 0;
                            }

                            //daGiainganHLvattu += (ChiTietHD.Contains("new_dachihoanlai_homgiong") ? ChiTietHD.GetAttributeValue<Money>("new_dachihoanlai_homgiong").Value : 0);
                            //daGiainganKHL += ((ChiTietHD.Contains("new_dachikhonghoanlai_homgiong") ? ChiTietHD.GetAttributeValue<Money>("new_dachikhonghoanlai_homgiong").Value : 0) + (ChiTietHD.Contains("new_dachihoanlai_homgiong") ? ChiTietHD.GetAttributeValue<Money>("new_dachihoanlai_homgiong").Value : 0));

                            TongDMDTHL += DMDTHL;
                            TongDMDTHLvattu += DMDTHLvattu;
                            TongDMDTKHL += DMDTKHL;
                        } // có Vu dau tu
                    } // foreach (Entity cthd in listCTHD.Entities)
                    #endregion
                }

                DenghiKHL = TongDMDTKHL - daGiainganKHL;
                DenghiHL = TongDMDTHL - (daGiainganHLtienmat + daGiainganHLvattu);
                DenghiHLvattu = TongDMDTHLvattu - daGiainganHLvattu;

                Entity newthuoc = new Entity("new_phieudangkythuoc");
                newthuoc.Id = pdkthuoc.Id;

                newthuoc["new_dinhmuc_khonghoanlai"] = new Money(TongDMDTKHL + (pdkthuoc.Contains("new_dinhmuc_khonghoanlai") ? ((Money)pdkthuoc["new_dinhmuc_khonghoanlai"]).Value : 0));
                newthuoc["new_dinhmuc_hoanlai_vattu"] = new Money(TongDMDTHLvattu + (pdkthuoc.Contains("new_dinhmuc_hoanlai_vattu") ? ((Money)pdkthuoc["new_dinhmuc_hoanlai_vattu"]).Value : 0));
                newthuoc["new_dinhmuc_hoanlai_tienmat"] = new Money(TongDMDTHL + (pdkthuoc.Contains("new_dinhmuc_hoanlai_tienmat") ? ((Money)pdkthuoc["new_dinhmuc_hoanlai_tienmat"]).Value : 0));

                newthuoc["new_giaingan_khonghoanlai"] = new Money(daGiainganKHL + (pdkthuoc.Contains("new_giaingan_khonghoanlai") ? ((Money)pdkthuoc["new_giaingan_khonghoanlai"]).Value : 0));
                newthuoc["new_giaingan_hoanlai_vattu"] = new Money(daGiainganHLvattu + (pdkthuoc.Contains("new_giaingan_hoanlai_vattu") ? ((Money)pdkthuoc["new_giaingan_hoanlai_vattu"]).Value : 0));
                newthuoc["new_giaingan_hoanlai_tienmat"] = new Money(0);

                newthuoc["new_denghi_khonghoanlai"] = new Money(DenghiKHL + (pdkthuoc.Contains("new_denghi_khonghoanlai") ? ((Money)pdkthuoc["new_denghi_khonghoanlai"]).Value : 0));
                newthuoc["new_denghi_hoanlai_vattu"] = new Money(DenghiHLvattu + (pdkthuoc.Contains("new_denghi_hoanlai_vattu") ? ((Money)pdkthuoc["new_denghi_hoanlai_vattu"]).Value : 0));

                service.Update(newthuoc);
            }
            #endregion
            #region Update
            if (context.MessageName.ToUpper() == "UPDATE")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity target = (Entity)context.InputParameters["Target"];

                if (target.Contains("new_soluong"))
                {
                    Entity chitietpdkthuoc = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                    if (!chitietpdkthuoc.Contains("new_phieudangkythuoc"))
                    {
                        throw new Exception("Chi tiết pdk thuốc không có phiếu đăng ký thuốc !!! ");
                    }

                    Entity pdkthuoc = service.Retrieve("new_phieudangkythuoc", ((EntityReference)chitietpdkthuoc["new_phieudangkythuoc"]).Id, new ColumnSet(true));

                    decimal thanhtien = 0;
                    decimal TongDMDTHL = 0;
                    decimal TongDMDTHLvattu = 0;
                    decimal TongDMDTKHL = 0;

                    decimal DMDTHL = 0;
                    decimal DMDTHLvattu = 0;
                    decimal DMDTKHL = 0;

                    decimal daGiainganHLtienmat = 0;
                    decimal daGiainganHLvattu = 0;
                    decimal daGiainganKHL = 0;

                    decimal DenghiHL = 0;
                    decimal DenghiHLvattu = 0;
                    decimal DenghiKHL = 0;

                    List<Entity> listCTPDKT = RetrieveMultiRecord(service, "new_chitietdangkythuoc", new ColumnSet(true), "new_phieudangkythuoc", pdkthuoc.Id);

                    foreach (Entity ct in listCTPDKT)
                    {
                        decimal dongia = ct.Contains("new_dongia") ? ((Money)ct["new_dongia"]).Value : 0;
                        decimal sluong = ct.Contains("new_soluong") ? (decimal)ct["new_soluong"] : 0;

                        thanhtien += (dongia * sluong);
                    }

                    EntityCollection listCTHD = RetrieveNNRecord(service, "new_thuadatcanhtac", "new_phieudangkythuoc", "new_new_pdkthuoc_new_chitiethddtmia", new ColumnSet(true), "new_phieudangkythuocid", pdkthuoc.Id);
                    if (listCTHD != null && listCTHD.Entities.Count > 0)
                    {
                        #region for listCTHD
                        foreach (Entity cthd in listCTHD.Entities)
                        {
                            #region Tim chinh sach dau tu
                            Entity ChiTietHD = service.Retrieve("new_thuadatcanhtac", cthd.Id, new ColumnSet(new string[] { "new_vutrong", "new_loaigocmia", "new_mucdichsanxuatmia", "new_giongmia", "new_thuadat", "createdon", "new_hopdongdautumia", "new_khachhang", "new_khachhangdoanhnghiep", "new_thamgiamohinhkhuyennong", "new_dientichthucte", "new_tuoimia", "new_dientichhopdong", "new_dinhmucphanbontoithieu", "new_copytuhddtthuedat", "new_trangthainghiemthu", "new_dachikhonghoanlai_homgiong", "new_dachihoanlai_homgiong", "new_tongchikhonghoanlai", "new_tongchihoanlai", "new_yeucaudacbiet" }));
                            DateTime ngaytao = chitietpdkthuoc.GetAttributeValue<DateTime>("createdon");

                            EntityReference thuadatEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_thuadat");
                            Entity thuadatObj = service.Retrieve("new_thuadat", thuadatEntityRef.Id, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy" }));

                            EntityReference giongmiaEntityRef = null;
                            Guid giongmiaId = new Guid();
                            Entity giongmiaObj = null;
                            if (ChiTietHD.Attributes.Contains("new_giongmia"))
                            {
                                giongmiaEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_giongmia");
                                giongmiaId = giongmiaEntityRef.Id;
                                giongmiaObj = service.Retrieve("new_giongmia", giongmiaId, new ColumnSet(new string[] { "new_nhomgiong", "new_name" }));
                            }

                            EntityReference HDDTmiaRef = ChiTietHD.GetAttributeValue<EntityReference>("new_hopdongdautumia");
                            Entity HDDTmia = service.Retrieve("new_hopdongdautumia", HDDTmiaRef.Id, new ColumnSet(new string[] { "new_vudautu" }));

                            EntityReference vudautuRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");
                            Entity Vudautu = service.Retrieve("new_vudautu", vudautuRef.Id, new ColumnSet(new string[] { "new_name" }));

                            if (vudautuRef == null || vudautuRef.Id == Guid.Empty)
                            {
                                throw new InvalidPluginExecutionException("Trong HĐĐT mía chưa có Vụ đầu tư !");
                            }
                            else
                            {
                                Guid vuDTId = vudautuRef.Id;

                                EntityCollection resultCol = FindCSDTtrongmia(service, ngaytao, Vudautu);
                                Entity mCSDT = null;

                                if (resultCol != null && resultCol.Entities.Count > 0)
                                {
                                    foreach (Entity a in resultCol.Entities)
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
                                                if (a["new_loaigocmia_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_loaigocmia"]).Value.ToString()) == -1)
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
                                            if (thuadatObj.Attributes.Contains("new_nhomdat"))
                                            {
                                                if (a["new_nhomdat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_nhomdat"]).Value.ToString()) == -1)
                                                    continue;
                                            }
                                            else
                                                continue;
                                        }

                                        if (a.Contains("new_loaisohuudat_vl"))  // Loai chu so huu
                                        {
                                            if (thuadatObj.Attributes.Contains("new_loaisohuudat"))
                                            {
                                                if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_loaisohuudat"]).Value.ToString()) == -1)
                                                    continue;
                                            }
                                            else
                                                continue;
                                        }

                                        if (a.Contains("new_nhomgiongmia_vl"))  // Nhom giong mia
                                        {
                                            if (giongmiaObj.Attributes.Contains("new_nhomgiong"))
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
                                }
                                #endregion

                                if (mCSDT != null && mCSDT.Id != Guid.Empty)
                                {
                                    Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", mCSDT.Id, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name", "new_loailaisuatcodinhthaydoi", "new_dinhmucdautukhonghoanlai", "new_dinhmucphanbontoithieu" }));

                                    decimal dientichhd = (ChiTietHD.Contains("new_dientichhopdong") ? (decimal)ChiTietHD["new_dientichhopdong"] : 0);
                                    string tinhtrangnghiemthu = ((OptionSetValue)ChiTietHD["new_trangthainghiemthu"]).Value.ToString();
                                    bool yeucaudacbiet = ChiTietHD.Contains("new_yeucaudacbiet") ? (bool)ChiTietHD["new_yeucaudacbiet"] : false;
                                    decimal tyle = 0;
                                    int langiaingan = 0;

                                    // Tim dinh muc dau tu
                                    EntityCollection DinhmucDTTMcol = FindDaututienmat(service, csdtKQEntity);
                                    if (DinhmucDTTMcol != null && DinhmucDTTMcol.Entities.Count > 0)
                                    {
                                        foreach (Entity dmdttm in DinhmucDTTMcol.Entities)
                                        {
                                            string yeucau = ((OptionSetValue)dmdttm["new_yeucau"]).Value.ToString();

                                            if (yeucaudacbiet == true)
                                            {
                                                DMDTHLvattu += dmdttm.Contains("new_phanbontoithieuyc") ? ((Money)dmdttm["new_phanbontoithieuyc"]).Value : 0;
                                                tyle += dmdttm.Contains("new_tyleyc") ? (decimal)dmdttm["new_tyleyc"] : 0;
                                            }
                                            else
                                            {
                                                DMDTHLvattu += dmdttm.Contains("new_phanbontoithieubt") ? ((Money)dmdttm["new_phanbontoithieubt"]).Value : 0;
                                                tyle += (dmdttm.Contains("new_phantramtilegiaingan") ? (decimal)dmdttm["new_phantramtilegiaingan"] : 0);
                                            }

                                            if (yeucau == tinhtrangnghiemthu)
                                            {
                                                langiaingan = (dmdttm.Contains("new_langiaingan") ? (int)dmdttm["new_langiaingan"] : 0);
                                                break;
                                            }
                                        }
                                    }

                                    //decimal dongiaDTKHL = (csdtKQEntity.Contains("new_dinhmucdautukhonghoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautukhonghoanlai").Value : 0);
                                    decimal dongiaDTKHL = cthd.Contains("new_dautukhonghoanlai") ? ((Money)cthd["new_dautukhonghoanlai"]).Value : 0;
                                    DMDTKHL = dongiaDTKHL * tyle / 100;
                                    ////decimal dongiaDTHLvattu = (csdtKQEntity.Contains("new_dinhmucphanbontoithieu") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucphanbontoithieu").Value : 0);
                                    ////DMDTHLvattu = dongiaDTHLvattu * tyle / 100;

                                    //decimal dongiaDTHL = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value : 0);
                                    // DMDTHL = ((dongiaDTHL - dongiaDTHLvattu) * dientichhd * tyle) / 100;
                                    decimal dongiaDTHL = cthd.Contains("new_dautuhoanlai") ? ((Money)cthd["new_dautuhoanlai"]).Value : 0;
                                    DMDTHL = dongiaDTHL * tyle / 100;
                                }

                                List<Entity> lstPhanbodautu = RetrieveMultiRecord(service, "new_phanbodautu", new ColumnSet(true), "new_thuacanhtac", cthd.Id);

                                foreach (Entity en1 in lstPhanbodautu)
                                {
                                    daGiainganKHL += en1.Contains("new_khonghoanlai_homgiong") ? ((Money)en1["new_khonghoanlai_homgiong"]).Value : 0;
                                    daGiainganHLvattu += en1.Contains("new_hoanlai_homgiong") ? ((Money)en1["new_hoanlai_homgiong"]).Value : 0;
                                    daGiainganHLtienmat += en1.Contains("new_hoanlai_tienmat") ? ((Money)en1["new_hoanlai_tienmat"]).Value : 0;
                                }

                                //daGiainganHLvattu += (ChiTietHD.Contains("new_dachihoanlai_homgiong") ? ChiTietHD.GetAttributeValue<Money>("new_dachihoanlai_homgiong").Value : 0);
                                //daGiainganKHL += ((ChiTietHD.Contains("new_dachikhonghoanlai_homgiong") ? ChiTietHD.GetAttributeValue<Money>("new_dachikhonghoanlai_homgiong").Value : 0) + (ChiTietHD.Contains("new_dachihoanlai_homgiong") ? ChiTietHD.GetAttributeValue<Money>("new_dachihoanlai_homgiong").Value : 0));

                                TongDMDTHL += DMDTHL;
                                TongDMDTHLvattu += DMDTHLvattu;
                                TongDMDTKHL += DMDTKHL;

                            } // có Vu dau tu
                        } // foreach (Entity cthd in listCTHD.Entities)
                        #endregion
                    }

                    DenghiKHL = TongDMDTKHL - daGiainganKHL;
                    DenghiHL = TongDMDTHL - (daGiainganHLtienmat + daGiainganHLvattu);
                    DenghiHLvattu = TongDMDTHLvattu - daGiainganHLvattu;

                    Entity newthuoc = new Entity("new_phieudangkythuoc");
                    newthuoc.Id = pdkthuoc.Id;

                    newthuoc["new_dinhmuc_khonghoanlai"] = new Money(TongDMDTKHL + (pdkthuoc.Contains("new_dinhmuc_khonghoanlai") ? ((Money)pdkthuoc["new_dinhmuc_khonghoanlai"]).Value : 0));
                    newthuoc["new_dinhmuc_hoanlai_vattu"] = new Money(TongDMDTHLvattu + (pdkthuoc.Contains("new_dinhmuc_hoanlai_vattu") ? ((Money)pdkthuoc["new_dinhmuc_hoanlai_vattu"]).Value : 0));
                    newthuoc["new_dinhmuc_hoanlai_tienmat"] = new Money(TongDMDTHL + (pdkthuoc.Contains("new_dinhmuc_hoanlai_tienmat") ? ((Money)pdkthuoc["new_dinhmuc_hoanlai_tienmat"]).Value : 0));

                    newthuoc["new_giaingan_khonghoanlai"] = new Money(daGiainganKHL + (pdkthuoc.Contains("new_giaingan_khonghoanlai") ? ((Money)pdkthuoc["new_giaingan_khonghoanlai"]).Value : 0));
                    newthuoc["new_giaingan_hoanlai_vattu"] = new Money(daGiainganHLvattu + (pdkthuoc.Contains("new_giaingan_hoanlai_vattu") ? ((Money)pdkthuoc["new_giaingan_hoanlai_vattu"]).Value : 0));
                    newthuoc["new_giaingan_hoanlai_tienmat"] = new Money(0);

                    newthuoc["new_denghi_khonghoanlai"] = new Money(DenghiKHL + (pdkthuoc.Contains("new_denghi_khonghoanlai") ? ((Money)pdkthuoc["new_denghi_khonghoanlai"]).Value : 0));
                    newthuoc["new_denghi_hoanlai_vattu"] = new Money(DenghiHLvattu + (pdkthuoc.Contains("new_denghi_hoanlai_vattu") ? ((Money)pdkthuoc["new_denghi_hoanlai_vattu"]).Value : 0));

                    service.Update(newthuoc);
                }
            }
            #endregion
        }
        public static EntityCollection FindCSDTtrongmia(IOrganizationService crmservices, DateTime ngayapdung, Entity Vudt)
        {
            string fetchXml =
                   @"<fetch version = '1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name = 'new_chinhsachdautu' >
                        <attribute name='new_name' />
                        <attribute name ='new_vudautu' />
                        <attribute name='new_ngayapdung' />
                        <attribute name ='new_mucdichdautu' />
                        <attribute name='new_loaihopdong' />
                        <attribute name ='new_dinhmucdautukhonghoanlai' />
                        <attribute name='new_dinhmucdautuhoanlai' />
                        <attribute name ='new_machinhsach' />
                        <attribute name='new_chinhsachdautuid' />
                        <order attribute ='new_ngayapdung' descending='true' />
                        <filter type = 'and' >
                           <condition attribute='statecode' operator='eq' value='0' />
                           <condition attribute='new_loaihopdong' operator='eq' value= '100000000' /> 
                           <condition attribute='new_mucdichdautu' operator='eq' value= '100000000' /> 
                           <condition attribute='new_ngayapdung' operator='on-or-before' value= '{0}' /> 
                           <condition attribute='new_vudautu' operator='eq' uitype= 'new_vudautu' value= '{1}' /> 
                         </filter >
                       </entity >
                     </fetch >";

            fetchXml = string.Format(fetchXml, ngayapdung, Vudt.Id);

            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }
        public static EntityCollection FindDaututienmat(IOrganizationService crmservices, Entity CSDT)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_dinhmucdautu'>
                    <attribute name='new_name' />
                    <attribute name='new_sotien' />
                    <attribute name='new_langiaingan' />
                    <attribute name='new_phantramtilegiaingan' />
                    <attribute name='new_dinhmucdautuid' />
                    <attribute name='new_yeucau' />
                    <attribute name='new_tyleyc' />
                    <attribute name='new_sotienyc' />
                    <attribute name='new_phanbontoithieuyc' />
                    <attribute name='new_phanbontoithieubt' />
                    <order attribute='new_langiaingan' descending='false' />
                    <filter type='and'>
                      <condition attribute='new_chinhsachdautu' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, CSDT.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindchitietPDKhomgiong(IOrganizationService crmservices, Entity PDKhomgiong)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_chitietdangkyhomgiong'>
                    <attribute name='new_name' />
                    <attribute name='new_thanhtien' />
                    <attribute name='new_soluong' />
                    <attribute name='new_phieudangkyhomgiong' />
                    <attribute name='new_giongmia' />
                    <attribute name='new_dongia' />
                    <attribute name='new_chitietdangkyhomgiongid' />
                    <attribute name='new_giaingan_khonghoanlai' />
                    <attribute name='new_giaingan_hoanlai_vattu' />
                    <attribute name='new_giaingan_hoanlai_tienmat' />
                    <attribute name='new_dinhmuc_khonghoanlai' />
                    <attribute name='new_dinhmuc_hoanlai_vattu' />
                    <attribute name='new_dinhmuc_hoanlai_tienmat' />
                    <attribute name='new_denghi_khonghoanlai' />
                    <attribute name='new_denghi_hoanlai_vattu' />
                    <attribute name='new_denghi_hoanlai_tienmat' />
                    <attribute name='createdon' />
                    <order attribute='createdon' descending='false' />
                    <filter type='and'>
                      <condition attribute='new_phieudangkyhomgiong' operator='eq' uitype='new_phieudangkyhomgiong' value='{0}' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, PDKhomgiong.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }
        public static EntityCollection FindPDKPhanbonKHCN(IOrganizationService crmservices, Entity KHCN, Entity HD)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_phieudangkyphanbon'>
                    <attribute name='new_name' />
                    <attribute name='new_masophieudangky' />
                    <attribute name='statuscode' />
                    <attribute name='new_ngaylapphieu' />
                    <attribute name='new_ngaydukienthuchien' />
                    <attribute name='new_hopdongdautumia' />
                    <attribute name='createdon' />
                    <attribute name='new_phieudangkyphanbonid' />
                    <order attribute='createdon' descending='true' />
                    <filter type='and'>
                      <condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='{0}' />
                      <condition attribute='new_khachhang' operator='eq' uitype='contact' value='{1}' />
                      <condition attribute='new_tinhtrangduyet' operator='ne' value='100000002' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, HD.Id, KHCN.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindPDKPhanbonKHDN(IOrganizationService crmservices, Entity KHDN, Entity HD)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_phieudangkyphanbon'>
                    <attribute name='new_name' />
                    <attribute name='new_masophieudangky' />
                    <attribute name='statuscode' />
                    <attribute name='new_ngaylapphieu' />
                    <attribute name='new_ngaydukienthuchien' />
                    <attribute name='new_hopdongdautumia' />
                    <attribute name='createdon' />
                    <attribute name='new_phieudangkyphanbonid' />
                    <order attribute='createdon' descending='true' />
                    <filter type='and'>
                      <condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='{0}' />
                      <condition attribute='new_khachhangdoanhnghiep' operator='eq' uitype='account' value='{1}' />
                      <condition attribute='new_tinhtrangduyet' operator='ne' value='100000002' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, HD.Id, KHDN.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }
        public static EntityCollection FindchitietPDKPhanbon(IOrganizationService crmservices, Entity PDKPhanbon)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_chitietdangkyphanbon'>
                    <attribute name='new_name' />
                    <attribute name='new_soluong' />
                    <attribute name='new_phieudangkyphanbon' />
                    <attribute name='new_phanbon' />
                    <attribute name='new_thanhtien' />
                    <attribute name='new_dongia' />
                    <attribute name='new_chitietdangkyphanbonid' />
                    <order attribute='new_name' descending='false' />
                    <filter type='and'>
                      <condition attribute='new_phieudangkyphanbon' operator='eq' uitype='new_phieudangkyphanbon' value='{0}' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, PDKPhanbon.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }
        public static EntityCollection FindPDKThuocKHCN(IOrganizationService crmservices, Entity KHCN, Entity HD)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_phieudangkythuoc'>
                    <attribute name='new_name' />
                    <attribute name='statuscode' />
                    <attribute name='new_tramnongvu' />
                    <attribute name='new_ngaylapphieu' />
                    <attribute name='new_masophieudangky' />
                    <attribute name='new_khachhangdoanhnghiep' />
                    <attribute name='new_khachhang' />
                    <attribute name='new_hopdongdautumia' />
                    <attribute name='createdon' />
                    <attribute name='new_phieudangkythuocid' />
                    <order attribute='createdon' descending='true' />
                    <filter type='and'>
                      <condition attribute='new_khachhang' operator='eq' uitype='contact' value='{0}' />
                      <condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='{1}' />
                      <condition attribute='new_tinhtrangduyet' operator='ne' value='100000002' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, KHCN.Id, HD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindPDKThuocKHDN(IOrganizationService crmservices, Entity KHDN, Entity HD)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_phieudangkythuoc'>
                    <attribute name='new_name' />
                    <attribute name='statuscode' />
                    <attribute name='new_tramnongvu' />
                    <attribute name='new_ngaylapphieu' />
                    <attribute name='new_masophieudangky' />
                    <attribute name='new_khachhangdoanhnghiep' />
                    <attribute name='new_khachhang' />
                    <attribute name='new_hopdongdautumia' />
                    <attribute name='createdon' />
                    <attribute name='new_phieudangkythuocid' />
                    <order attribute='createdon' descending='true' />
                    <filter type='and'>
                      <condition attribute='new_khachhangdoanhnghiep' operator='eq' uitype='account' value='{0}' />
                      <condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='{1}' />
                      <condition attribute='new_tinhtrangduyet' operator='ne' value='100000002' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, KHDN.Id, HD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindchitietPDKThuoc(IOrganizationService crmservices, Entity PDKThuoc)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_chitietdangkythuoc'>
                    <attribute name='new_name' />
                    <attribute name='new_thuoc' />
                    <attribute name='new_thanhtien' />
                    <attribute name='new_soluong' />
                    <attribute name='new_phieudangkythuoc' />
                    <attribute name='new_dongia' />
                    <attribute name='new_chitietdangkythuocid' />
                    <order attribute='new_name' descending='false' />
                    <filter type='and'>
                      <condition attribute='new_phieudangkythuoc' operator='eq' uitype='new_phieudangkythuoc' value='{0}' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, PDKThuoc.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindPDKVattukhacKHCN(IOrganizationService crmservices, Entity KHCN, Entity HD)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_phieudangkyvattu'>
                    <attribute name='new_name' />
                    <attribute name='new_ngaydukienthuchien' />
                    <attribute name='new_masophieudangky' />
                    <attribute name='statuscode' />
                    <attribute name='new_ngaylapphieu' />
                    <attribute name='new_hopdongdautumia' />
                    <attribute name='createdon' />
                    <attribute name='new_phieudangkyvattuid' />
                    <order attribute='createdon' descending='true' />
                    <filter type='and'>
                      <condition attribute='new_khachhang' operator='eq' uitype='contact' value='{0}' />
                      <condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='{1}' />
                      <condition attribute='new_tinhtrangduyet' operator='ne' value='100000002' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, KHCN.Id, HD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindPDKVattukhacKHDN(IOrganizationService crmservices, Entity KHDN, Entity HD)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_phieudangkyvattu'>
                    <attribute name='new_name' />
                    <attribute name='new_ngaydukienthuchien' />
                    <attribute name='new_masophieudangky' />
                    <attribute name='statuscode' />
                    <attribute name='new_ngaylapphieu' />
                    <attribute name='new_hopdongdautumia' />
                    <attribute name='createdon' />
                    <attribute name='new_phieudangkyvattuid' />
                    <order attribute='createdon' descending='true' />
                    <filter type='and'>
                      <condition attribute='new_khachhangdoanhnghiep' operator='eq' uitype='account' value='{0}' />
                      <condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='{1}' />
                      <condition attribute='new_tinhtrangduyet' operator='ne' value='100000002' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, KHDN.Id, HD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindPDKHomgiongKHCN(IOrganizationService crmservices, Entity KHCN, Entity HD)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_phieudangkyhomgiong'>
                    <attribute name='new_name' />
                    <attribute name='new_masophieudangky' />
                    <attribute name='new_ngaylapphieu' />
                    <attribute name='new_khachhangdoanhnghiep' />
                    <attribute name='new_khachhang' />
                    <attribute name='new_hopdongdautumia' />
                    <attribute name='statuscode' />
                    <attribute name='new_canbonongvu' />
                    <attribute name='createdon' />
                    <attribute name='new_phieudangkyhomgiongid' />
                    <order attribute='createdon' descending='true' />
                    <filter type='and'>
                      <condition attribute='new_khachhang' operator='eq' uitype='contact' value='{0}' />
                      <condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='{1}' />
                      <condition attribute='new_tinhtrangduyet' operator='ne' value='100000002' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, KHCN.Id, HD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindPDKHomgiongKHDN(IOrganizationService crmservices, Entity KHDN, Entity HD)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_phieudangkyhomgiong'>
                    <attribute name='new_name' />
                    <attribute name='new_masophieudangky' />
                    <attribute name='new_ngaylapphieu' />
                    <attribute name='new_khachhangdoanhnghiep' />
                    <attribute name='new_khachhang' />
                    <attribute name='new_hopdongdautumia' />
                    <attribute name='statuscode' />
                    <attribute name='new_canbonongvu' />
                    <attribute name='createdon' />
                    <attribute name='new_phieudangkyhomgiongid' />
                    <order attribute='createdon' descending='true' />
                    <filter type='and'>
                      <condition attribute='new_khachhangdoanhnghiep' operator='eq' uitype='account' value='{0}' />
                      <condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='{1}' />
                      <condition attribute='new_tinhtrangduyet' operator='ne' value='100000002' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, KHDN.Id, HD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindchitietPDKVattukhac(IOrganizationService crmservices, Entity PDKVattukhac)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_chitietdangkyvattu'>
                    <attribute name='new_name' />
                    <attribute name='new_soluong' />
                    <attribute name='new_vattu' />
                    <attribute name='new_phieudangkyvattu' />
                    <attribute name='new_nguongoc' />
                    <attribute name='new_donvitinh' />
                    <attribute name='new_chitietdangkyvattuid' />
                    <order attribute='new_name' descending='false' />
                    <filter type='and'>
                      <condition attribute='new_phieudangkyvattu' operator='eq' uitype='new_phieudangkyvattu' value='{0}' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, PDKVattukhac.Id);
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
