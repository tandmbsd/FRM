﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Activities;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Crm.Sdk.Messages;

namespace Plugin_ThemCSDTvaoPDNTamUng
{
    public sealed class Plugin_ThemCSDTvaoPDNTamUng : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            if (context.InputParameters.Contains("Target") && (context.InputParameters["Target"] is Entity))
            {
                //throw new Exception("chay plugin");

                Entity ChiTietPDNTamUng = (Entity)context.InputParameters["Target"];
                Guid entityId = ChiTietPDNTamUng.Id;

                if (ChiTietPDNTamUng.LogicalName == "new_chitietphieudenghitamung")
                {
                    //traceService.Trace("Begin plugin");
                    if (context.MessageName.ToUpper() == "CREATE")
                    {
                        traceService.Trace("chay plugin");

                        ChiTietPDNTamUng = service.Retrieve("new_chitietphieudenghitamung", entityId, new ColumnSet(true));
                        DateTime ngaytao = ChiTietPDNTamUng.GetAttributeValue<DateTime>("createdon");

                        Entity mCSDT = new Entity();
                        Entity en = new Entity(ChiTietPDNTamUng.LogicalName);
                        en.Id = ChiTietPDNTamUng.Id;

                        if (ChiTietPDNTamUng.Contains("new_loaihopdong") && ChiTietPDNTamUng.GetAttributeValue<OptionSetValue>("new_loaihopdong").Value.ToString() == "100000000")  // HDDT Mía
                        {
                            traceService.Trace("Loai HD: HDDT mia");
                            if (ChiTietPDNTamUng.Contains("new_hopdongdautumia") && ChiTietPDNTamUng.Contains("new_chitiethddtmia"))
                            {
                                traceService.Trace("có HDDT và chi tiet ");
                                EntityReference HDDTmiaRef = ChiTietPDNTamUng.GetAttributeValue<EntityReference>("new_hopdongdautumia");
                                Guid HDDTmiaId = HDDTmiaRef.Id;
                                Entity HDDTmia = service.Retrieve("new_hopdongdautumia", HDDTmiaId, new ColumnSet(new string[] { "new_vudautu", "new_tongdinhmucdautu" }));

                                EntityReference chitietHDDTRef = ChiTietPDNTamUng.GetAttributeValue<EntityReference>("new_chitiethddtmia");
                                Guid chitietDHDTId = chitietHDDTRef.Id;
                                Entity ChiTietHD = service.Retrieve("new_thuadatcanhtac", chitietDHDTId,
                                    new ColumnSet(new string[] { "new_vutrong", "new_loaisohuudat",
                                        "new_loaigocmia", "new_mucdichsanxuatmia", "new_giongmia", "new_thuadat",
                                        "createdon", "new_hopdongdautumia", "new_khachhang", "new_khachhangdoanhnghiep",
                                        "new_thamgiamohinhkhuyennong", "new_tuoimia","new_dientichconlai" }));

                                //DateTime ngaytao = ChiTietHD.GetAttributeValue<DateTime>("createdon");
                                List<Entity> lstLenhdon = RetrieveMultiRecord(service, "new_lenhdon",
                                    new ColumnSet(new string[] {"new_lenhdoncuoi"}), "new_thuacanhtac", chitietDHDTId);

                                foreach (Entity a in lstLenhdon)
                                {
                                    if(a.Contains("new_lenhdoncuoi") && (bool)a["new_lenhdoncuoi"] == true)
                                        throw  new Exception("Thửa đất đã có lệnh đốn cuối");
                                }

                                EntityReference thuadatEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_thuadat");
                                Guid thuadatId = thuadatEntityRef.Id;
                                Entity thuadatObj = service.Retrieve("new_thuadat", thuadatId, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy" , "new_diachi" }));

                                EntityReference giongmiaEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_giongmia");
                                Guid giongmiaId = giongmiaEntityRef.Id;
                                Entity giongmiaObj = service.Retrieve("new_giongmia", giongmiaId, new ColumnSet(new string[] { "new_nhomgiong", "new_name" }));

                                EntityReference vudautuRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");

                                decimal dientichconlai = ChiTietHD.Contains("new_dientichconlai")
                                    ? (decimal) ChiTietHD["new_dientichconlai"]
                                    : 0;

                                if (vudautuRef == null || vudautuRef.Id == Guid.Empty)
                                {
                                    throw new InvalidPluginExecutionException("Trong HĐĐT mía chưa có Vụ đầu tư !");
                                }
                                else
                                {
                                    traceService.Trace("có VU DT ");
                                    Guid vuDTId = vudautuRef.Id;
                                    EntityCollection resultCol = FindCSDTtrongmia(service, ChiTietPDNTamUng);

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
                                            traceService.Trace("Xong vu trong");

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
                                            traceService.Trace("Xong loai goc mia");

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

                                            traceService.Trace("new_mucdichsanxuatmia_vl");

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

                                            traceService.Trace("Pass new_nhomdat");

                                            if (a.Contains("new_loaisohuudat_vl"))  // Loai chu so huu
                                            {
                                                if (ChiTietHD.Attributes.Contains("new_loaisohuudat"))
                                                {
                                                    if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_loaisohuudat"]).Value.ToString()) == -1)
                                                        continue;
                                                }
                                                else
                                                {
                                                    continue;
                                                }
                                            }

                                            traceService.Trace("Pass loai CSH");

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

                                            traceService.Trace("Pass new_nhomgiong");

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

                                            traceService.Trace("Pass nhom kh");

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

                                            traceService.Trace("Pass vung DL");

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

                                            traceService.Trace("Pass giong mia");

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

                                            traceService.Trace("Pass KKPT");

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

                                            traceService.Trace("Pass nhom cu ly");

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

                                            traceService.Trace("Pass MH khuyen nong");

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

                                            traceService.Trace("Pass nhom nang suat");

                                            mCSDT = a;
                                            break;
                                        }

                                        if (mCSDT != null && mCSDT.Id != Guid.Empty)
                                        {

                                            traceService.Trace("Tim duoc CSDT tam ung");
                                            traceService.Trace("1");

                                            // ------Gan vao Chi tiet Tam ung
                                            EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                                            en.Attributes.Add("new_chinhsachdautu", csdtRef);
                                            traceService.Trace("2");
                                            // Gan Tạm  ứng %
                                            Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", mCSDT.Id,
                                                new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name" }));

                                            decimal tamung = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? ((Money)csdtKQEntity["new_dinhmucdautuhoanlai"]).Value : 0);
                                            traceService.Trace("3");
                                            en["new_dongiadinhmucung"] = new Money(tamung);
                                            en["new_dientichconlai"] =dientichconlai;
                                            traceService.Trace("4");
                                            traceService.Trace("gan xonf " + tamung);

                                            // Tinh Đinh muc Tạm  ứng
                                            //decimal tongTienDT = (HDDTmia.Contains("new_tongtiendautu") ? HDDTmia.GetAttributeValue<Money>("new_tongtiendautu").Value : 0) * tamung;
                                            //Money MtongTienDT = new Money(tongTienDT);

                                            //en["new_dinhmuctamung"] = MtongTienDT;

                                            service.Update(en);

                                        }
                                        else
                                        {
                                            throw new InvalidPluginExecutionException("Không tìm thấy Chính sách Tạm Ứng phù hợp");
                                        }
                                    } // nếu có list CSDT
                                    else
                                        throw new InvalidPluginExecutionException("Không có Chính sách Tạm Ứng cho vụ đầu tư này");
                            } // else vu dau tu
                            }
                            else
                            {
                                throw new InvalidPluginExecutionException("Vui lòng nhập HDDT mía và chi tiết HDDT mía");
                            }
                        }

                        if (ChiTietPDNTamUng.Contains("new_loaihopdong") && ChiTietPDNTamUng.GetAttributeValue<OptionSetValue>("new_loaihopdong").Value.ToString() == "100000001")  // // HDDT thuê đất
                        {
                            if (ChiTietPDNTamUng.Contains("new_hopdongdaututhuedat") && ChiTietPDNTamUng.Contains("new_chitiethddtthuedat"))
                            {
                                EntityReference HDDTtdRef = ChiTietPDNTamUng.GetAttributeValue<EntityReference>("new_hopdongdaututhuedat");
                                Guid DHDTtdId = HDDTtdRef.Id;
                                Entity HDDTThuedat = service.Retrieve("new_hopdongthuedat", DHDTtdId, new ColumnSet(new string[] { "new_vudautu", "new_tongdinhmucdautu" }));

                                EntityReference chitietHDDTtdRef = ChiTietPDNTamUng.GetAttributeValue<EntityReference>("new_chitiethddtthuedat");
                                Guid chitietDHDTtdId = chitietHDDTtdRef.Id;
                                Entity ChiTietHDThueDat = service.Retrieve("new_datthue", chitietDHDTtdId,
                                    new ColumnSet(new string[] { "createdon", "new_hopdongthuedat", "new_benchothuedatkh", "new_benchothuedatkhdn", "new_dientichthucthue" }));

                                EntityReference vuDTRef = HDDTThuedat.GetAttributeValue<EntityReference>("new_vudautu");

                                if (vuDTRef == null || vuDTRef.Id == Guid.Empty)
                                {
                                    throw new InvalidPluginExecutionException("Trong HĐ thuê đất chưa có Vụ đầu tư !");
                                }
                                else
                                {
                                    Guid vuDTId = vuDTRef.Id;

                                    EntityCollection result = FindCSDTthuedat(service, ChiTietPDNTamUng);

                                    if (result != null && result.Entities.Count > 0)
                                    {
                                        foreach (Entity a in result.Entities)
                                        {

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
                                                    if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
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
                                                    if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                                    {
                                                        co = true;
                                                    }
                                                }
                                            }

                                            if (co == false)
                                                continue;
                                            traceService.Trace("Pass DK nhom KH");

                                            // NHom nang suat
                                            co = false;

                                            if (ChiTietHDThueDat.Attributes.Contains("new_benchothuedatkh"))
                                            {
                                                EntityReference khEntityRef = ChiTietHDThueDat.GetAttributeValue<EntityReference>("new_benchothuedatkh");
                                                Guid khId = khEntityRef.Id;
                                                Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nangsuatbinhquan" }));

                                                EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

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
                                            if (ChiTietHDThueDat.Attributes.Contains("new_benchothuedatkhdn"))
                                            {
                                                Guid khId = ChiTietHDThueDat.GetAttributeValue<EntityReference>("new_benchothuedatkhdn").Id;
                                                Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));

                                                EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

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
                                            traceService.Trace("DK nhom KH");

                                            mCSDT = a;
                                            break;
                                        }

                                        if (mCSDT != null && mCSDT.Id != Guid.Empty)
                                        {
                                            // ------Gan vao Chi tiet Tam ung
                                            EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                                            ChiTietPDNTamUng.Attributes.Add("new_chinhsachdautu", csdtRef);

                                            // Gan Tạm  ứng %
                                            Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", mCSDT.Id,
                                                new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name" }));

                                            decimal tamung = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? ((Money)csdtKQEntity["new_dinhmucdautuhoanlai"]).Value : 0);

                                            en["new_dongiadinhmucung"] = tamung;
                                           

                                            // Tinh Đinh muc Tạm  ứng
                                            //decimal tongDMDT = (HDDTThuedat.Contains("new_tongtienthuedat") ? HDDTThuedat.GetAttributeValue<Money>("new_tongtienthuedat").Value : 0) * tamung;
                                            //Money MtongDMDT = new Money(tongDMDT);

                                            //en["new_dinhmuctamung"] = MtongDMDT;

                                            service.Update(en);
                                        }
                                        else
                                        {
                                            throw new InvalidPluginExecutionException("Không tìm thấy Chính sách Ứng phù hợp");
                                        }
                                    } // neu list ds CSDT > 0
                                }
                            }
                            else
                            {
                                throw new InvalidPluginExecutionException("Vui lòng nhập HDDT thuê đất và chi tiết HDDT thuê đất");
                            }
                        }

                        if (ChiTietPDNTamUng.Contains("new_loaihopdong") && ChiTietPDNTamUng.GetAttributeValue<OptionSetValue>("new_loaihopdong").Value.ToString() == "100000004")  // HDDT trang thiet bị
                        {
                            if (ChiTietPDNTamUng.Contains("new_hopdongdaututrangthietbi") && ChiTietPDNTamUng.Contains("new_chitiethddttrangthietbi"))
                            {
                                EntityReference HDDTttbRef = ChiTietPDNTamUng.GetAttributeValue<EntityReference>("new_hopdongdaututrangthietbi");
                                Guid DHDTttbId = HDDTttbRef.Id;
                                Entity hddtTTBObj = service.Retrieve("new_hopdongdaututrangthietbi", DHDTttbId, new ColumnSet(new string[] { "new_vudautu", "new_tonggiatrihopdong", "new_doitaccungcap", "new_doitaccungcapkhdn" }));

                                EntityReference chitietHDDTttbRef = ChiTietPDNTamUng.GetAttributeValue<EntityReference>("new_chitiethddttrangthietbi");
                                Guid chitietDHDTttbId = chitietHDDTttbRef.Id;
                                Entity ChiTietHDDTTrangThietBi = service.Retrieve("new_hopdongdaututrangthietbichitiet", chitietDHDTttbId, new ColumnSet(new string[] { "new_maymocthietbi", "createdon", "new_name", "new_giatrihopdong", "new_hopdongdaututrangthietbi" }));

                                EntityReference vudautuTTBRef = hddtTTBObj.GetAttributeValue<EntityReference>("new_vudautu");
                                if (vudautuTTBRef == null || vudautuTTBRef.Id == Guid.Empty)
                                {
                                    throw new InvalidPluginExecutionException("Trong HĐ thuê đất chưa có Vụ đầu tư !");
                                }
                                else
                                {
                                    Guid vuDTId = vudautuTTBRef.Id;

                                    EntityCollection resultTTB = FindCSDTtrangthietbi(service, ChiTietPDNTamUng);

                                    if (resultTTB != null && resultTTB.Entities.Count > 0)
                                    {
                                        foreach (Entity a in resultTTB.Entities)
                                        {
                                            // NHom khach hang
                                            bool co = false;

                                            EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                            if (hddtTTBObj.Attributes.Contains("new_doitaccungcap"))
                                            {
                                                Guid khId = hddtTTBObj.GetAttributeValue<EntityReference>("new_doitaccungcap").Id;
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
                                            if (hddtTTBObj.Attributes.Contains("new_doitaccungcapkhdn"))
                                            {
                                                Guid khId = hddtTTBObj.GetAttributeValue<EntityReference>("new_doitaccungcapkhdn").Id;
                                                Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nhomkhachhang", "new_nangsuatbinhquan" }));

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
                                                    if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                                    {
                                                        co = true;
                                                    }
                                                }
                                            }

                                            if (co == false)
                                                continue;

                                            // NHom nang suat
                                            co = false;

                                            EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);
                                            if (hddtTTBObj.Attributes.Contains("new_doitaccungcap"))
                                            {
                                                Guid khId = hddtTTBObj.GetAttributeValue<EntityReference>("new_doitaccungcap").Id;
                                                Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang", "new_nangsuatbinhquan" }));

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

                                            if (hddtTTBObj.Attributes.Contains("new_doitaccungcapkhdn"))
                                            {
                                                Guid khId = hddtTTBObj.GetAttributeValue<EntityReference>("new_doitaccungcapkhdn").Id;
                                                Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));

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

                                        if (mCSDT != null && mCSDT.Id != Guid.Empty)
                                        {
                                            // ------Gan vao Chi tiet Tam ung
                                            EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                                            ChiTietPDNTamUng.Attributes.Add("new_chinhsachdautu", csdtRef);

                                            // Gan Tạm  ứng %
                                            Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", mCSDT.Id,
                                                new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name" }));

                                            decimal tamung = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? ((Money)csdtKQEntity["new_dinhmucdautuhoanlai"]).Value : 0);

                                            en["new_dongiadinhmucung"] = tamung;

                                            // Tinh Đinh muc Tạm  ứng
                                            //decimal tongDMDT = (hddtTTBObj.Contains("new_tonggiatrihopdong") ? hddtTTBObj.GetAttributeValue<Money>("new_tonggiatrihopdong").Value : 0) * tamung;
                                            //Money MtongDMDT = new Money(tongDMDT);
                                            //en["new_dinhmuctamung"] = MtongDMDT;

                                            service.Update(en);
                                        }
                                        else
                                        {
                                            throw new InvalidPluginExecutionException("Không tìm thấy Chính sách Ứng phù hợp");
                                        }
                                    }
                                }
                            }
                            else
                            {
                                throw new InvalidPluginExecutionException("Vui lòng nhập HDDT trang thiết bị và chi tiết HDDT trang thiết bị");
                            }
                        }
                    }
                }
            }
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

        public static EntityCollection FindCSDTtrongmia(IOrganizationService crmservices, Entity ctPDNTU)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctPDNTU["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctPDNTU["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000004));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }

        public static EntityCollection FindCSDTthuedat(IOrganizationService crmservices, Entity ctPDNTU)
        {
            Entity HD = crmservices.Retrieve("new_hopdongthuedat", ((EntityReference)ctPDNTU["new_hopdongdaututhuedat"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctPDNTU["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000001));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000004));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }

        public static EntityCollection FindCSDTtrangthietbi(IOrganizationService crmservices, Entity ctPDNTU)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdaututrangthietbi", ((EntityReference)ctPDNTU["new_hopdongdaututrangthietbi"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctPDNTU["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000002));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000004));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
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
