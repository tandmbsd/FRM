using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Activities;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;

namespace Plugin_ThemCSDTvaoChiTietHDDTMia
{
    public sealed class ThemCSDTvaoChiTietHDDTmiaUpdate : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            //throw new Exception("chay plugin");
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            traceService.Trace(string.Format("Context Depth {0}", context.Depth));
            if (context.Depth > 1)
                return;

            if (context.InputParameters.Contains("Target") && (context.InputParameters["Target"] is Entity))
            {
                traceService.Trace("Truoc id target");
                Entity ChiTietHD = (Entity)context.InputParameters["Target"];
                Guid entityId = ChiTietHD.Id;

                if (ChiTietHD.LogicalName == "new_thuadatcanhtac")
                {                    
                    traceService.Trace("Begin plugin");
                    if (context.MessageName.ToUpper() == "UPDATE")
                    {
                        if (ChiTietHD.Contains("new_vutrong") || ChiTietHD.Contains("new_luugoc") || ChiTietHD.Contains("new_loaisohuudat") || ChiTietHD.Contains("new_loaigocmia") || ChiTietHD.Contains("new_mucdichsanxuatmia") || ChiTietHD.Contains("new_hopdongdautumia") || ChiTietHD.Contains("new_giongmia") || ChiTietHD.Contains("new_khachhang") || ChiTietHD.Contains("new_khachhangdoanhnghiep") || ChiTietHD.Contains("new_thamgiamohinhkhuyennong"))
                        {                 
                            ChiTietHD = service.Retrieve("new_thuadatcanhtac", entityId, new ColumnSet(new string[] { "new_vutrong", "new_loaisohuudat", "new_loaigocmia", "new_mucdichsanxuatmia", "new_giongmia", "new_thuadat", "createdon", "new_hopdongdautumia", "new_khachhang", "new_khachhangdoanhnghiep", "new_thamgiamohinhkhuyennong", "new_dientichthucte", "new_tuoimia", "new_dientichhopdong", "new_dinhmucphanbontoithieu", "new_chinhsachdautu", "new_copytuhddtthuedat", "new_luugoc", "new_dautuhoanlai" }));
                            DateTime ngaytao = ChiTietHD.GetAttributeValue<DateTime>("createdon");
                            //throw new Exception("chay plugin");
                            if (!ChiTietHD.Contains("new_hopdongdautumia") || !ChiTietHD.Contains("new_giongmia") || !ChiTietHD.Contains("new_thuadat"))
                            {
                                throw new InvalidPluginExecutionException("Thiếu thông tin bắt buộc về thửa đất / giống mía / vụ đầu tư");
                            }
                            else
                            {
                                EntityReference thuadatEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_thuadat");
                                Guid thuadatId = thuadatEntityRef.Id;
                                Entity thuadatObj = service.Retrieve("new_thuadat", thuadatId, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy", "new_diachi" }));

                                EntityReference giongmiaEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_giongmia");
                                Guid giongmiaId = giongmiaEntityRef.Id;
                                Entity giongmiaObj = service.Retrieve("new_giongmia", giongmiaId, new ColumnSet(new string[] { "new_nhomgiong", "new_name" }));

                                EntityReference HDDTmiaRef = ChiTietHD.GetAttributeValue<EntityReference>("new_hopdongdautumia");
                                Guid DHDTmiaId = HDDTmiaRef.Id;
                                Entity HDDTmia = service.Retrieve("new_hopdongdautumia", DHDTmiaId, new ColumnSet(new string[] { "new_vudautu" }));

                                EntityReference vudautuRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");
                                
                                Guid vuDTId = vudautuRef.Id;
                                EntityCollection resultCol = FindCSDTtrongmia(service, ChiTietHD);
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

                                        //throw new Exception(a["new_loaisohuudat_vl"].ToString() + "-" + ((OptionSetValue)ChiTietHD["new_loaisohuudat"]).Value.ToString());

                                        //if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_loaisohuudat"]).Value.ToString()) == -1)
                                        //{
                                        //    throw new Exception("ok");
                                        //}

                                        if (a.Contains("new_loaisohuudat_vl"))  // Loai chu so huu
                                        {
                                            traceService.Trace(a["new_loaisohuudat_vl"].ToString());
                                            traceService.Trace(((OptionSetValue)ChiTietHD["new_loaisohuudat"]).Value.ToString());
                                            if (ChiTietHD.Attributes.Contains("new_loaisohuudat"))
                                            {
                                                if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_loaisohuudat"]).Value.ToString()) == -1)
                                                    continue;
                                            }
                                            else
                                                continue;
                                        }
                                        traceService.Trace("Begin new_nhomgiongmia_vl");
                                        if (a.Contains("new_nhomgiongmia_vl"))  // Nhom giong mia
                                        {
                                            traceService.Trace("1");
                                            if (giongmiaObj.Attributes.Contains("new_nhomgiong"))
                                            {
                                                traceService.Trace("2");
                                                if (a["new_nhomgiongmia_vl"].ToString().IndexOf(((OptionSetValue)giongmiaObj["new_nhomgiong"]).Value.ToString()) == -1)
                                                    continue;
                                            } 
                                            else
                                                continue;
                                        }

                                        //if (ChiTietHD.Contains("new_loaigocmia") && ChiTietHD.GetAttributeValue<OptionSetValue>("new_loaigocmia").Value.ToString() == "100000001")
                                        //{

                                        //throw new Exception("chay plugin toi Luu goc");
                                        if (a.Contains("new_luugoc"))  // Luu goc
                                        {
                                            if (ChiTietHD.Attributes.Contains("new_luugoc"))
                                            {
                                                traceService.Trace(((OptionSetValue)a["new_luugoc"]).Value.ToString() + " - " + ((OptionSetValue)ChiTietHD["new_luugoc"]).Value.ToString());

                                                if (((OptionSetValue)a["new_luugoc"]).Value != ((OptionSetValue)ChiTietHD["new_luugoc"]).Value)
                                                    continue;
                                            }
                                            else
                                                continue;
                                        }
                                        //}

                                        // NHom khach hang
                                        bool co = false;
                                        //throw new Exception(a["new_vutrong_vl"].ToString() + "-" + a["new_loaigocmia_vl"].ToString() + "-" + a["new_mucdichsanxuatmia_vl"].ToString()
                                        //    + "-" + a["new_nhomdat_vl"].ToString() + "-" + a["new_loaisohuudat_vl"].ToString() + "-" + a["new_nhomgiongmia_vl"].ToString() + "-" + a["new_luugoc"].ToString());
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
                                                    if (co == true)
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
                                else
                                    throw new InvalidPluginExecutionException("Chưa có Chính sách Đầu tư trồng chăm sóc mía nào cho vụ đầu tư này");

                                if (mCSDT != null && mCSDT.Id != Guid.Empty)
                                {
                                    traceService.Trace("Tim duoc CSDT");
                                    // ------Gan vao Chi tiet HDDT mia
                                    EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                                    Entity en = new Entity(ChiTietHD.LogicalName);
                                    en.Id = ChiTietHD.Id;

                                    en["new_chinhsachdautu"] = csdtRef;

                                    traceService.Trace("Sau khi update CSDT");

                                    // Xóa ty le thu hoi von du kien cu
                                    EntityCollection oldlTLTHVDK = FindTLTHVDK(service, ChiTietHD);
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
                                        <attribute name='new_vuthuhoi' />
                                        <attribute name='new_chinhsachdautu' />
                                        <attribute name='new_chinhsachdautubosung' />
                                        <attribute name='new_tilethuhoivonid' />
                                        <order attribute='new_nam' descending='false' />
                                        <link-entity name='new_chinhsachdautu' from='new_chinhsachdautuid' to='new_chinhsachdautu' alias='ac'>
                                          <filter type='and'>
                                            <condition attribute='statecode' operator='eq' value='0' />
                                            <condition attribute='new_chinhsachdautuid' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                                          </filter>
                                        </link-entity>
                                      </entity>
                                    </fetch>";

                                    Guid csdtKQ = mCSDT.Id;

                                    traceService.Trace("Tim duoc ty le thu hoi von");

                                    fetchTLTHV = string.Format(fetchTLTHV, csdtKQ);
                                    EntityCollection collTLTHV = service.RetrieveMultiple(new FetchExpression(fetchTLTHV));
                                    List<Entity> listTLTHV = collTLTHV.Entities.ToList<Entity>();
                                    Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name", "new_loailaisuatcodinhthaydoi", "new_muclaisuatdautu", "new_cachtinhlai", "new_dinhmucdautukhonghoanlai", "new_dinhmucphanbontoithieu" }));
                                    traceService.Trace("Tim duoc ty le thu hoi von 1");
                                    foreach (Entity TLTHV in listTLTHV)
                                    {
                                        Entity tlthvdkHDCT = new Entity("new_tylethuhoivondukien");

                                        //EntityReference vudautuEntityRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");
                                        EntityReference hdctEntityRef = new EntityReference("new_thuadatcanhtac", entityId);
                                        traceService.Trace("Tim duoc ty le thu hoi von 2");

                                        if (TLTHV.Attributes.Contains("new_phantramtilethuhoi") && TLTHV.Attributes.Contains("new_vuthuhoi") && csdtKQEntity.Attributes.Contains("new_dinhmucdautuhoanlai"))
                                        {
                                            traceService.Trace("Tim duoc ty le thu hoi von 3");
                                            EntityReference vdtRef = TLTHV.GetAttributeValue<EntityReference>("new_vuthuhoi");
                                            traceService.Trace("Tim duoc ty le thu hoi von 4");
                                            Entity vdt = service.Retrieve("new_vudautu", vdtRef.Id, new ColumnSet(new string[] { "new_name" }));
                                            traceService.Trace("Tim duoc ty le thu hoi von 5");
                                            string tenvdt = vdt["new_name"].ToString();
                                            traceService.Trace("Tim duoc ty le thu hoi von 6");

                                            string tenTLTHVDK = "Tỷ lệ thu hồi " + tenvdt;
                                            decimal tyle = (TLTHV.Contains("new_phantramtilethuhoi") ? (decimal)TLTHV["new_phantramtilethuhoi"] : 0);
                                            //decimal dientichtt = (ChiTietHD.Contains("new_dientichthucte") ? (decimal)ChiTietHD["new_dientichthucte"] : 0);
                                            decimal sotienDTHL = (ChiTietHD.Contains("new_dinhmucdautuhoanlai") ? ChiTietHD.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value : 0);
                                            decimal sotien = 0;

                                            sotien = (sotienDTHL * tyle) / 100;

                                            Money sotienM = new Money(sotien);

                                            tlthvdkHDCT.Attributes.Add("new_name", tenTLTHVDK);
                                            tlthvdkHDCT.Attributes.Add("new_loaityle", new OptionSetValue(100000000));
                                            tlthvdkHDCT.Attributes.Add("new_chitiethddtmia", hdctEntityRef);
                                            tlthvdkHDCT.Attributes.Add("new_vudautu", vdtRef);
                                            tlthvdkHDCT.Attributes.Add("new_tylephantram", tyle);
                                            tlthvdkHDCT.Attributes.Add("new_sotienthuhoi", sotienM);

                                            service.Create(tlthvdkHDCT);
                                        }
                                    }
                                    // ------End Gan vao ty le thu hoi von du kien

                                    traceService.Trace("Gan xong ty le thu hoi von");

                                    //EntityReference vudautuRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");
                                    //Guid vuDTId = vudautuRef.Id;

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
                                            <order attribute='new_name' descending='false' />
                                            <filter type='and'>
                                              <condition attribute='statecode' operator='eq' value='0' />
                                              <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{0}' />
                                            </filter>
                                          </entity>
                                        </fetch>";

                                    fetchTSVDT = string.Format(fetchTSVDT, vuDTId);
                                    EntityCollection collTSVDT = service.RetrieveMultiple(new FetchExpression(fetchTSVDT));
                                    List<Entity> listTSVDT = collTSVDT.Entities.ToList<Entity>();

                                    // ------ Gan NHom du lieu Lai suat
                                    if (collTSVDT.Entities.Count > 0)
                                    {
                                        // Loai lai suat
                                        if (csdtKQEntity.Attributes.Contains("new_loailaisuatcodinhthaydoi"))
                                        {
                                            bool loails = csdtKQEntity.GetAttributeValue<bool>("new_loailaisuatcodinhthaydoi");

                                            if (loails == false) // ls thay doi
                                                en["new_loailaisuat"] = new OptionSetValue(100000001);
                                            else   // ls co dinh
                                                en["new_loailaisuat"] = new OptionSetValue(100000000);
                                        }

                                        // Muc lai suat

                                        if (csdtKQEntity.Attributes.Contains("new_loailaisuatcodinhthaydoi"))
                                        {
                                            bool loails = csdtKQEntity.GetAttributeValue<bool>("new_loailaisuatcodinhthaydoi");
                                            if (loails == false)   // ls thay doi
                                            {

                                            }
                                            else // ls co dinh
                                            {
                                                if (csdtKQEntity.Contains("new_muclaisuatdautu"))
                                                {
                                                    decimal mucls = (csdtKQEntity.Contains("new_muclaisuatdautu") ? (decimal)csdtKQEntity["new_muclaisuatdautu"] : 0);
                                                    en["new_laisuat"] = mucls;
                                                }
                                                else
                                                {
                                                    foreach (Entity TSVDT in listTSVDT)
                                                    {
                                                        if (TSVDT.GetAttributeValue<OptionSetValue>("new_loai").Value == 100000001) //100,000,001 : Loai ls
                                                        {
                                                            if (TSVDT.Attributes.Contains("new_giatri"))
                                                            {
                                                                decimal mucls = (TSVDT.Contains("new_giatri") ? TSVDT.GetAttributeValue<decimal>("new_giatri") : 0);
                                                                en["new_laisuat"] = mucls;

                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        if (csdtKQEntity.Attributes.Contains("new_cachtinhlai"))
                                        {
                                            OptionSetValue cachlinhlai = csdtKQEntity.GetAttributeValue<OptionSetValue>("new_cachtinhlai");
                                            en["new_cachtinhlai"] = cachlinhlai;
                                        }

                                        // ------ End nhom du lieu Gan Lai suat

                                        // -------- Gan nhom du lieu  Dinh muc

                                        foreach (Entity TSVDT in listTSVDT)       // Gia mia du kien
                                        {
                                            if (TSVDT.GetAttributeValue<OptionSetValue>("new_loai").Value == 100000004) //100,000,004 : Gia mia du kien
                                            {
                                                if (TSVDT.Attributes.Contains("new_giatien"))
                                                {
                                                    Money giamiadk = TSVDT.GetAttributeValue<Money>("new_giatien");
                                                    en.Attributes["new_giamiadukien"] = giamiadk;
                                                    break;
                                                }
                                            }
                                        }
                                    }

                                    decimal dongiabsKHL = 0;
                                    decimal dongiabsHL = 0;
                                    decimal dongiabsPB = 0;
                                    decimal dongiabsTM = 0;

                                    decimal dientichhd = (ChiTietHD.Contains("new_dientichhopdong") ? (decimal)ChiTietHD["new_dientichhopdong"] : 0);

                                    //EntityReferenceCollection OldlistCSDTBSRef = new EntityReferenceCollection();
                                    //EntityCollection OldlistCSDTBS = RetrieveNNRecord(service, "new_chinhsachdautuchitiet", "new_thuadatcanhtac", "new_new_thuadatcanhtac_new_chinhsachdautuct", new ColumnSet(new string[] { "new_chinhsachdautuchitietid" }), "new_thuadatcanhtacid", ChiTietHD.Id);
                                    //foreach (Entity oldCSDT in OldlistCSDTBS.Entities)
                                    //{
                                    //    OldlistCSDTBSRef.Add(oldCSDT.ToEntityReference());
                                    //}

                                    //service.Disassociate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_thuadatcanhtac_new_chinhsachdautuct"), OldlistCSDTBSRef);


                                    // ------------ Tìm CSDT bổ sung
                                    string fetchCSDTBS =
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
                                                <attribute name='new_loaisohuudat' />
                                                <attribute name='new_sotienbosung' />
                                                <attribute name='new_sotienbosung_khl' />
                                                <attribute name='new_bosungphanbon' />
                                                <attribute name='new_bosungtienmat' />
                                                <order attribute='new_name' descending='false' />
                                                <filter type='and'>
                                                  <condition attribute='statecode' operator='eq' value='0' />
                                                  <condition attribute='new_vudautu' operator='eq' value='{0}' />
                                                  <condition attribute='new_nghiemthu' operator='eq' value='0' />
                                                  <condition attribute='new_tungay' operator='on-or-before' value='{1}' />
                                                  <condition attribute='new_denngay' operator='on-or-after' value='{2}' />
                                                </filter>
                                              </entity>
                                            </fetch>";

                                    fetchCSDTBS = string.Format(fetchCSDTBS, vuDTId, ngaytao, ngaytao);
                                    EntityCollection resultCSDTBS = service.RetrieveMultiple(new FetchExpression(fetchCSDTBS));

                                    traceService.Trace("so CSDT bs " + resultCSDTBS.Entities.Count());

                                    Entity mCSDTBS = null;
                                    EntityReferenceCollection listCSDTBS = new EntityReferenceCollection();

                                    foreach (Entity csdtbs in resultCSDTBS.Entities)
                                    {
                                        //// Loai so huu dat
                                        //bool phuhop = true;
                                        //if (csdtbs.Contains("new_loaisohuudat"))  // Loai chu so huu
                                        //{
                                        //    //traceService.Trace(csdtbs["new_loaisohuudat_vl"].ToString());
                                        //    //traceService.Trace(((OptionSetValue)ChiTietHD["new_loaisohuudat"]).Value.ToString());
                                        //    if (ChiTietHD.Attributes.Contains("new_loaisohuudat"))
                                        //    {
                                        //        string loaishdCSDTBS = ((OptionSetValue)csdtbs["new_loaisohuudat"]).Value.ToString();
                                        //        string loaishdHDCT = ((OptionSetValue)ChiTietHD["new_loaisohuudat"]).Value.ToString();

                                        //        if (loaishdCSDTBS != loaishdHDCT)
                                        //            phuhop = false;
                                        //    }
                                        //    else
                                        //        phuhop = false;
                                        //}

                                        // NHom khach hang

                                        bool phuhop = true;
                                        if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                        {
                                            if (csdtbs.Attributes.Contains("new_nhomkhachhang"))
                                            {
                                                phuhop = false;
                                                EntityReference nhomkhCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_nhomkhachhang");
                                                Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                                Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang" }));

                                                if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                                {
                                                    Guid nhomkhCSDTBSId = nhomkhCSDTBSRef.Id;
                                                    Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;

                                                    if (nhomkhId != nhomkhCSDTBSId)
                                                    {
                                                        phuhop = true;
                                                        break;
                                                        //traceService.Trace("Phu hop ");
                                                    }
                                                }
                                                else   //neu khong co NHomKH trong CTHD
                                                    phuhop = false;
                                            }

                                            if (phuhop == false)
                                                continue;
                                        }

                                        if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                        {
                                            if (csdtbs.Attributes.Contains("new_nhomkhachhang"))
                                            {
                                                phuhop = false;
                                                EntityReference nhomkhCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_nhomkhachhang");
                                                Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
                                                Entity khObj = service.Retrieve("account", khId, new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

                                                if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                                {
                                                    Guid nhomkhCSDTBSId = nhomkhCSDTBSRef.Id;
                                                    Guid nhomkhId = khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang").Id;

                                                    if (nhomkhId != nhomkhCSDTBSId)
                                                    {
                                                        phuhop = true;
                                                        break;
                                                        //traceService.Trace("Phu hop ");
                                                    }
                                                }
                                                else   //neu khong co NHomKH trong CTHD
                                                    phuhop = false;
                                            }

                                            if (phuhop == false)
                                                continue;
                                        }


                                        // Giong mia


                                        if (csdtbs.Attributes.Contains("new_giongmia"))
                                        {
                                            phuhop = false;
                                            EntityReference giongmiaCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_giongmia");
                                            if (giongmiaEntityRef != null && giongmiaEntityRef.Id != Guid.Empty)
                                            {
                                                Guid giongmiaCSDTBSId = giongmiaCSDTBSRef.Id;

                                                if (giongmiaId != giongmiaCSDTBSId)
                                                {
                                                    phuhop = true;
                                                    break;
                                                    //traceService.Trace("Phu hop ");
                                                }
                                            }
                                            else   //neu khong co Giongmia trong CTHD
                                                phuhop = false;

                                            if (phuhop == false)
                                                continue;
                                        }


                                        // NHom nang suat


                                        if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                        {
                                            
                                            if (csdtbs.Attributes.Contains("new_nhomnangsuat"))
                                            {
                                                phuhop = false;
                                                EntityReference nhomnangsuatCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_nhomnangsuat");
                                                Guid nhomnangsuatCSDTBSRefId = nhomnangsuatCSDTBSRef.Id;
                                                Entity nhomnangsuatCSDTBS = service.Retrieve("new_nhomnangsuat", nhomnangsuatCSDTBSRefId, new ColumnSet(new string[] { "new_nangsuattu", "new_nangsuatden" }));

                                                traceService.Trace("NS tu " + nhomnangsuatCSDTBS["new_nangsuattu"].ToString());
                                                traceService.Trace("NS den " + nhomnangsuatCSDTBS["new_nangsuatden"].ToString());

                                                Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                                Entity khObj = service.Retrieve("contact", khId, new ColumnSet(new string[] { "fullname", "new_nhomkhachhang", "new_nangsuatbinhquan" }));


                                                if (khObj.Attributes.Contains("new_nangsuatbinhquan") && nhomnangsuatCSDTBS.Attributes.Contains("new_nangsuattu") && nhomnangsuatCSDTBS.Attributes.Contains("new_nangsuatden"))
                                                {
                                                    traceService.Trace("NS BQ " + khObj["new_nangsuatbinhquan"].ToString());

                                                    decimal nangsuatbq = khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                                    decimal nangsuattu = nhomnangsuatCSDTBS.GetAttributeValue<decimal>("new_nangsuattu");
                                                    decimal nangsuatden = nhomnangsuatCSDTBS.GetAttributeValue<decimal>("new_nangsuatden");

                                                    if (!((nangsuatbq >= nangsuattu) && (nangsuatbq <= nangsuatden)))
                                                    {
                                                        phuhop = true;
                                                        break;
                                                        //traceService.Trace("Phu hop ");
                                                    }
                                                }
                                                else
                                                {
                                                    phuhop = false;
                                                }
                                            }

                                            if (phuhop == false)
                                                continue;
                                        }

                                        if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                        {
                                            
                                            if (csdtbs.Attributes.Contains("new_nhomnangsuat"))
                                            {
                                                phuhop = false;
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
                                                        phuhop = true;
                                                        break;
                                                    }
                                                }
                                                else
                                                {
                                                    phuhop = false;
                                                }
                                            }
                                            if (phuhop == false)
                                                continue;
                                        }

                                        // Khuyen khich phat trien
                                        if (csdtbs.Attributes.Contains("new_khuyenkhichphattrien"))
                                        {
                                            phuhop = false;
                                            EntityCollection dsKKPTHDCT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_khuyenkhichpt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_thuadatcanhtacid", ChiTietHD.Id);
                                            EntityReference kkptCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_khuyenkhichphattrien");
                                            if (dsKKPTHDCT.Entities.Count > 0)
                                            {
                                                foreach (Entity kkptHDCT in dsKKPTHDCT.Entities)
                                                {
                                                    if (kkptHDCT.Id != kkptCSDTBSRef.Id)
                                                    {
                                                        phuhop = true;
                                                        break;
                                                    }
                                                }
                                            }
                                            else
                                                phuhop = false;

                                            if (phuhop == false)
                                                continue;
                                        }

                                        // Mo hinh khuyen nong

                                        if (csdtbs.Attributes.Contains("new_mohinhkhuyennong"))
                                        {
                                            phuhop = false;
                                            if (ChiTietHD.Attributes.Contains("new_thamgiamohinhkhuyennong"))
                                            {
                                                EntityReference mhknEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_thamgiamohinhkhuyennong");
                                                EntityReference mhknCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_mohinhkhuyennong");

                                                if (mhknCSDTBSRef.Id != mhknEntityRef.Id)
                                                    phuhop = true;
                                            }
                                            else
                                                phuhop = false;

                                            if (phuhop == false)
                                                continue;
                                        }

                                        // Nhom cu ly

                                        if (csdtbs.Attributes.Contains("new_nhomculy"))
                                        {
                                            phuhop = false;
                                            if (thuadatObj.Attributes.Contains("new_nhomculy"))
                                            {
                                                EntityReference nhomclEntityRef = thuadatObj.GetAttributeValue<EntityReference>("new_nhomculy");
                                                EntityReference nhomclCSDTBSRef = csdtbs.GetAttributeValue<EntityReference>("new_nhomculy");
                                                if (nhomclEntityRef.Id != nhomclCSDTBSRef.Id)
                                                {
                                                    phuhop = true;
                                                    break;
                                                }
                                            }
                                            else
                                                phuhop = false;

                                            if (phuhop == false)
                                                continue;
                                        }

                                        //Vung dia ly
                                        EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachdautuchitiet", "new_new_chinhsachdautuchitiet_new_vung", new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuchitietid", csdtbs.Id);

                                        if (dsVungDL.Entities.Count > 0)
                                        {
                                            phuhop = false;

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
                                                        phuhop = true;
                                                        break;
                                                    }
                                                }
                                            }
                                            if (phuhop == false)
                                                continue;
                                        }

                                        if (phuhop == false)
                                            continue;

                                        mCSDTBS = csdtbs;

                                        traceService.Trace("Tim duoc CSDT bs " + mCSDTBS.Id.ToString());

                                        if (mCSDTBS != null && mCSDTBS.Id != Guid.Empty)
                                            listCSDTBS.Add(mCSDTBS.ToEntityReference());
                                        //service.Associate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), new EntityReferenceCollection() { mCSDTthamcanh.ToEntityReference() });

                                        traceService.Trace("gan duoc vao list ");

                                        dongiabsKHL += (csdtbs.Contains("new_sotienbosung_khl") ? csdtbs.GetAttributeValue<Money>("new_sotienbosung_khl").Value : 0);
                                        dongiabsHL += (csdtbs.Contains("new_sotienbosung") ? csdtbs.GetAttributeValue<Money>("new_sotienbosung").Value : 0);
                                        dongiabsPB += (csdtbs.Contains("new_bosungphanbon") ? csdtbs.GetAttributeValue<Money>("new_bosungphanbon").Value : 0);
                                        dongiabsTM += (csdtbs.Contains("new_bosungtienmat") ? csdtbs.GetAttributeValue<Money>("new_bosungtienmat").Value : 0);

                                        //EntityCollection tlthvBScol = FindTLTHVbosung(service, csdtbs);

                                        //traceService.Trace("Tim duoc ty le thu hoi von bs");

                                        //if (tlthvBScol != null && tlthvBScol.Entities.Count() > 0)
                                        //{
                                        //    foreach (Entity tlthvbs in tlthvBScol.Entities)
                                        //    {
                                        //        Entity tlthvdkBS = new Entity("new_tylethuhoivondukien");

                                        //        //EntityReference vudautuEntityRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");
                                        //        EntityReference hdctEntityRef = new EntityReference("new_thuadatcanhtac", entityId);

                                        //        if (tlthvbs.Attributes.Contains("new_phantramtilethuhoi") && tlthvbs.Attributes.Contains("new_vuthuhoi"))
                                        //        {
                                        //            EntityReference vdtRef = tlthvbs.GetAttributeValue<EntityReference>("new_vuthuhoi");
                                        //            Entity vdt = service.Retrieve("new_vudautu", vdtRef.Id, new ColumnSet(new string[] { "new_name" }));

                                        //            string tenvdt = vdt["new_name"].ToString();

                                        //            string tenTLTHVbs = "TLTH Bổ sung " + tenvdt;
                                        //            decimal tyle = (tlthvbs.Contains("new_phantramtilethuhoi") ? (decimal)tlthvbs["new_phantramtilethuhoi"] : 0);
                                        //            decimal dinhmucDThl = dongiabsHL + dongiabsPB + dongiabsTM;
                                        //            decimal sotien = 0;

                                        //            sotien = (dinhmucDThl * dientichhd * tyle) / 100;

                                        //            Money sotienM = new Money(sotien);

                                        //            tlthvdkBS.Attributes.Add("new_name", tenTLTHVbs);
                                        //            tlthvdkBS.Attributes.Add("new_loaityle", new OptionSetValue(100000000));
                                        //            tlthvdkBS.Attributes.Add("new_chitiethddtmia", hdctEntityRef);
                                        //            tlthvdkBS.Attributes.Add("new_vudautu", vdtRef);
                                        //            tlthvdkBS.Attributes.Add("new_tylephantram", tyle);
                                        //            tlthvdkBS.Attributes.Add("new_sotienthuhoi", sotienM);

                                        //            service.Create(tlthvdkBS);
                                        //        }
                                        //    }
                                        //}
                                    }

                                    // Add list ds CSDT bổ sung vào chi tiết HDDT mía
                                    service.Associate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_thuadatcanhtac_new_chinhsachdautuct"), listCSDTBS);
                                    traceService.Trace("Asoso ");

                                    // ----------------- DINH MUC KHONG HOAN LAI
                                    //decimal dientichconlai = (ChiTietHD.Contains("new_dientichconlai") ? (decimal)ChiTietHD["new_dientichconlai"] : 0);

                                    decimal dongiaDTKHL = (csdtKQEntity.Contains("new_dinhmucdautukhonghoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautukhonghoanlai").Value : 0);
                                    decimal dongiaPhanbon = (csdtKQEntity.Contains("new_dinhmucphanbontoithieu") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucphanbontoithieu").Value : 0);


                                    dongiaDTKHL += dongiabsKHL;
                                    Money MdongiaDTKHL = new Money(dongiaDTKHL);

                                    decimal dinhmucDTKHL = dongiaDTKHL * dientichhd;
                                    Money MdinhmucDTKHL = new Money(dinhmucDTKHL);

                                    //traceService.Trace("truoc cap nhat " + dinhmucDTKHL);

                                    en["new_dongiadautukhonghoanlai"] = MdongiaDTKHL;
                                    en["new_dinhmucdautukhonghoanlai"] = MdinhmucDTKHL;

                                    // ----------END ------- DINH MUC KHONG HOAN LAI

                                    // ----------------- DINH MUC HOAN LAI

                                    decimal dongiaDTHL = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value : 0);
                                    dongiaDTHL = dongiaDTHL + dongiabsHL + dongiabsPB + dongiabsTM;
                                    decimal dinhmucDTHL = dongiaDTHL * dientichhd;

                                        //traceService.Trace("dongiaDTHL " + dongiaDTHL);
                                        //traceService.Trace("dientichhd " + dientichhd);

                                        Money MdongiaDT = new Money(dongiaDTHL);
                                        Money MdinhmucDT = new Money(dinhmucDTHL);

                                        en.Attributes.Add("new_dongiadautuhoanlai", MdongiaDT);
                                        en.Attributes.Add("new_dinhmucdautuhoanlai", MdinhmucDT);
                                    

                                    // ----------END ------- DINH MUC HOAN LAI

                                    // -------------------- DINH MUC PHAN BON

                                    decimal tongDMPB = 0;

                                    if (!HDDTmia.Contains("new_chinhantienmat") || (HDDTmia.Contains("new_chinhantienmat") && (bool)HDDTmia["new_chinhantienmat"] == false))
                                    {
                                        Money MdongiaPhanbon = new Money(dongiaPhanbon);
                                        tongDMPB = dongiaPhanbon * dientichhd;
                                        Money MtongDMDTKHL = new Money(tongDMPB);

                                        en.Attributes.Add("new_dongiaphanbontoithieu", MdongiaPhanbon);
                                        en.Attributes.Add("new_dinhmucphanbontoithieu", MtongDMDTKHL);
                                    }

                                    // --------END--------- DINH MUC PHAN BON

                                    // -------------------- DINH MUC DAU TU

                                    //decimal dmDTHL = (ChiTietHD.Contains("new_dinhmucdautuhoanlai") ? ChiTietHD.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value : 0);
                                    //decimal dmDTKHL = (ChiTietHD.Contains("new_dinhmucdautukhonghoanlai") ? ChiTietHD.GetAttributeValue<Money>("new_dinhmucdautukhonghoanlai").Value : 0);
                                    //decimal dmPhanbon = (ChiTietHD.Contains("new_dinhmucphanbontoithieu") ? ChiTietHD.GetAttributeValue<Money>("new_dinhmucphanbontoithieu").Value : 0);

                                    decimal tongDM = dinhmucDTHL + dinhmucDTKHL;

                                    Money MtongDM = new Money(tongDM);
                                    en.Attributes.Add("new_dinhmucdautu", MtongDM);
                                    

                                    // --------END--------- DINH MUC DAU TU

                                    // -------- End nhom du lieu  Gan Dinh muc

                                    service.Update(en);

                                    //EntityReferenceCollection OldlistCSDTRef = new EntityReferenceCollection();
                                    //EntityCollection OldlistCSDT = RetrieveNNRecord(service, "new_chinhsachdautu", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_chinhsachdautu", new ColumnSet(new string[] { "new_chinhsachdautuid" }), "new_thuadatcanhtacid", ChiTietHD.Id);
                                    //foreach (Entity oldCSDT in OldlistCSDT.Entities)
                                    //{
                                    //    OldlistCSDTRef.Add(oldCSDT.ToEntityReference());
                                    //}

                                    //service.Disassociate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), OldlistCSDTRef);

                                    //EntityReferenceCollection listCSDT = new EntityReferenceCollection();
                                    //listCSDT.Add(mCSDT.ToEntityReference());
                                    //service.Associate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), new EntityReferenceCollection() { mCSDT.ToEntityReference() });

                                    //-------- Tìm CSDT thâm canh --------------------

                                    EntityCollection csdtThamcanhCol = FindCSDTthamcanh(service, ChiTietHD);
                                    Entity mCSDTthamcanh = null;

                                    if (csdtThamcanhCol != null && csdtThamcanhCol.Entities.Count > 0)
                                    {
                                        foreach (Entity a in csdtThamcanhCol.Entities)
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
                                                traceService.Trace(a["new_loaisohuudat_vl"].ToString());
                                                traceService.Trace(((OptionSetValue)ChiTietHD["new_loaisohuudat"]).Value.ToString());
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
                                                if (giongmiaObj.Attributes.Contains("new_nhomgiong"))
                                                {
                                                    if (a["new_nhomgiongmia_vl"].ToString().IndexOf(((OptionSetValue)giongmiaObj["new_nhomgiong"]).Value.ToString()) == -1)
                                                        continue;
                                                }
                                                else
                                                    continue;
                                            }

                                            // Nhom khach hang
                                            bool co = false;
                                            if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                            {
                                                Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
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
                                                        co = true;
                                                }
                                                else   //neu khong co NHomKH trong CTHD
                                                {
                                                    if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                                        co = true;
                                                }
                                            }
                                            if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                            {
                                                Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
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
                                                        co = true;
                                                }
                                                else   //neu khong co NHomKH trong CTHD
                                                {
                                                    if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                                        co = true;
                                                }
                                            }

                                            if (co == false)
                                                continue;

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
                                                co = true;

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
                                                            break;  //thoat vong for thu 1
                                                    }
                                                }
                                                else
                                                    co = true;
                                            }
                                            else   //neu khong co KKPT trong CTHD
                                            {
                                                if (dsKKPTCSDT == null || dsKKPTCSDT.Entities.Count() == 0)
                                                    co = true;
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
                                                    co = true;
                                            }
                                            else   //neu khong co NHomCL trong CTHD
                                            {
                                                if (dsNHomCL == null || dsNHomCL.Entities.Count() == 0)
                                                    co = true;
                                            }
                                            if (co == false)
                                                continue;

                                            // Mo hinh khuyen nong
                                            co = false;

                                            EntityCollection dsMHKN = RetrieveNNRecord(service, "new_mohinhkhuyennong", "new_chinhsachdautu", "new_new_chinhsachdautu_new_mohinhkhuyennong", new ColumnSet(new string[] { "new_mohinhkhuyennongid" }), "new_chinhsachdautuid", a.Id);

                                            if (ChiTietHD.Attributes.Contains("new_thamgiamohinhkhuyennong"))
                                            {
                                                Guid mhknId = ChiTietHD.GetAttributeValue<EntityReference>("new_thamgiamohinhkhuyennong").Id;
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
                                                    co = true;
                                            }
                                            else   //neu khong co MNKH trong CTHD
                                            {
                                                if (dsMHKN == null || dsMHKN.Entities.Count() == 0)
                                                    co = true;
                                            }
                                            if (co == false)
                                                continue;

                                            // NHom nang suat
                                            co = false;
                                            if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                            {
                                                Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
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
                                                        co = true;
                                                }
                                                else
                                                {
                                                    if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                                        co = true;
                                                }
                                            }
                                            if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                            {
                                                Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
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
                                                        co = true;
                                                }
                                                else
                                                {
                                                    if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                                        co = true;
                                                }
                                            }

                                            if (co == false)
                                                continue;

                                            mCSDTthamcanh = a;
                                            break;
                                        }
                                        if (mCSDTthamcanh != null && mCSDTthamcanh.Id != Guid.Empty)
                                        {

                                        }
                                        //listCSDT.Add(mCSDTthamcanh.ToEntityReference());
                                        //service.Associate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), new EntityReferenceCollection() { mCSDTthamcanh.ToEntityReference() });
                                    }
                                    //----END---- Tìm CSDT thâm canh ----------------

                                    //-------- Tìm CSDT tưới mía --------------------

                                    EntityCollection csdtTuoimiaCol = FindCSDTtuoi(service, ChiTietHD);
                                    Entity mCSDTtuoimia = null;

                                    if (csdtTuoimiaCol != null && csdtTuoimiaCol.Entities.Count > 0)
                                    {
                                        foreach (Entity a in csdtTuoimiaCol.Entities)
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

                                            if (a.Contains("new_mucdichtuoi_vl"))  // Muc dich tuoi
                                            {
                                                if (ChiTietHD.Attributes.Contains("new_mucdichtuoi"))
                                                {
                                                    if (a["new_mucdichtuoi_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_mucdichtuoi"]).Value.ToString()) == -1)
                                                        continue;
                                                }
                                                else
                                                    continue;
                                            }

                                            if (a.Contains("new_phuongphaptuoi_vl"))  // Phuong phap tuoi
                                            {
                                                if (ChiTietHD.Attributes.Contains("new_phuongphaptuoi"))
                                                {
                                                    if (a["new_phuongphaptuoi_vl"].ToString().IndexOf(((OptionSetValue)ChiTietHD["new_phuongphaptuoi"]).Value.ToString()) == -1)
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
                                                    if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_loaisohuudat"]).Value.ToString()) == -1)
                                                        continue;
                                                }
                                                else
                                                    continue;
                                            }

                                            traceService.Trace("vi trí Nhom KH");
                                            // NHom khach hang
                                            bool co = false;
                                            if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                            {
                                                Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
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
                                                        co = true;
                                                }
                                                else   //neu khong co NHomKH trong CTHD
                                                {
                                                    if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                                        co = true;
                                                }
                                            }

                                            if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                            {
                                                Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
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
                                                        co = true;
                                                }
                                                else   //neu khong co NHomKH trong CTHD
                                                {
                                                    if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                                        co = true;
                                                }
                                            }
                                            if (co == false)
                                                continue;

                                            traceService.Trace("vi trí truoc Vung dia ly");

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

                                            traceService.Trace("vi trí truoc Nhom cu ly");
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
                                            }
                                            else   //neu khong co NHomCL trong CTHD
                                            {
                                                if (dsNHomCL == null || dsNHomCL.Entities.Count() == 0)
                                                    co = true;
                                            }
                                            if (co == false)
                                                continue;

                                            traceService.Trace("vi trí truoc NHom nang suat");
                                            // NHom nang suat
                                            co = false;
                                            if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                            {
                                                Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
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
                                                        co = true;
                                                }
                                                else
                                                {
                                                    if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                                        co = true;
                                                }
                                            }

                                            if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                            {
                                                Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
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
                                                        co = true;
                                                }
                                                else
                                                {
                                                    if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                                        co = true;
                                                }
                                            }

                                            if (co == false)
                                                continue;

                                            mCSDTtuoimia = a;
                                            break;
                                        }
                                        if (mCSDTtuoimia != null && mCSDTtuoimia.Id != Guid.Empty)
                                        {

                                        }
                                        //listCSDT.Add(mCSDTtuoimia.ToEntityReference());
                                        //service.Associate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), new EntityReferenceCollection() { mCSDTtuoimia.ToEntityReference() });
                                    }
                                    //----END---- Tìm CSDT tưới mía --------------------

                                    //-------- Tìm CSDT bóc lá mía --------------------

                                    traceService.Trace("vi trí bat dau Tìm CSDT bóc lá mía");
                                    EntityCollection csdtBocLamiaCol = FindCSDTbocla(service, ChiTietHD);
                                    Entity mCSDTbocla = null;

                                    if (csdtBocLamiaCol != null && csdtBocLamiaCol.Entities.Count > 0)
                                    {
                                        foreach (Entity a in csdtBocLamiaCol.Entities)
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
                                                    if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_loaisohuudat"]).Value.ToString()) == -1)
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
                                                        co = true;
                                                }
                                                else   //neu khong co NHomKH trong CTHD
                                                {
                                                    if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                                        co = true;
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
                                                        co = true;
                                                }
                                                else   //neu khong co NHomKH trong CTHD
                                                {
                                                    if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                                        co = true;
                                                }
                                            }

                                            if (co == false)
                                                continue;

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
                                                co = true;

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
                                                            //thoat vong for thu 1
                                                            break;
                                                    }
                                                }
                                                else
                                                    co = true;
                                            }
                                            else   //neu khong co KKPT trong CTHD
                                            {
                                                if (dsKKPTCSDT == null || dsKKPTCSDT.Entities.Count() == 0)
                                                    co = true;
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
                                                    co = true;
                                            }
                                            else   //neu khong co NHomCL trong CTHD
                                            {
                                                if (dsNHomCL == null || dsNHomCL.Entities.Count() == 0)
                                                    co = true;
                                            }
                                            if (co == false)
                                                continue;

                                            // Mo hinh khuyen nong
                                            co = false;

                                            EntityCollection dsMHKN = RetrieveNNRecord(service, "new_mohinhkhuyennong", "new_chinhsachdautu", "new_new_chinhsachdautu_new_mohinhkhuyennong", new ColumnSet(new string[] { "new_mohinhkhuyennongid" }), "new_chinhsachdautuid", a.Id);

                                            if (ChiTietHD.Attributes.Contains("new_thamgiamohinhkhuyennong"))
                                            {
                                                Guid mhknId = ChiTietHD.GetAttributeValue<EntityReference>("new_thamgiamohinhkhuyennong").Id;
                                                Entity mhkn = service.Retrieve("new_mohinhkhuyennong", mhknId, new ColumnSet(new string[] { "new_name" }));

                                                if (dsMHKN != null && dsMHKN.Entities.Count() > 0)
                                                {
                                                    //List<Entity> ldsMHKN = dsMHKN.Entities.ToList<Entity>();

                                                    //foreach (Entity mhkn1 in ldsMHKN)
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
                                                    co = true;
                                            }
                                            else   //neu khong co MNKH trong CTHD
                                            {
                                                if (dsMHKN == null || dsMHKN.Entities.Count() == 0)
                                                    co = true;
                                            }
                                            if (co == false)
                                                continue;

                                            // NHom nang suat
                                            co = false;
                                            if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                            {
                                                Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
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
                                                        co = true;
                                                }
                                                else
                                                {
                                                    if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                                        co = true;
                                                }
                                            }
                                            if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                            {
                                                Guid khId = ChiTietHD.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
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
                                                        co = true;
                                                }
                                                else
                                                {
                                                    if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                                        co = true;
                                                }
                                            }

                                            if (co == false)
                                                continue;

                                            mCSDTbocla = a;
                                            break;
                                        }
                                        if (mCSDTbocla != null && mCSDTbocla.Id != Guid.Empty)
                                        {

                                        }
                                        //listCSDT.Add(mCSDTbocla.ToEntityReference());
                                        //service.Associate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), new EntityReferenceCollection() { mCSDTbocla.ToEntityReference() }); 
                                    }
                                    //----END---- Tìm CSDT bóc lá mía --------------------

                                    //----------- Tìm CSDT ứng --------------------

                                    EntityCollection csdtUngCol = FindCSDTung(service, ChiTietHD);
                                    Entity mCSDTung = null;

                                    if (csdtUngCol != null && csdtUngCol.Entities.Count > 0)
                                    {
                                        foreach (Entity a in csdtUngCol.Entities)
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

                                            mCSDTung = a;
                                            break;
                                        }
                                        if (mCSDTung != null && mCSDTung.Id != Guid.Empty)
                                        {

                                        }
                                        //listCSDT.Add(mCSDTung.ToEntityReference());
                                        //service.Associate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), new EntityReferenceCollection() { mCSDTung.ToEntityReference() });
                                    }

                                    traceService.Trace("vi trí add  ứng");
                                    //----END---- Tìm CSDT ứng --------------------

                                    //service.Associate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), listCSDT);

                                    traceService.Trace("vi trí sau add list");

                                }  //if (mCSDT != null && mCSDT.Id != Guid.Empty)
                                else
                                {
                                    throw new InvalidPluginExecutionException("Không tìm thấy Chính sách Đầu tư trồng chăm sóc mía phù hợp khi cập nhật");
                                }
                                //} // if (ChiTietHD.Contains("new_chinhsachdautu"))
                                //else // neu da co CSDT
                                //{ 

                                //}

                            }
                        }
                    }  //if(context.MessageName.ToUpper() == "UPDATE")
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

        public static EntityCollection FindCSDTtrongmia(IOrganizationService crmservices, Entity ctHDDT)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctHDDT["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctHDDT["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000000));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }


        public static EntityCollection FindCSDTthamcanh(IOrganizationService crmservices, Entity ctHDDT)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctHDDT["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctHDDT["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000001));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }

        public static EntityCollection FindCSDTtuoi(IOrganizationService crmservices, Entity ctHDDT)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctHDDT["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctHDDT["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000002));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }

        public static EntityCollection FindCSDTbocla(IOrganizationService crmservices, Entity ctHDDT)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctHDDT["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctHDDT["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000003));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }

        public static EntityCollection FindCSDTung(IOrganizationService crmservices, Entity ctHDDT)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctHDDT["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctHDDT["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000004));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }
        public static EntityCollection FindTLTHVDK(IOrganizationService crmservices, Entity chitietHD)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_tylethuhoivondukien'>
                        <attribute name='new_name' />
                        <attribute name='new_vudautu' />
                        <attribute name='new_tylephantram' />
                        <attribute name='new_sotienthuhoi' />
                        <attribute name='new_loaityle' />
                        <attribute name='new_chitiethddtmia' />
                        <attribute name='new_tylethuhoivondukienid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_chitiethddtmia' operator='eq' uitype='new_thuadatcanhtac' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, chitietHD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindTLTHVbosung(IOrganizationService crmservices, Entity CSDTbosung)
        {
            QueryExpression q = new QueryExpression("new_tilethuhoivon");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_chinhsachdautubosung", ConditionOperator.Equal, CSDTbosung.Id));
            //q.Orders.Add(new OrderExpression("new_nam", OrderType.Ascending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }

    }
}