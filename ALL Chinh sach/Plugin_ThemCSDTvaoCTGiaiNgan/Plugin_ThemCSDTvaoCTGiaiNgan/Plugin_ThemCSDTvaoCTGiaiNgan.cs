using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Activities;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Crm.Sdk.Messages;

namespace Plugin_ThemCSDTvaoCTGiaiNgan
{
    public class Plugin_ThemCSDTvaoCTGiaiNgan : IPlugin
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
                Entity target = (Entity)context.InputParameters["Target"];
                Guid targetId = target.Id;

                if (target.LogicalName == "new_chitietphieudenghigiaingan")
                {
                    traceService.Trace("Begin plugin");
                    if (context.MessageName.ToUpper() == "CREATE" || context.MessageName.ToUpper() == "UPDATE")
                    {
                        Entity ChiTietPDNgiaingan = service.Retrieve("new_chitietphieudenghigiaingan", targetId, new ColumnSet(true));
                        DateTime ngaytao = ChiTietPDNgiaingan.GetAttributeValue<DateTime>("createdon");

                        traceService.Trace("Chay plugin");

                        //DateTime ngaytaoqq = ChiTietPDNgiaingan.GetAttributeValue<DateTime>("createdonNNNN");

                        // Kiem tra Tien mat da chi cho PDN GN cho PHAN BON
                        if (ChiTietPDNgiaingan.Contains("new_noidunggiaingan") && ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_noidunggiaingan").Value.ToString() == "100000001" && ChiTietPDNgiaingan.Contains("new_loaigiaingan") && ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_loaigiaingan").Value.ToString() == "100000000" && ChiTietPDNgiaingan.Contains("new_hopdongdautumia"))
                        {
                            EntityReference HDDTmiaRef = ChiTietPDNgiaingan.GetAttributeValue<EntityReference>("new_hopdongdautumia");
                            Guid DHDTmiaId = HDDTmiaRef.Id;
                            Entity HDDTmia = service.Retrieve("new_hopdongdautumia", DHDTmiaId, new ColumnSet(new string[] { "new_vudautu", "new_dachihoanlai_phanbon", "new_dachikhonghoanlai_phanbon", "new_tongtienphanbontoithieu" }));

                            decimal dachiPBhoanlai = (HDDTmia.Contains("new_dachihoanlai_phanbon") ? HDDTmia.GetAttributeValue<Money>("new_dachihoanlai_phanbon").Value : 0);
                            decimal dachiPBkhongHL = (HDDTmia.Contains("new_dachikhonghoanlai_phanbon") ? HDDTmia.GetAttributeValue<Money>("new_dachikhonghoanlai_phanbon").Value : 0);

                            decimal tongdachiPB = dachiPBhoanlai + dachiPBkhongHL;
                            decimal sotienPBtoithieu = (HDDTmia.Contains("new_tongtienphanbontoithieu") ? HDDTmia.GetAttributeValue<Money>("new_tongtienphanbontoithieu").Value : 0);

                            if (tongdachiPB < sotienPBtoithieu)
                                throw new InvalidPluginExecutionException("Hiện chưa giải ngân hết số tiền phân bón tối thiểu, vui lòng chọn Hình thức giải ngân là 'Vật tư'");
                        }
                        // END ----- Kiem tra Tien mat da chi cho PDN GN cho PHAN BON

                        Entity en = new Entity(ChiTietPDNgiaingan.LogicalName);
                        en.Id = ChiTietPDNgiaingan.Id;

                        if (ChiTietPDNgiaingan.Contains("new_noidunggiaingan") && ChiTietPDNgiaingan.Contains("new_hopdongdautumia") && ChiTietPDNgiaingan.Contains("new_loaigiaingan") && ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_loaigiaingan").Value.ToString() == "100000000")
                        {
                            // -------  Gắn Dịnh muc cho Nghiem thu Tham canh - Danh gia nang suat
                            if (ChiTietPDNgiaingan.Contains("new_noidunggiaingan") && ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_noidunggiaingan").Value.ToString() == "100000003" && ChiTietPDNgiaingan.Attributes.Contains("new_danhgianangsuat"))
                            {
                                EntityReference NTThamcanhRef = ChiTietPDNgiaingan.GetAttributeValue<EntityReference>("new_danhgianangsuat");
                                Guid NTThamcanhId = NTThamcanhRef.Id;
                                Entity NTThamcanh = service.Retrieve("new_danhgianangsuat", NTThamcanhId, new ColumnSet(new string[] { "subject", "createdon", "new_vudautu", "new_hopdongdautumia", "new_thuadatcanhtac", "new_loaidanhgia", "new_dinhmuc", "new_dientich", "new_dukienno" , "new_tongtien" }));

                                decimal dtnghiemthu = (NTThamcanh.Contains("new_dientich") ? (decimal)NTThamcanh["new_dientich"] : 0);
                                DateTime ngaytaoTC = NTThamcanh.GetAttributeValue<DateTime>("createdon");

                                if (NTThamcanh.Attributes.Contains("new_loaidanhgia") && NTThamcanh.GetAttributeValue<OptionSetValue>("new_loaidanhgia").Value.ToString() == "100000001") //Loai danh gia: Tham canh la 100000001
                                {
                                    if (!NTThamcanh.Contains("new_thuadatcanhtac"))
                                    {
                                        throw new InvalidPluginExecutionException("Thiếu thông tin bắt buộc về thửa đất / giống mía / vụ đầu tư");
                                    }
                                    else
                                    {
                                        EntityReference ctHDDTmiaRef = NTThamcanh.GetAttributeValue<EntityReference>("new_thuadatcanhtac");
                                        Guid ctHDDTmiaId = ctHDDTmiaRef.Id;
                                        Entity ctHDDTmia = service.Retrieve("new_thuadatcanhtac", ctHDDTmiaId, new ColumnSet(new string[] { "new_vutrong", "new_loaigocmia", "new_mucdichsanxuatmia", "new_giongmia", "new_thuadat", "createdon", "new_hopdongdautumia", "new_khachhang", "new_khachhangdoanhnghiep", "new_thamgiamohinhkhuyennong", "new_dientichthucte", "new_loaisohuudat" }));

                                        EntityReference thuadatEntityRef = ctHDDTmia.GetAttributeValue<EntityReference>("new_thuadat");
                                        EntityReference giongmiaEntityRef = ctHDDTmia.GetAttributeValue<EntityReference>("new_giongmia");

                                        EntityReference vudautuRef = NTThamcanh.GetAttributeValue<EntityReference>("new_vudautu");
                                        Entity vuDT = service.Retrieve("new_vudautu", vudautuRef.Id, new ColumnSet(new string[] { "new_name" }));

                                        Guid thuadatId = thuadatEntityRef.Id;
                                        Entity thuadatObj = service.Retrieve("new_thuadat", thuadatEntityRef.Id, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy" , "new_diachi" }));

                                        Guid giongmiaId = giongmiaEntityRef.Id;
                                        Entity giongmiaObj = service.Retrieve("new_giongmia", giongmiaId, new ColumnSet(new string[] { "new_nhomgiong", "new_name" }));

                                        EntityCollection dsCSDTThamcanh = FindCSDTNTThamcanh(service, ngaytaoTC, vuDT);

                                        Entity mCSDT = new Entity();
                                        decimal HL = 0;
                                        decimal KHL = 0;
                                        decimal Vattu = 0;

                                        decimal dmHL = 0;
                                        decimal dmKHL = 0;
                                        decimal dmVattu = 0;
                                        decimal dmTienmat = 0;

                                        if (dsCSDTThamcanh != null && dsCSDTThamcanh.Entities.Count() > 0)
                                        {
                                            foreach (Entity a in dsCSDTThamcanh.Entities)
                                            {
                                                if (a.Contains("new_vutrong_vl"))  // Vu trong
                                                {
                                                    if (ctHDDTmia.Contains("new_vutrong"))
                                                    {
                                                        if (a["new_vutrong_vl"].ToString().IndexOf(((OptionSetValue)ctHDDTmia["new_vutrong"]).Value.ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                if (a.Contains("new_loaigocmia_vl"))  // Loai goc mia
                                                {
                                                    if (ctHDDTmia.Contains("new_loaigocmia"))
                                                    {
                                                        if (a["new_loaigocmia_vl"].ToString().IndexOf(((OptionSetValue)ctHDDTmia["new_loaigocmia"]).Value.ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                if (a.Contains("new_mucdichsanxuatmia_vl"))  // Muc dich san xuat mia
                                                {
                                                    if (ctHDDTmia.Contains("new_mucdichsanxuatmia"))
                                                    {
                                                        if (a["new_mucdichsanxuatmia_vl"].ToString().IndexOf(((OptionSetValue)ctHDDTmia["new_mucdichsanxuatmia"]).Value.ToString()) == -1)
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
                                                    if (ctHDDTmia.Attributes.Contains("new_loaisohuudat"))
                                                    {
                                                        if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)ctHDDTmia["new_loaisohuudat"]).Value.ToString()) == -1)
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
                                                            co = true;
                                                    }
                                                    else   //neu khong co NHomKH trong CTHD
                                                    {
                                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                                            co = true;
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
                                                            co = true;
                                                    }
                                                    else
                                                    {
                                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                                            co = true;
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

                                                mCSDT = a;
                                                break;
                                            }
                                        }
                                        else
                                            throw new InvalidPluginExecutionException("Chưa có Chính sách Đầu tư NT thâm canh nào cho vụ đầu tư này");

                                        if (mCSDT != null && mCSDT.Id != Guid.Empty)
                                        {
                                            Guid csdtKQ = mCSDT.Id;
                                            Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_dinhmucdautukhonghoanlai", "new_dinhmucphanbontoithieu" }));

                                            en["new_chinhsachdautu"] = csdtKQEntity.ToEntityReference();

                                            HL = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value : 0);
                                            KHL = (csdtKQEntity.Contains("new_dinhmucdautukhonghoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautukhonghoanlai").Value : 0);
                                            Vattu = (csdtKQEntity.Contains("new_dinhmucphanbontoithieu") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucphanbontoithieu").Value : 0);

                                            dmHL = HL;
                                            dmKHL = KHL;
                                            dmVattu = Vattu;

                                            dmTienmat = dmHL + dmKHL - dmVattu;

                                            Money MdmHL = new Money(dmHL);
                                            Money MdmKHL = new Money(dmKHL);
                                            Money MdmVattu = new Money(dmVattu);
                                            Money MdmTienmat = new Money(dmTienmat);

                                            en["new_dinhmucdautuhl"] = MdmHL;
                                            en["new_dinhmucdautukhl"] = MdmKHL;
                                            en["new_dinhmuchoanlaivattu"] = MdmVattu;
                                            en["new_dinhmuchoanlaitienmat"] = MdmTienmat;

                                            decimal sotienGN = (NTThamcanh.Contains("new_tongtien") ? NTThamcanh.GetAttributeValue<Money>("new_tongtien").Value : 0);
                                            Money MsotienGN = new Money(sotienGN);

                                            if (ChiTietPDNgiaingan.Contains("new_loaigiaingan"))
                                            {
                                                if (ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_loaigiaingan").Value.ToString() == "100000000")
                                                    en["new_sotiendtkhonghoanlai"] = MsotienGN;
                                                else
                                                    en["new_sotiendthoanlai"] = MsotienGN;

                                                service.Update(en);
                                            }

                                            service.Update(en);

                                        }  //if (mCSDT != null && mCSDT.Id != Guid.Empty)
                                        else
                                            throw new InvalidPluginExecutionException("Không tìm thấy Chính sách Đầu tư NT thâm canh phù hợp");
                                    }
                                } //if(NTThamcanh.GetAttributeValue<OptionSetValue>("new_loaidanhgia") == loaidanhgia) 
                            } //if (ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_noidunggiaingan").Value.ToString() == "100000005" && ChiTietPDNgiaingan.Attributes.Contains("new_nghiemthutrongmia"))
                            // ----END---  Gắn Dịnh muc cho Nghiem thu Tham canh - Uoc nang suat

                            // -------  Gắn Dịnh muc cho Nghiem thu Tuoi mia
                            if (ChiTietPDNgiaingan.Contains("new_noidunggiaingan") && ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_noidunggiaingan").Value.ToString() == "100000001" && ChiTietPDNgiaingan.Contains("new_nghiemthutuoi") && ChiTietPDNgiaingan.Contains("new_loaigiaingan") && ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_loaigiaingan").Value.ToString() == "100000000")
                            {
                                traceService.Trace("Bat dau vao NT tuoi mia");

                                EntityReference NTtuoimiaRef = ChiTietPDNgiaingan.GetAttributeValue<EntityReference>("new_nghiemthutuoi");
                                Guid NTtuoimiaId = NTtuoimiaRef.Id;
                                Entity NTtuoimiaObj = service.Retrieve("new_nghiemthutuoimia", NTtuoimiaId, new ColumnSet(new string[] { "subject", "new_khachhang", "new_khachhangdoanhnghiep", "new_hopdongtrongmia", "new_mucdichsanxuatmia" , "new_tongtien" }));

                                traceService.Trace("Lay duoc NT tuoi mia");

                                EntityReference HDDTmiaRef = NTtuoimiaObj.GetAttributeValue<EntityReference>("new_hopdongtrongmia");
                                Guid DHDTmiaId = HDDTmiaRef.Id;
                                Entity HDDTmia = service.Retrieve("new_hopdongdautumia", DHDTmiaId, new ColumnSet(new string[] { "new_vudautu" }));

                                traceService.Trace("Lay duoc HDDT mia");

                                EntityReference vudautuRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");
                                Guid vuDTId = vudautuRef.Id;
                                Entity vuDT = service.Retrieve("new_vudautu", vuDTId, new ColumnSet(new string[] { "new_name" }));

                                traceService.Trace("Lay duoc Vu dt");

                                EntityCollection dsChitietNTTuoimia = FindChitietNTTuoimia(service, NTtuoimiaObj);

                                traceService.Trace("ds chi tiet NT tuoi " + dsChitietNTTuoimia.Entities.Count());

                                if (dsChitietNTTuoimia != null && dsChitietNTTuoimia.Entities.Count > 0)
                                {
                                    decimal HL = 0;
                                    decimal KHL = 0;
                                    decimal Vattu = 0;

                                    decimal dmHL = 0;
                                    decimal dmKHL = 0;
                                    decimal dmVattu = 0;
                                    decimal dmTienmat = 0;

                                    foreach (Entity ChiTietNTTuoiMia in dsChitietNTTuoimia.Entities)
                                    {

                                        EntityCollection dsChitietNTTuoimiaTuoimia = FindChitietNTTuoimiaTuoimia(service, ChiTietNTTuoiMia);
                                        traceService.Trace("ds chi tiet NT tuoi - tuoi " + dsChitietNTTuoimiaTuoimia.Entities.Count());

                                        if (dsChitietNTTuoimiaTuoimia != null && dsChitietNTTuoimiaTuoimia.Entities.Count > 0)
                                        {
                                            

                                            foreach (Entity ChiTietNTTuoiMiaTuoiMia in dsChitietNTTuoimiaTuoimia.Entities)
                                            {
                                                EntityReference TuoiMiaRef = ChiTietNTTuoiMiaTuoiMia.GetAttributeValue<EntityReference>("new_tuoimia");
                                                Entity TuoiMia = service.Retrieve("new_tuoimia", TuoiMiaRef.Id, new ColumnSet(new string[] { "subject", "new_hopdongtrongmia", "new_thuacanhtac", "new_phuongphaptuoi", "new_mucdichtuoi", "createdon", "new_dientichthuchien" }));

                                                Entity thuadatObj = new Entity();
                                                if (ChiTietNTTuoiMia.Contains("new_thuadat"))
                                                {
                                                    EntityReference thuadatEntityRef = ChiTietNTTuoiMia.GetAttributeValue<EntityReference>("new_thuadat");
                                                    Guid thuadatId = thuadatEntityRef.Id;
                                                    thuadatObj = service.Retrieve("new_thuadat", thuadatId, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy" , "new_diachi" }));
                                                }

                                                traceService.Trace("lay entity Thua dat ");

                                                Entity ctHDDTmia = new Entity();
                                                if (TuoiMia != null && TuoiMia.Contains("new_thuacanhtac"))
                                                {
                                                    EntityReference ctHDDTmiaRef = TuoiMia.GetAttributeValue<EntityReference>("new_thuacanhtac");
                                                    Guid ctHDDTmiaId = ctHDDTmiaRef.Id;
                                                    ctHDDTmia = service.Retrieve("new_thuadatcanhtac", ctHDDTmiaId, new ColumnSet(new string[] { "new_loaisohuudat" }));
                                                }

                                                traceService.Trace("vi trí khoi tao");

                                                if (vuDTId != null)
                                                {
                                                    string fetchXml =
                                                              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                                                  <entity name='new_chinhsachdautu'>
                                                                    <attribute name='new_name' />
                                                                    <attribute name='new_vudautu' />
                                                                    <attribute name='new_ngayapdung' />
                                                                    <attribute name='new_mucdichdautu' />
                                                                    <attribute name='new_loaihopdong' />
                                                                    <attribute name='new_dinhmucdautuhoanlai' />
                                                                    <attribute name='new_mucdichtuoi_vl' />
                                                                    <attribute name='new_phuongphaptuoi_vl' /> 
                                                                    <attribute name='new_nhomdat_vl' />
                                                                    <attribute name='new_vutrong_vl' />
                                                                    <attribute name='new_mucdichsanxuatmia_vl' />
                                                                    <attribute name='new_loaisohuudat_vl' />
                                                                    <attribute name='new_chinhsachdautuid' />
                                                                    <order attribute='new_ngayapdung' descending='true' />
                                                                    <filter type='and'>
                                                                      <condition attribute='statecode' operator='eq' value='0' />
                                                                      <condition attribute='new_loaihopdong' operator='eq' value='100000000' />
                                                                      <condition attribute='new_mucdichdautu' operator='eq' value='100000002' />
                                                                      <condition attribute='new_ngayapdung' operator='on-or-before' value='{0}' />
                                                                      <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{1}' />                                      
                                                                    </filter>
                                                                  </entity>
                                                                </fetch>";

                                                    fetchXml = string.Format(fetchXml, ngaytao, vuDTId);
                                                    EntityCollection result = service.RetrieveMultiple(new FetchExpression(fetchXml));
                                                    List<Entity> CSDT = result.Entities.ToList<Entity>();

                                                    traceService.Trace("vi trí tim CSDT");

                                                    Entity mCSDT = null;

                                                    if (CSDT != null && CSDT.Count() > 0)
                                                    {
                                                        foreach (Entity a in CSDT)
                                                        {
                                                            if (a.Contains("new_mucdichtuoi_vl"))  // Muc dich tuoi
                                                            {
                                                                if (TuoiMia.Attributes.Contains("new_mucdichtuoi"))
                                                                {
                                                                    if (a["new_mucdichtuoi_vl"].ToString().IndexOf(((OptionSetValue)ChiTietNTTuoiMia["new_mucdichtuoi"]).Value.ToString()) == -1)
                                                                        continue;
                                                                }
                                                                else
                                                                {
                                                                    continue;
                                                                }
                                                            }

                                                            if (a.Contains("new_phuongphaptuoi_vl"))  // Phuong phap tuoi
                                                            {
                                                                if (TuoiMia.Attributes.Contains("new_phuongphaptuoi"))
                                                                {
                                                                    if (a["new_phuongphaptuoi_vl"].ToString().IndexOf(((OptionSetValue)ChiTietNTTuoiMia["new_phuongphaptuoi"]).Value.ToString()) == -1)
                                                                        continue;
                                                                }
                                                                else
                                                                {
                                                                    continue;
                                                                }
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
                                                                if (ctHDDTmia.Attributes.Contains("new_loaisohuudat"))
                                                                {
                                                                    if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)ctHDDTmia["new_loaisohuudat"]).Value.ToString()) == -1)
                                                                        continue;
                                                                }
                                                                else
                                                                {
                                                                    continue;
                                                                }
                                                            }

                                                            traceService.Trace("vi trí Nhom KH");
                                                            // NHom khach hang
                                                            bool co = false;
                                                            if (NTtuoimiaObj.Attributes.Contains("new_khachhang"))
                                                            {
                                                                Guid khId = NTtuoimiaObj.GetAttributeValue<EntityReference>("new_khachhang").Id;
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

                                                            if (NTtuoimiaObj.Attributes.Contains("new_khachhangdoanhnghiep"))
                                                            {
                                                                Guid khId = NTtuoimiaObj.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
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

                                                            traceService.Trace("vi trí truoc NHom nang suat");
                                                            // NHom nang suat
                                                            co = false;
                                                            if (NTtuoimiaObj.Attributes.Contains("new_khachhang"))
                                                            {
                                                                Guid khId = NTtuoimiaObj.GetAttributeValue<EntityReference>("new_khachhang").Id;
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
                                                            if (NTtuoimiaObj.Attributes.Contains("new_khachhangdoanhnghiep"))
                                                            {
                                                                Guid khId = NTtuoimiaObj.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
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
                                                    else
                                                        throw new InvalidPluginExecutionException("Chưa có Chính sách Đầu tư NT Tưới mía nào cho vụ đầu tư này");

                                                    if (mCSDT != null && mCSDT.Id != Guid.Empty)
                                                    {
                                                        Guid csdtKQ = mCSDT.Id;
                                                        Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name" }));

                                                        HL += (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value : 0);
                                                        KHL += (csdtKQEntity.Contains("new_dinhmucdautukhonghoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautukhonghoanlai").Value : 0);
                                                        Vattu += (csdtKQEntity.Contains("new_dinhmucphanbontoithieu") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucphanbontoithieu").Value : 0);

                                                        dmHL = HL;
                                                        dmKHL = KHL;
                                                        dmVattu = Vattu;

                                                    }  //if (mCSDT != null && mCSDT.Id != Guid.Empty)
                                                    else
                                                    {
                                                        traceService.Trace("else");
                                                        throw new InvalidPluginExecutionException("Không tìm thấy Chính sách Đầu tư tưới mía phù hợp");
                                                    }
                                                } // if(vuDTId != null)


                                                //    if (mCSDT != null && mCSDT.Id != Guid.Empty)
                                                //    {
                                                //        Guid csdtKQ = mCSDT.Id;
                                                //        Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name" }));

                                                //        HL += (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value : 0);
                                                //        KHL += (csdtKQEntity.Contains("new_dinhmucdautukhonghoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautukhonghoanlai").Value : 0);
                                                //        Vattu += (csdtKQEntity.Contains("new_dinhmucphanbontoithieu") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucphanbontoithieu").Value : 0);

                                                //        dmHL = HL * dttuoi;
                                                //        dmKHL = KHL * dttuoi;
                                                //        dmVattu = Vattu * dttuoi;

                                                //    }  //if (mCSDT != null && mCSDT.Id != Guid.Empty)

                                            } // foreach (Entity ChiTietNTTuoiMiaTuoiMia in dsChitietNTTuoimiaTuoimia.Entities)
                                        } // if (dsChitietNTTuoimiaTuoimia != null && dsChitietNTTuoimiaTuoimia.Entities.Count > 0)

                                    }  //foreach (Entity ChiTietNTTuoiMia in dsChitietNTTuoimia.Entities)

                                    dmTienmat = dmHL + dmKHL - dmVattu;

                                    Money MdmHL = new Money(dmHL);
                                    Money MdmKHL = new Money(dmKHL);
                                    Money MdmVattu = new Money(dmVattu);
                                    Money MdmTienmat = new Money(dmTienmat);

                                    en["new_dinhmucdautuhl"] = MdmHL;
                                    en["new_dinhmucdautukhl"] = MdmKHL;
                                    en["new_dinhmuchoanlaivattu"] = MdmVattu;
                                    en["new_dinhmuchoanlaitienmat"] = MdmTienmat;

                                    decimal sotienGN = (NTtuoimiaObj.Contains("new_tongtien") ? NTtuoimiaObj.GetAttributeValue<Money>("new_tongtien").Value : 0);
                                    Money MsotienGN = new Money(sotienGN);

                                    if (ChiTietPDNgiaingan.Contains("new_loaigiaingan"))
                                    {
                                        if (ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_loaigiaingan").Value.ToString() == "100000000")
                                            en["new_sotiendtkhonghoanlai"] = MsotienGN;
                                        else
                                            en["new_sotiendthoanlai"] = MsotienGN;
                                    }

                                    service.Update(en);
                                }
                            }
             // ---END ----  Gắn Dịnh muc cho  Nghiem thu Tuoi mia
                            
             // -------  Gắn Dịnh muc cho  Nghiem thu Boc la mia
                            if (ChiTietPDNgiaingan.Contains("new_noidunggiaingan") && ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_noidunggiaingan").Value.ToString() == "100000002" && ChiTietPDNgiaingan.Attributes.Contains("new_nghiemthuboclamia") && ChiTietPDNgiaingan.Contains("new_loaigiaingan") && ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_loaigiaingan").Value.ToString() == "100000000")
                            {
                                EntityReference NTboclamiaRef = ChiTietPDNgiaingan.GetAttributeValue<EntityReference>("new_nghiemthuboclamia");
                                Guid NTboclamiaId = NTboclamiaRef.Id;
                                Entity NTboclamiaObj = service.Retrieve("new_nghiemthuboclamia", NTboclamiaId, new ColumnSet(new string[] { "new_vudautu" , "new_tongtien" }));

                                EntityCollection dsCTNTboclamia = FindChitietNTBoclamia(service, NTboclamiaObj);
                                if (dsCTNTboclamia != null && dsCTNTboclamia.Entities.Count() > 0)
                                { 
                                    decimal HL = 0;
                                    decimal KHL = 0;
                                    decimal Vattu = 0;

                                    decimal dmHL = 0;
                                    decimal dmKHL = 0;
                                    decimal dmVattu = 0;
                                    decimal dmTienmat = 0;

                                    foreach (Entity ChiTietNTBocLaMia in dsCTNTboclamia.Entities)
                                    {
                                        decimal dtnghiemthu = (ChiTietNTBocLaMia.Contains("new_dientich") ? (decimal)ChiTietNTBocLaMia["new_dientich"] : 0);
                                        DateTime ngaytaoBocla = ChiTietNTBocLaMia.GetAttributeValue<DateTime>("createdon");

                                        EntityReference ctHDDTmiaRef = ChiTietNTBocLaMia.GetAttributeValue<EntityReference>("new_chitiethddtmia");
                                        Guid ctHDDTmiaId = ctHDDTmiaRef.Id;
                                        Entity ChiTietHD = service.Retrieve("new_thuadatcanhtac", ctHDDTmiaId, new ColumnSet(new string[] { "new_vutrong", "new_loaigocmia", "new_mucdichsanxuatmia", "new_giongmia", "new_thuadat", "createdon", "new_hopdongdautumia", "new_khachhang", "new_khachhangdoanhnghiep", "new_thamgiamohinhkhuyennong", "new_dientichthucte" }));

                                        EntityReference thuadatEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_thuadat");
                                        EntityReference giongmiaEntityRef = ChiTietHD.GetAttributeValue<EntityReference>("new_giongmia");
                                        EntityReference vudautuRef = NTboclamiaObj.GetAttributeValue<EntityReference>("new_vudautu");

                                        Guid thuadatId = thuadatEntityRef.Id;
                                        Entity thuadatObj = service.Retrieve("new_thuadat", thuadatId, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy" , "new_diachi" }));

                                        Guid giongmiaId = giongmiaEntityRef.Id;
                                        Entity giongmiaObj = service.Retrieve("new_giongmia", giongmiaId, new ColumnSet(new string[] { "new_nhomgiong", "new_name" }));

                                        Guid vuDTId = vudautuRef.Id;
                                        Entity vuDT = service.Retrieve("new_vudautu", vuDTId, new ColumnSet(new string[] { "new_name" }));

                                        EntityCollection dsCSDTntBoclamia = FindCSDTNTBoclamia(service, ngaytaoBocla, vuDT);
                                        Entity mCSDT = new Entity();

                                        foreach (Entity a in dsCSDTntBoclamia.Entities)
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

                                            mCSDT = a;
                                            break;
                                        }
                                        if (mCSDT != null && mCSDT.Id != Guid.Empty)
                                        {
                                            Guid csdtKQ = mCSDT.Id;
                                            Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name" }));

                                            HL += (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value : 0);
                                            KHL += (csdtKQEntity.Contains("new_dinhmucdautukhonghoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautukhonghoanlai").Value : 0);
                                            Vattu += (csdtKQEntity.Contains("new_dinhmucphanbontoithieu") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucphanbontoithieu").Value : 0);

                                            dmHL = HL;
                                            dmKHL = KHL;
                                            dmVattu = Vattu;

                                        }  //if (mCSDT != null && mCSDT.Id != Guid.Empty)
                                    }

                                    dmTienmat = dmHL + dmKHL - dmVattu;

                                    Money MdmHL = new Money(dmHL);
                                    Money MdmKHL = new Money(dmKHL);
                                    Money MdmVattu = new Money(dmVattu);
                                    Money MdmTienmat = new Money(dmTienmat);

                                    en["new_dinhmucdautuhl"] = MdmHL;
                                    en["new_dinhmucdautukhl"] = MdmKHL;
                                    en["new_dinhmuchoanlaivattu"] = MdmVattu;
                                    en["new_dinhmuchoanlaitienmat"] = MdmTienmat;

                                    decimal sotienGN = (NTboclamiaObj.Contains("new_tongtien") ? NTboclamiaObj.GetAttributeValue<Money>("new_tongtien").Value : 0);
                                    Money MsotienGN = new Money(sotienGN);

                                    if (ChiTietPDNgiaingan.Contains("new_loaigiaingan"))
                                    {
                                        if (ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_loaigiaingan").Value.ToString() == "100000000")
                                            en["new_sotiendtkhonghoanlai"] = MsotienGN;
                                        else
                                            en["new_sotiendthoanlai"] = MsotienGN;
                                    }

                                    

                                    service.Update(en);
                                }
                            }
            // ----END---  Gắn Dịnh muc cho  Nghiem thu Boc la mia

                        } //if (ChiTietPDNgiaingan.Contains("new_noidunggiaingan") && ChiTietPDNgiaingan.Contains("new_hopdongdautumia") && ChiTietPDNgiaingan.Contains("new_loaigiaingan") && ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_loaigiaingan").Value.ToString() == "100000000")

                        // Giai ngan nghiem thu thue dat
                        //if (ChiTietPDNgiaingan.Contains("new_hopdongdaututhuedat") && ChiTietPDNgiaingan.Contains("new_chitietnghiemthuthuedat") && ChiTietPDNgiaingan.Contains("new_noidunggiaingan") && ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_noidunggiaingan").Value.ToString() == "100000000")
                        if (ChiTietPDNgiaingan.Contains("new_hopdongdaututhuedat") && ChiTietPDNgiaingan.Contains("new_chitietnghiemthuthuedat"))
                        {

                            traceService.Trace("Bat dau Giai ngan thue dat ");

                            Entity ctHDTDTD = new Entity("new_chitiethdthuedat_thuadat");
                            Entity ChiTietHDThueDat = new Entity("new_datthue");

                            EntityReference PDNGNref = ChiTietPDNgiaingan.GetAttributeValue<EntityReference>("new_phieudenghigiaingan");
                            Entity PDNGN = service.Retrieve("new_phieudenghigiaingan", PDNGNref.Id, new ColumnSet(new string[] { "new_langiaingan", "new_vudautu", "new_loaihopdong", "new_hopdongdaututhuedat" }));

                            //EntityReference chitietHDTDTDref = ChiTietPDNgiaingan.GetAttributeValue<EntityReference>("new_chitiethdthuedat_thuadat");
                            //Entity chitietHDTDTD = service.Retrieve("new_chitiethdthuedat_thuadat", chitietHDTDTDref.Id, new ColumnSet(new string[] { "new_thuadat", "new_chitiethdthuedat" }));

                            EntityReference ctNTTDRef = ChiTietPDNgiaingan.GetAttributeValue<EntityReference>("new_chitietnghiemthuthuedat");
                            Entity ctNTTD = service.Retrieve("new_chitietnghiemthuthuedat", ctNTTDRef.Id, new ColumnSet(new string[] { "new_nghiemthuthuedat", "new_thuadat" , "new_thoigianthue" }));

                            EntityReference thuadatEntityRef = ctNTTD.GetAttributeValue<EntityReference>("new_thuadat");
                            Entity thuadatObj = service.Retrieve("new_thuadat", thuadatEntityRef.Id, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy" , "new_diachi" }));

                            EntityReference HDDTThuedatRef = ChiTietPDNgiaingan.GetAttributeValue<EntityReference>("new_hopdongdaututhuedat");
                            Entity HDDTThuedat = service.Retrieve("new_hopdongthuedat", HDDTThuedatRef.Id, new ColumnSet(new string[] { "new_vudautu", "new_khachhang", "new_khachhangdoanhnghiep" }));

                            Entity Vudautu = service.Retrieve("new_vudautu", ((EntityReference)HDDTThuedat["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

                            traceService.Trace("Lay thong tin");

                            //if (ChiTietPDNgiaingan.Contains("new_chitiethdthuedat_thuadat"))
                            //{
                            //    traceService.Trace("Lay thong tin new_chitiethdthuedat_thuadat");

                            //    EntityReference ctHDTDTDRef = ChiTietPDNgiaingan.GetAttributeValue<EntityReference>("new_chitiethdthuedat_thuadat");
                            //    ctHDTDTD = service.Retrieve("new_chitiethdthuedat_thuadat", ctHDTDTDRef.Id, new ColumnSet(new string[] { "createdon", "new_chitiethdthuedat", "new_thuadat", "new_dinhmuc", "new_chinhsachdautu", "new_sonamthuedat", "new_dientichthucthue" }));

                            //    EntityReference ctHDDTThuedatRef = ctHDTDTD.GetAttributeValue<EntityReference>("new_chitiethdthuedat");
                            //    ChiTietHDThueDat = service.Retrieve("new_datthue", ctHDDTThuedatRef.Id, new ColumnSet(new string[] { "new_hopdongthuedat" }));
                            //}
                            //else
                            //{
                                if (ctNTTD.Contains("new_thuadat"))
                                {
                                    traceService.Trace("Lay thong tin new_thuadat");

                                    string thuadatId = thuadatObj.Id.ToString();

                                    EntityCollection ctHDTDcol = FindctHDTD(service, HDDTThuedat);

                                    traceService.Trace("So luong ctHDDT " + ctHDTDcol.Entities.Count());

                                    if (ctHDTDcol != null && ctHDTDcol.Entities.Count > 0)
                                    {
                                        foreach (Entity cthdtd in ctHDTDcol.Entities)
                                        {
                                            EntityCollection ctHDTDTDcol = FindctHDTDTD(service, cthdtd);
                                            traceService.Trace("So luong ctHDDTTD " + ctHDTDTDcol.Entities.Count());
                                            if (ctHDTDTDcol != null && ctHDTDTDcol.Entities.Count > 0)
                                            {
                                                foreach (Entity cthdtdtd in ctHDTDTDcol.Entities)
                                                {
                                                    EntityReference tdRef = ChiTietPDNgiaingan.GetAttributeValue<EntityReference>("new_thuadat");
                                                    Entity td = service.Retrieve("new_thuadat", thuadatEntityRef.Id, new ColumnSet(new string[] { "new_nhomdat" }));
                                                    string tdId = td.Id.ToString();
                                                    if (thuadatId == tdId)
                                                    {
                                                        ctHDTDTD = service.Retrieve("new_chitiethdthuedat_thuadat", cthdtdtd.Id, new ColumnSet(new string[] { "createdon", "new_chitiethdthuedat", "new_thuadat", "new_dinhmuc", "new_chinhsachdautu", "new_sonamthuedat", "new_dientichthucthue" }));
                                                        break;
                                                    }
                                                }
                                            }
                                            ChiTietHDThueDat = service.Retrieve("new_datthue", cthdtd.Id, new ColumnSet(new string[] { "new_hopdongthuedat", "new_benchothuedatkh", "new_benchothuedatkhdn" }));
                                            break;
                                        }
                                    }
                                }
                            //}

                            traceService.Trace("Tim CSDT");
                            Entity mCSDT = new Entity();
                            EntityCollection CSDTcol = FindCSDTTD(service, ngaytao, Vudautu);

                            traceService.Trace("So CSDT " + CSDTcol.Entities.Count());

                            if (CSDTcol != null && CSDTcol.Entities.Count > 0)
                            {
                                foreach (Entity a in CSDTcol.Entities)
                                {
                                    if (a.Contains("new_nhomdat_vl"))  // Nhom dat
                                    {
                                        if (thuadatObj.Attributes.Contains("new_nhomdat"))
                                        {
                                            if (a["new_nhomdat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_nhomdat"]).Value.ToString()) == -1)
                                                continue;
                                        }
                                    }

                                    if (a.Contains("new_sonamthue"))  // So nam thue
                                    {
                                        if (ctHDTDTD.Contains("new_sonamthuedat"))
                                        {
                                            int sonamthueCSDT = (int)a["new_sonamthue"];
                                            int sonamthueCTHDTD = (int)ctHDTDTD["new_sonamthuedat"];
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
                                    traceService.Trace("DK nhom KH");

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

                                        if (dsNHomCL == null || dsNHomCL.Entities.Count() == 0)
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
                                    if (HDDTThuedat.Attributes.Contains("new_khachhangdoanhnghiep"))
                                    {
                                        Guid khId = HDDTThuedat.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
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
                            }

                            if (mCSDT != null && mCSDT.Id != Guid.Empty)
                            {
                                traceService.Trace("Tim duoc CSDT Thue dat");
                                // ------Gan vao Chi tiet PDN giải ngân 
                                EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                                en["new_chinhsachdautu"] = csdtRef;

                                traceService.Trace("Gan CSDT Thue dat");

                                // ------Gan vao Chi tiet NT thuê đất
                                //ctNTTD["new_chinhsachdautu"] = csdtRef;
                                //service.Update(ctNTTD);

                                Guid csdtKQ = mCSDT.Id;
                                Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name", "new_muclaisuatdautu", "new_dinhmucdautukhonghoanlai", "new_sonamthue" }));
                                int sonam = (ctNTTD.Contains("new_thoigianthue") ? (int)ctNTTD["new_thoigianthue"] : 0);
                                int langiainganPDNGN = (PDNGN.Contains("new_langiaingan") ? (int)PDNGN["new_langiaingan"] : 0);
                                decimal tyle = 0;
                                traceService.Trace("lan GN " + langiainganPDNGN);
                                traceService.Trace("so nam " + sonam);

                                // Tim dinh muc dau tu
                                EntityCollection DinhmucDTTMcol = FindDaututienmat(service, csdtKQEntity);
                                if (DinhmucDTTMcol != null && DinhmucDTTMcol.Entities.Count > 0)
                                {
                                    foreach (Entity dmdttm in DinhmucDTTMcol.Entities)
                                    {
                                        int langn = (dmdttm.Contains("new_langiaingan") ? (int)dmdttm["new_langiaingan"] : 10);
                                        traceService.Trace("lan GN DMDT" + langn);
                                        if (langn == langiainganPDNGN)
                                        {
                                            tyle = (dmdttm.Contains("new_phantramtilegiaingan") ? (decimal)dmdttm["new_phantramtilegiaingan"] : 0);
                                            break;
                                        }
                                    }
                                }

                                traceService.Trace("ty le " + tyle);
                                decimal tienHL = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? ((Money)csdtKQEntity["new_dinhmucdautuhoanlai"]).Value : 0) * tyle * sonam / 100;
                                decimal tienKHL = (csdtKQEntity.Contains("new_dinhmucdautukhonghoanlai") ? ((Money)csdtKQEntity["new_dinhmucdautukhonghoanlai"]).Value : 0) * tyle * sonam / 100;

                                Money MdmHL = new Money(tienHL);
                                Money MdmKHL = new Money(tienKHL);

                                //en["new_dinhmucdautuhl"] = MdmHL;
                                //en["new_dinhmucdautukhl"] = MdmKHL;

                                traceService.Trace("HL " + tienHL);

                                en["new_phantramtylegiaingan"] = tyle;

                                if (ChiTietPDNgiaingan.Contains("new_loaidautu") && ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_loaidautu").Value.ToString() == "100000001")
                                {
                                    en["new_dinhmucdautuhl"] = MdmHL;
                                }
                                if (ChiTietPDNgiaingan.Contains("new_loaidautu") && ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_loaidautu").Value.ToString() == "100000000")
                                {
                                    en["new_dinhmucdautukhl"] = MdmKHL;
                                }

                                service.Update(en);
                            }

                        } // if (ChiTietPDNgiaingan.Contains("new_hopdongdaututhuedat") && ChiTietPDNgiaingan.Contains("new_chitietnghiemthuthuedat"))

                        // Nghiem thu may moc thiet bi
                        //if (ChiTietPDNgiaingan.Contains("new_hopdongdautummtb") && ChiTietPDNgiaingan.Contains("new_nghiemthummtb") && ChiTietPDNgiaingan.Contains("new_chitiethddttrangthietbi") && ChiTietPDNgiaingan.Contains("new_chitietnghiemthumaymocthietbi") && ChiTietPDNgiaingan.Contains("new_noidunggiaingan") && ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_noidunggiaingan").Value.ToString() == "100000000")
                        if (ChiTietPDNgiaingan.Contains("new_hopdongdautummtb") && ChiTietPDNgiaingan.Contains("new_nghiemthummtb") && ChiTietPDNgiaingan.Contains("new_chitietnghiemthumaymocthietbi"))
                        {
                            EntityReference ChiTietNTTrangThietBiRef = ChiTietPDNgiaingan.GetAttributeValue<EntityReference>("new_chitietnghiemthumaymocthietbi");
                            Entity ChiTietNTTrangThietBi = service.Retrieve("new_chitietnghiemthummtb", ChiTietNTTrangThietBiRef.Id, new ColumnSet(new string[] { "new_giamua" }));

                            EntityReference PDNGNref = ChiTietPDNgiaingan.GetAttributeValue<EntityReference>("new_phieudenghigiaingan");
                            Entity PDNGN = service.Retrieve("new_phieudenghigiaingan", PDNGNref.Id, new ColumnSet(new string[] { "new_langiaingan", "new_vudautu", "new_loaihopdong" }));

                            EntityReference hddtTTBEntityRef = ChiTietPDNgiaingan.GetAttributeValue<EntityReference>("new_hopdongdautummtb");
                            Entity hddtTTBObj = service.Retrieve("new_hopdongdaututrangthietbi", hddtTTBEntityRef.Id, new ColumnSet(new string[] { "new_vudautu", "new_doitaccungcap", "new_doitaccungcapkhdn" }));

                            Entity Vudautu = service.Retrieve("new_vudautu", ((EntityReference)hddtTTBObj["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

                            EntityCollection listCSDTMMTB = FindCSDTMMTB(service, ngaytao, Vudautu);

                            Entity mCSDT = new Entity();
                            //Entity enMMTB = new Entity(ChiTietHDDTTrangThietBi.LogicalName);
                            //enMMTB.Id = ChiTietHDDTTrangThietBi.Id;

                            if (listCSDTMMTB != null && listCSDTMMTB.Entities.Count > 0)
                            {
                                foreach (Entity a in listCSDTMMTB.Entities)
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
                            }
                            else
                                throw new InvalidPluginExecutionException("Chưa có Chính sách Đầu tư Trang thiết bị nào cho vụ đầu tư này");

                            if (mCSDT != null && mCSDT.Id != Guid.Empty)
                            {
                                EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                                //enMMTB.Attributes["new_chinhsachdautu"] = csdtRef;
                                //service.Update(enMMTB);

                                //EntityCollection oldlTLTHVDK = FindTLTHVDKMMTB(service, ChiTietHDDTTrangThietBi);
                                //if (oldlTLTHVDK != null && oldlTLTHVDK.Entities.Count > 0)
                                //{
                                //    foreach (Entity a in oldlTLTHVDK.Entities)
                                //    {
                                //        service.Delete("new_tylethuhoivondukien", a.Id);
                                //    }
                                //}
                                // -------Gan ty le thu hoi von du kien
                                // Lay nhung tylethuhoivon trong chinh sach dau tu
                                //string fetchTLTHV =
                                //@"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                //  <entity name='new_tilethuhoivon'>
                                //       <attribute name='new_name' />
                                //       <attribute name='new_phantramtilethuhoi' />
                                //       <attribute name='new_vuthuhoi' />
                                //       <attribute name='new_chinhsachdautu' />
                                //       <attribute name='new_tilethuhoivonid' />
                                //       <order attribute='new_nam' descending='false' />
                                //       <link-entity name='new_chinhsachdautu' from='new_chinhsachdautuid' to='new_chinhsachdautu' alias='ac'>
                                //           <filter type='and'>
                                //                 <condition attribute='new_chinhsachdautuid' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                                //           </filter>
                                //       </link-entity>
                                //  </entity>
                                //</fetch>";



                                //fetchTLTHV = string.Format(fetchTLTHV, csdtKQ);
                                //EntityCollection collTLTHV = service.RetrieveMultiple(new FetchExpression(fetchTLTHV));

                                Guid csdtKQ = mCSDT.Id;
                                Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name", "new_loailaisuatcodinhthaydoi", "new_muclaisuatdautu", "new_cachtinhlai", "new_dinhmucdautukhonghoanlai" }));

                                //foreach (Entity TLTHV in collTLTHV.Entities)
                                //{
                                //    Entity tlthvdkHDCT = new Entity("new_tylethuhoivondukien");

                                //    EntityReference vudautuEntityRef = hddtTTBObj.GetAttributeValue<EntityReference>("new_vudautu");
                                //    EntityReference hdctEntityRef = new EntityReference("new_hopdongdaututrangthietbichitiet", ChiTietHDDTTrangThietBi.Id);

                                //    if (ChiTietHDDTTrangThietBi.Attributes.Contains("new_giatrihopdong") && TLTHV.Attributes.Contains("new_phantramtilethuhoi") && TLTHV.Attributes.Contains("new_vuthuhoi"))
                                //    {
                                //        EntityReference vdtRef = TLTHV.GetAttributeValue<EntityReference>("new_vuthuhoi");
                                //        Entity vdt = service.Retrieve("new_vudautu", vdtRef.Id, new ColumnSet(new string[] { "new_name" }));

                                //        string tenvdt = vdt["new_name"].ToString();
                                //        string tenTLTHVDK = "Tỷ lệ thu hồi " + tenvdt;

                                //        decimal tyleth = (TLTHV.Contains("new_phantramtilethuhoi") ? (decimal)TLTHV["new_phantramtilethuhoi"] : 0);
                                //        decimal giatrihopdong = ChiTietHDDTTrangThietBi.GetAttributeValue<Money>("new_giatrihopdong").Value;

                                //        decimal sotien = 0;

                                //        sotien = (giatrihopdong * tyleth) / 100;
                                //        Money sotienM = new Money(sotien);

                                //        tlthvdkHDCT.Attributes.Add("new_name", tenTLTHVDK);
                                //        tlthvdkHDCT.Attributes.Add("new_loaityle", new OptionSetValue(100000002));
                                //        tlthvdkHDCT.Attributes.Add("new_chitiethddttrangthietbi", hdctEntityRef);
                                //        tlthvdkHDCT.Attributes.Add("new_vudautu", vudautuEntityRef);
                                //        tlthvdkHDCT.Attributes.Add("new_tylephantram", tyleth);
                                //        tlthvdkHDCT.Attributes.Add("new_sotienthuhoi", sotienM);

                                //        service.Create(tlthvdkHDCT);
                                //    }
                                //}
                                // ------End Gan vao ty le thu hoi von du kien

                                // -------- Gan Gia tri toi da
                                decimal giatrithietbi = (ChiTietNTTrangThietBi.Contains("new_giamua") ? ChiTietNTTrangThietBi.GetAttributeValue<Money>("new_giamua").Value : 0);
                                decimal DMDT = 0;
                                decimal phantramtyle = 0;

                                EntityCollection dmUngvonDTcol = FindDMUVMMTB(service, csdtKQEntity);
                                Entity dmUngvonDT = null;
                                decimal tu = 0;
                                decimal den = 0;

                                foreach (Entity a in dmUngvonDTcol.Entities)
                                {
                                    if (a.Attributes.Contains("new_giatritu") && a.Attributes.Contains("new_giatriden"))
                                    {
                                        string pttu = a.GetAttributeValue<OptionSetValue>("new_phuongthuctinhtu").Value.ToString();
                                        string ptden = a.GetAttributeValue<OptionSetValue>("new_phuongthuctinhden").Value.ToString();
                                        tu = a.GetAttributeValue<decimal>("new_giatritu");
                                        den = a.GetAttributeValue<decimal>("new_giatriden");

                                        // 100000000
                                        if (pttu == "100000000" && ptden == "100000000")
                                        {
                                            if ((giatrithietbi == tu) && (giatrithietbi == den))    //  1.     = và =
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000000" && ptden == "100000001")
                                        {
                                            if ((giatrithietbi == tu) && (giatrithietbi < den))    // 2.      = và <
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000000" && ptden == "100000002")
                                        {
                                            if ((giatrithietbi == tu) && (giatrithietbi > den))     // 3.      = và >
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000000" && ptden == "100000003")
                                        {
                                            if ((giatrithietbi == tu) && (giatrithietbi <= den))     // 4.     = và <=
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000000" && ptden == "100000004")
                                        {
                                            if ((giatrithietbi == tu) && (giatrithietbi >= den))     // 5.     = và >=
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }

                                        // 100000001
                                        if (pttu == "100000001" && ptden == "100000000")
                                        {
                                            if ((giatrithietbi < tu) && (giatrithietbi == den))     //  1.     < và =
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000001" && ptden == "100000001")
                                        {
                                            if ((giatrithietbi < tu) && (giatrithietbi < den))     // 2.      < và <
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000001" && ptden == "100000002")
                                        {
                                            if ((giatrithietbi < tu) && (giatrithietbi > den))     // 3.      < và >
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000001" && ptden == "100000003")
                                        {
                                            if ((giatrithietbi < tu) && (giatrithietbi <= den))     // 4.     < và <=
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000001" && ptden == "100000004")
                                        {
                                            if ((giatrithietbi < tu) && (giatrithietbi >= den))     // 5.     < và >=
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }

                                        // 100000002
                                        if (pttu == "100000002" && ptden == "100000000")   //  1.     > và =
                                        {
                                            if ((giatrithietbi > tu) && (giatrithietbi == den))
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000002" && ptden == "100000001")   // 2.      > và <
                                        {
                                            if ((giatrithietbi > tu) && (giatrithietbi < den))
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000002" && ptden == "100000002")
                                        {
                                            if ((giatrithietbi > tu) && (giatrithietbi > den))     // 3.      > và >
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000002" && ptden == "100000003")
                                        {
                                            if ((giatrithietbi > tu) && (giatrithietbi <= den))     // 4.     > và <=
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000002" && ptden == "100000004")
                                        {
                                            if ((giatrithietbi > tu) && (giatrithietbi >= den))     // 5.     > và >=
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }

                                        // 100000003
                                        if (pttu == "100000003" && ptden == "100000000")   //  1.     <= và =
                                        {
                                            if ((giatrithietbi <= tu) && (giatrithietbi == den))
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000003" && ptden == "100000001")   // 2.      <= và <
                                        {
                                            if ((giatrithietbi <= tu) && (giatrithietbi < den))
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000003" && ptden == "100000002")
                                        {
                                            if ((giatrithietbi <= tu) && (giatrithietbi > den))     // 3.      <= và >
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000003" && ptden == "100000003")
                                        {
                                            if ((giatrithietbi <= tu) && (giatrithietbi <= den))     // 4.     <= và <=
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000003" && ptden == "100000004")
                                        {
                                            if ((giatrithietbi <= tu) && (giatrithietbi >= den))     // 5.     <= và >=
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }

                                        // 100000004
                                        if (pttu == "100000004" && ptden == "100000000")   //  1.     >= và =
                                        {
                                            if ((giatrithietbi >= tu) && (giatrithietbi == den))
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000004" && ptden == "100000001")   // 2.      >= và <
                                        {
                                            if ((giatrithietbi >= tu) && (giatrithietbi < den))
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000004" && ptden == "100000002")
                                        {
                                            if ((giatrithietbi >= tu) && (giatrithietbi > den))     // 3.      >= và >
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000004" && ptden == "100000003")
                                        {
                                            if ((giatrithietbi >= tu) && (giatrithietbi <= den))     // 4.     >= và <=
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                        if (pttu == "100000004" && ptden == "100000004")
                                        {
                                            if ((giatrithietbi >= tu) && (giatrithietbi >= den))     // 5.     >= và >=
                                            {
                                                dmUngvonDT = a;
                                                break;
                                            }
                                        }
                                    }
                                }

                                if (dmUngvonDT != null && dmUngvonDT.Id != Guid.Empty)
                                {
                                    phantramtyle = (dmUngvonDT.Contains("new_phantramgiatri") ? dmUngvonDT.GetAttributeValue<decimal>("new_phantramgiatri") : 0);
                                }

                                traceService.Trace("Phan tram MMTB " + phantramtyle);

                                DMDT = (giatrithietbi * phantramtyle) / 100;

                                traceService.Trace("DM MMTB " + DMDT);
                                //Money dinhmucDT = new Money(DMDT);
                                //enMMTB["new_giatritoida"] = dinhmucDT;

                                //service.Update(enMMTB);

                                // Gan CSDT vao chi tiet PDN GN
                                en["new_chinhsachdautu"] = csdtRef;

                                // -------- End Gan Gia tri toi da

                                // Gan DM cho phieu de nghi Gian ngan
                                int langiainganPDNGN = (PDNGN.Contains("new_langiaingan") ? (int)PDNGN["new_langiaingan"] : 0);
                                decimal tyle = 0;

                                traceService.Trace("langiainganPDNGN la " + langiainganPDNGN);

                                // Tim dinh muc dau tu
                                EntityCollection DinhmucDTTMcol = FindDaututienmat(service, csdtKQEntity);

                                traceService.Trace("so DMDT la " + DinhmucDTTMcol.Entities.Count());

                                if (DinhmucDTTMcol != null && DinhmucDTTMcol.Entities.Count > 0)
                                {
                                    foreach (Entity dmdttm in DinhmucDTTMcol.Entities)
                                    {
                                        int langn = (dmdttm.Contains("new_langiaingan") ? (int)dmdttm["new_langiaingan"] : 10);
                                        if (langn == langiainganPDNGN)
                                        {
                                            tyle = (dmdttm.Contains("new_phantramtilegiaingan") ? (decimal)dmdttm["new_phantramtilegiaingan"] : 0);
                                            break;
                                        }
                                    }
                                }

                                traceService.Trace("ty le " + tyle);

                                decimal tien = DMDT * tyle / 100;

                                traceService.Trace("tien " + tien);

                                if (ChiTietPDNgiaingan.Contains("new_loaidautu") && ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_loaidautu").Value.ToString() == "100000001")
                                {
                                    en["new_dinhmucdautuhl"] = new Money(tien);
                                }
                                if (ChiTietPDNgiaingan.Contains("new_loaidautu") && ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_loaidautu").Value.ToString() == "100000000")
                                {
                                    en["new_dinhmucdautukhl"] = new Money(tien);
                                }

                                service.Update(en);
                            }
                        } // if (ChiTietPDNgiaingan.Contains("new_hopdongdautummtb") && ChiTietPDNgiaingan.Contains("new_nghiemthummtb") && ChiTietPDNgiaingan.Contains("new_chitiethddttrangthietbi") && ChiTietPDNgiaingan.Contains("new_chitietnghiemthumaymocthietbi") && ChiTietPDNgiaingan.Contains("new_noidunggiaingan") && ChiTietPDNgiaingan.GetAttributeValue<OptionSetValue>("new_noidunggiaingan").Value.ToString() == "100000000")
                        //else
                        //    throw new InvalidPluginExecutionException("Thiếu NT MMTB/ chi tiết NT MMTB");

                    }  // if (context.MessageName.ToUpper() == "CREATE")
                }
            }
        }

        public static EntityCollection FindChitietNTTrongmia(IOrganizationService crmservices, Entity NTTrongmia)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_chitietnghiemthutrongmia'>
                        <attribute name='new_name' />
                        <attribute name='new_nghiemthutrongmia' />
                        <attribute name='new_ngaytrongxulygoc' />
                        <attribute name='new_hopdongdautumia' />
                        <attribute name='new_giongmia' />
                        <attribute name='new_thuadat' />
                        <attribute name='new_luugoc' />
                        <attribute name='createdon' />
                        <attribute name='new_dientichnghiemthu' />
                        <attribute name='new_chitietnghiemthutrongmiaid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_nghiemthutrongmia' operator='eq' uitype='new_nghiemthutrongmia' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, NTTrongmia.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindChitietNTTuoimiaTuoimia(IOrganizationService crmservices, Entity ChitietNTTuoimia)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_chitietnghiemthutuoimia_tuoimia'>
                        <attribute name='new_name' />
                        <attribute name='createdon' />
                        <attribute name='new_tuoimia' />
                        <attribute name='new_chitietnghiemthutuoimia_tuoimiaid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_chitietnghiemthutuoimia' operator='eq' uitype='new_chitietnghiemthutuoimia' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, ChitietNTTuoimia.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindChitietNTTuoimia(IOrganizationService crmservices, Entity NTTuoimia)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_chitietnghiemthutuoimia'>
                        <attribute name='new_name' />
                        <attribute name='createdon' />
                        <attribute name='new_solantuoi' />
                        <attribute name='new_nghiemthutuoimia' />
                        <attribute name='new_ngaytuoi' />
                        <attribute name='new_dientichtuoi' />
                        <attribute name='new_thuadatcanhtac' />
                        <attribute name='new_chitietnghiemthutuoimiaid' />
                        <attribute name='new_thuadat' />
                        <attribute name='new_phuongphaptuoi' />
                        <attribute name='new_mucdichtuoi' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_nghiemthutuoimia' operator='eq' uitype='new_nghiemthutuoimia' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, NTTuoimia.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindChitietNTBoclamia(IOrganizationService crmservices, Entity NTBoclamia)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_nghiemthuboclamiathuadat'>
                        <attribute name='new_name' />
                        <attribute name='new_nghiemthuboclamia' />
                        <attribute name='new_hopdongdautumia' />
                        <attribute name='new_chitiethddtmia' />
                        <attribute name='new_dientich' />
                        <attribute name='createdon' />
                        <attribute name='new_nghiemthuboclamiathuadatid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_nghiemthuboclamia' operator='eq' uitype='new_nghiemthuboclamia' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, NTBoclamia.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindCSDTNTTrongmia(IOrganizationService crmservices, DateTime ngaytao, Entity VuDT)
        {
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
                                        <attribute name='new_dinhmucphanbontoithieu' />
                                        <attribute name='new_loaigocmia_vl' />
                                        <attribute name='new_nhomgiongmia_vl' />
                                        <attribute name='new_nhomdat_vl' />
                                        <attribute name='new_vutrong_vl' />
                                        <attribute name='new_mucdichsanxuatmia_vl' />
                                        <attribute name='new_loaisohuudat_vl' />
                                        <attribute name='new_luugoc' />
                                        <attribute name='new_chinhsachdautuid' />
                                        <order attribute='new_ngayapdung' descending='true' />
                                        <filter type='and'>
                                          <condition attribute='statecode' operator='eq' value='0' />
                                          <condition attribute='new_loaihopdong' operator='eq' value='100000000' />
                                          <condition attribute='new_mucdichdautu' operator='eq' value='100000000' />
                                          <condition attribute='new_ngayapdung' operator='on-or-before' value='{0}' />    
                                          <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{1}' />                                   
                                        </filter>
                                      </entity>
                                    </fetch>";

            fetchXml = string.Format(fetchXml, ngaytao, VuDT.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindCSDTNTThamcanh(IOrganizationService crmservices, DateTime ngaytao, Entity VuDT)
        {
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
                                            <attribute name='new_dinhmucphanbontoithieu' />
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

            fetchXml = string.Format(fetchXml, ngaytao, VuDT.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindCSDTNTTuoimia(IOrganizationService crmservices, DateTime ngaytao, Entity VuDT)
        {
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
                                        <attribute name='new_dinhmucphanbontoithieu' />
                                        <attribute name='new_mucdichtuoi_vl' />
                                        <attribute name='new_phuongphaptuoi_vl' /> 
                                        <attribute name='new_nhomdat_vl' />
                                        <attribute name='new_vutrong_vl' />
                                        <attribute name='new_mucdichsanxuatmia_vl' />
                                        <attribute name='new_loaisohuudat_vl' />
                                        <attribute name='new_chinhsachdautuid' />
                                        <order attribute='new_ngayapdung' descending='true' />
                                        <filter type='and'>
                                          <condition attribute='statecode' operator='eq' value='0' />
                                          <condition attribute='new_loaihopdong' operator='eq' value='100000000' />
                                          <condition attribute='new_mucdichdautu' operator='eq' value='100000002' />
                                          <condition attribute='new_ngayapdung' operator='on-or-before' value='{0}' />
                                          <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{1}' />                                      
                                        </filter>
                                      </entity>
                                    </fetch>";

            fetchXml = string.Format(fetchXml, ngaytao, VuDT.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindCSDTNTBoclamia(IOrganizationService crmservices, DateTime ngaytao, Entity VuDT)
        {
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
                                        <attribute name='new_dinhmucphanbontoithieu' />
                                        <attribute name='new_loaigocmia_vl' />
                                        <attribute name='new_nhomdat_vl' />
                                        <attribute name='new_vutrong_vl' />
                                        <attribute name='new_mucdichsanxuatmia_vl' />
                                        <attribute name='new_nhomgiongmia_vl' />
                                        <attribute name='new_loaisohuudat_vl' />
                                        <attribute name='new_chinhsachdautuid' />
                                        <order attribute='new_ngayapdung' descending='true' />
                                        <filter type='and'>
                                          <condition attribute='new_loaihopdong' operator='eq' value='100000000' />
                                          <condition attribute='new_mucdichdautu' operator='eq' value='100000003' />
                                          <condition attribute='new_ngayapdung' operator='on-or-before' value='{0}' />
                                          <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{1}' />                                       
                                        </filter>
                                      </entity>
                                    </fetch>";                   

            fetchXml = string.Format(fetchXml, ngaytao, VuDT.Id);
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

        public static EntityCollection FindctHDTD(IOrganizationService crmservices, Entity HDTD)
        {
            string fetchXml =
                   @"<fetch version = '1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name = 'new_datthue' >
                        <attribute name='new_sotienthuedat' />
                        <attribute name = 'new_sotiendautu' />
                        <attribute name='new_dientichthucthue' />
                        <attribute name = 'new_dientichhopdong' />
                        <attribute name='new_benchothuedatkh' />
                        <attribute name = 'createdon' />
                        <attribute name='new_datthueid' />
                        <order attribute = 'createdon' descending='true' />
                        <filter type = 'and' >
                          <condition attribute='new_hopdongthuedat' operator='eq' uitype='new_hopdongthuedat' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, HDTD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindctHDTDTD(IOrganizationService crmservices, Entity ctHDTD)
        {
            string fetchXml =
                   @"<fetch version = '1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name = 'new_chitiethdthuedat_thuadat' >
                        <attribute name='new_thuadat' />
                        <attribute name = 'new_sotiendaututhucte' />
                        <attribute name='new_sotiendautu' />
                        <attribute name = 'new_sonamthuedat' />
                        <attribute name='new_hientrangcaytrong' />
                        <attribute name = 'new_dientichthuehd' />
                        <attribute name='new_dientichthucthue' />
                        <attribute name = 'new_chitiethdthuedat_thuadatid' />
                        <order attribute='new_thuadat' descending='false' />
                        <filter type = 'and' >
                          <condition attribute='new_chitiethdthuedat' operator='eq' uitype='new_datthue' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, ctHDTD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindCSDTTD(IOrganizationService crmservices, DateTime ngaytao, Entity Vudt)
        {
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

            fetchXml = string.Format(fetchXml, ngaytao, Vudt.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindCSDTMMTB(IOrganizationService crmservices, DateTime ngaytao, Entity Vudt)
        {
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
                                        <attribute name='new_loailaisuatcodinhthaydoi' />
                                        <attribute name='new_muclaisuatdautu' />
                                        <attribute name='new_cachtinhlai' />
                                        <attribute name='new_chinhsachdautuid' />
                                        <order attribute='new_ngayapdung' descending='true' />
                                        <filter type='and'>
                                          <condition attribute='new_loaihopdong' operator='eq' value='100000002' />
                                          <condition attribute='new_mucdichdautu' operator='eq' value='100000006' />
                                          <condition attribute='new_ngayapdung' operator='on-or-before' value='{0}' />
                                          <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{1}' />                                       
                                        </filter>
                                      </entity>
                                    </fetch>";

            fetchXml = string.Format(fetchXml, ngaytao, Vudt.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindTLTHVDKMMTB(IOrganizationService crmservices, Entity chitietHDttb)
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
                        <attribute name='new_chitiethdthuedat_thuadat' />
                        <attribute name='new_chitiethddtmia' />
                        <attribute name='new_tylethuhoivondukienid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_chitiethddttrangthietbi' operator='eq' uitype='new_hopdongdaututrangthietbichitiet' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, chitietHDttb.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindDMUVMMTB(IOrganizationService crmservices, Entity csdtMMTB)
        {
            string fetchXml =
              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_dinhmucungvondautummtb'>
                    <attribute name='new_dinhmucungvondautummtbid' />
                    <attribute name='new_name' />
                    <attribute name='createdon' />
                    <attribute name='new_giatritu' />
                    <attribute name='new_phuongthuctinhtu' />
                    <attribute name='new_phuongthuctinhden' />
                    <attribute name='new_giatriden' />
                    <attribute name='new_phantramgiatri' />
                    <attribute name='new_chinhsachdautu' />
                    <order attribute='new_name' descending='false' />
                    <filter type='and'>
                      <condition attribute='new_chinhsachdautu' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, csdtMMTB.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }
        EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
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
            EntityCollection collRecords = service.RetrieveMultiple(query);

            return collRecords;
        }
    }
}
