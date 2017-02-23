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

namespace Plugin_ThemCSDTvaoNTBocLaMia
{
    public sealed class Plugin_ThemCSDTvaoNTBocLaMiaUpdate : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            traceService.Trace(string.Format("Context Depth {0}", context.Depth));
            if (context.Depth > 1)
                return;

            if (context.InputParameters.Contains("Target") && (context.InputParameters["Target"] is Entity))
            {
                Entity ChiTietNTBocLaMia = (Entity)context.InputParameters["Target"];
                Guid entityId = ChiTietNTBocLaMia.Id;

                if (ChiTietNTBocLaMia.LogicalName == "new_nghiemthuboclamiathuadat")
                {
                    //traceService.Trace("Begin plugin");
                    if (context.MessageName.ToUpper() == "UPDATE")
                    {
                        ChiTietNTBocLaMia = service.Retrieve("new_nghiemthuboclamiathuadat", entityId, new ColumnSet(new string[] { "new_name", "new_nghiemthuboclamia", "new_hopdongdautumia", "new_chitiethddtmia", "new_thuadat", "createdon" , "new_dientich" }));
                        DateTime ngaytao = ChiTietNTBocLaMia.GetAttributeValue<DateTime>("createdon");

                        if(!ChiTietNTBocLaMia.Contains("new_nghiemthuboclamia") || !ChiTietNTBocLaMia.Contains("new_chitiethddtmia"))
                        { 
                            throw new InvalidPluginExecutionException("Thiếu thông tin bắt buộc về thửa đất / giống mía / vụ đầu tư");
                        }
                        else
                        {
                            EntityReference NTboclamiaRef = ChiTietNTBocLaMia.GetAttributeValue<EntityReference>("new_nghiemthuboclamia");
                            Guid NTboclamiaId = NTboclamiaRef.Id;
                            Entity NTboclamia = service.Retrieve("new_nghiemthuboclamia", NTboclamiaId, new ColumnSet(new string[] { "new_vudautu" }));

                            traceService.Trace("NT boc la mia");

                            Guid ctHDDTmiaId = new Guid();
                            Entity ctHDDTmia = new Entity();
                            if (ChiTietNTBocLaMia.Contains("new_chitiethddtmia"))
                            {
                                EntityReference ctHDDTmiaRef = ChiTietNTBocLaMia.GetAttributeValue<EntityReference>("new_chitiethddtmia");
                                ctHDDTmiaId = ctHDDTmiaRef.Id;
                                ctHDDTmia = service.Retrieve("new_thuadatcanhtac", ctHDDTmiaId, new ColumnSet(new string[] { "new_vutrong", "new_loaisohuudat", "new_loaigocmia", "new_mucdichsanxuatmia", "new_giongmia", "new_thuadat", "createdon", "new_hopdongdautumia", "new_khachhang", "new_khachhangdoanhnghiep", "new_thamgiamohinhkhuyennong", "new_dientichthucte", "new_tuoimia", "new_dientichhopdong", "new_dinhmucphanbontoithieu", "new_chinhsachdautu", "new_copytuhddtthuedat", "new_luugoc", "new_dautuhoanlai" }));
                            }
                            else
                                throw new InvalidPluginExecutionException("Thiếu thông tin chi tiết HĐĐT mía");

                            traceService.Trace("chi tiet HDDT mia");

                            Guid thuadatId = new Guid();
                            Entity thuadatObj = new Entity();
                            if (ctHDDTmia.Contains("new_thuadat"))
                            {
                                EntityReference thuadatEntityRef = ctHDDTmia.GetAttributeValue<EntityReference>("new_thuadat");
                                thuadatId = thuadatEntityRef.Id;
                                thuadatObj = service.Retrieve("new_thuadat", thuadatId, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy", "new_diachi" }));
                            }
                            else
                                throw new InvalidPluginExecutionException("Thiếu thông tin thửa đất");

                            traceService.Trace("Thua dat");

                            Guid giongmiaId = new Guid();
                            Entity giongmiaObj = new Entity();
                            if (ctHDDTmia.Contains("new_giongmia"))
                            {
                                EntityReference giongmiaEntityRef = ctHDDTmia.GetAttributeValue<EntityReference>("new_giongmia");
                                giongmiaId = giongmiaEntityRef.Id;
                                giongmiaObj = service.Retrieve("new_giongmia", giongmiaId, new ColumnSet(new string[] { "new_nhomgiong", "new_name" }));
                            }
                            else
                                throw new InvalidPluginExecutionException("Thiếu thông tin giống mía");
                            traceService.Trace("Giong mia");

                            EntityReference vudautuRef = null;
                            if(NTboclamia.Contains("new_vudautu"))
                                vudautuRef = NTboclamia.GetAttributeValue<EntityReference>("new_vudautu");

                            Guid vuDTId = Guid.Empty;
                            if (vudautuRef != null)
                                vuDTId = vudautuRef.Id;

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
                                          <condition attribute='new_loaihopdong' operator='eq' value='100000000' />
                                          <condition attribute='new_mucdichdautu' operator='eq' value='100000003' />
                                          <condition attribute='new_ngayapdung' operator='le' value='{0}' />
                                          <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{1}' />                                       
                                        </filter>
                                      </entity>
                                    </fetch>";

                            fetchXml = string.Format(fetchXml, ngaytao, vuDTId);
                            EntityCollection result = service.RetrieveMultiple(new FetchExpression(fetchXml));
                            List<Entity> CSDT = result.Entities.ToList<Entity>();

                            Entity mCSDT = null;
                            if (CSDT != null && CSDT.Count() > 0)
                            {
                                foreach (Entity a in CSDT)
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
                                    if (ctHDDTmia.Attributes.Contains("new_khachhang"))
                                    {
                                        EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                        Guid khId = ctHDDTmia.GetAttributeValue<EntityReference>("new_khachhang").Id;
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

                                    if (ctHDDTmia.Attributes.Contains("new_khachhangdoanhnghiep"))
                                    {
                                        EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                        Guid khId = ctHDDTmia.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
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
                                                if (n.Contains("new_path") && n["new_path"] != null)
                                                {
                                                    traceService.Trace("n co new_path " + n["new_path"].ToString());
                                                    if (diachi["new_path"].ToString().Contains(n["new_path"].ToString()))
                                                    {
                                                        co = true;
                                                        break;
                                                    }
                                                }
                                                else
                                                {
                                                    traceService.Trace("n KHONG co new_path ");
                                                    co = false;
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
                            else
                                throw new InvalidPluginExecutionException("Chưa có Chính sách Đầu tư NT bóc lá mía nào cho vụ đầu tư này");

                            if (mCSDT != null && mCSDT.Id != Guid.Empty)
                            {
                                // ------Gan vao Chi tiet HDDT mia
                                Entity en = new Entity(ChiTietNTBocLaMia.LogicalName);
                                en.Id = ChiTietNTBocLaMia.Id;

                                EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                                en["new_chinhsachdautu"] = csdtRef;

                                // -------- Gan nhom du lieu  Dinh muc

                                Guid csdtKQ = mCSDT.Id;
                                Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_dinhmucdautukhonghoanlai" }));

                                decimal dientich = (ChiTietNTBocLaMia.Contains("new_dientich") ? ChiTietNTBocLaMia.GetAttributeValue<decimal>("new_dientich") : 0);

                                decimal dongiaHL = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value : 0);
                                decimal dongiaKHL = (csdtKQEntity.Contains("new_dinhmucdautukhonghoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautukhonghoanlai").Value : 0);
                                decimal tiendautu = dongiaKHL * dientich;

                                //decimal tongDG = dongiaHL + dongiaKHL;
                                en["new_dongia"] = new Money(dongiaKHL);
                                en["new_dinhmuc"] = new Money(dongiaKHL);
                                en["new_sotien"] = new Money(tiendautu);

                                // -------- End nhom du lieu  Gan Dinh muc

                                service.Update(en);

                            }  //if (mCSDT != null && mCSDT.Id != Guid.Empty)
                            else
                            {
                                throw new InvalidPluginExecutionException("Không tìm thấy Chính sách Đầu tư bóc lá mía phù hợp");
                            }
                        }
                    }  //if(context.MessageName.ToUpper() == "CREATE")
                }
            }

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
