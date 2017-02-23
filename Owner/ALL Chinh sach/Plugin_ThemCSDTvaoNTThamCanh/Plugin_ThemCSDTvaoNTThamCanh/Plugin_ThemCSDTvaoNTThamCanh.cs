using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace Plugin_ThemCSDTvaoNTThamCanh
{
    public class Plugin_ThemCSDTvaoNTThamCanh : IPlugin
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

                if (target.LogicalName == "new_chitietnhiemthudautubosungvon")
                {
                    traceService.Trace("Begin plugin");
                    //throw new Exception("chay plugin tim CSDT 1");
                    if (context.MessageName.ToUpper() == "CREATE")
                    {
                        Entity chitietnghiemthudautubosungvon = service.Retrieve(target.LogicalName, target.Id,
                            new ColumnSet(true));

                        Entity NTThamcanh = service.Retrieve("new_danhgianangsuat", ((EntityReference)target["new_nghiemthudautubosungvon"]).Id,
                            new ColumnSet(true));
                        DateTime ngaytao = chitietnghiemthudautubosungvon.GetAttributeValue<DateTime>("createdon");

                        if (!NTThamcanh.Contains("new_hopdongdautumia"))
                            throw new Exception("Đánh giá năng suất không có hợp đồng đầu tư mía");

                        if (NTThamcanh.Attributes.Contains("new_loaidanhgia") && NTThamcanh.GetAttributeValue<OptionSetValue>("new_loaidanhgia").Value.ToString() == "100000001") //Loai danh gia: Tham canh la 100000001
                        {
                            if (!chitietnghiemthudautubosungvon.Contains("new_thuadat"))
                                throw new InvalidPluginExecutionException("Thiếu thông tin bắt buộc về thửa đất / giống mía / vụ đầu tư");
                            else
                            {
                                QueryExpression q = new QueryExpression("new_thuadatcanhtac");
                                q.ColumnSet = new ColumnSet(new string[] { "new_vutrong", "new_loaigocmia", "new_mucdichsanxuatmia",
                                    "new_giongmia", "new_thuadat", "createdon", "new_hopdongdautumia", "new_khachhang",
                                    "new_khachhangdoanhnghiep", "new_thamgiamohinhkhuyennong", "new_dientichthucte",
                                    "new_loaisohuudat","new_dientichconlai" });
                                q.Criteria = new FilterExpression();
                                q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, ((EntityReference)NTThamcanh["new_hopdongdautumia"]).Id));
                                q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
                                q.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, ((EntityReference)chitietnghiemthudautubosungvon["new_thuadat"]).Id));
                                EntityCollection entc = service.RetrieveMultiple(q);

                                Entity ctHDDTmia = entc.Entities.FirstOrDefault();
                                Guid ctHDDTmiaId = ctHDDTmia.Id;

                                EntityReference thuadatEntityRef = ctHDDTmia.GetAttributeValue<EntityReference>("new_thuadat");
                                EntityReference giongmiaEntityRef = ctHDDTmia.GetAttributeValue<EntityReference>("new_giongmia");
                                EntityReference vudautuRef = NTThamcanh.GetAttributeValue<EntityReference>("new_vudautu");

                                Guid thuadatId = thuadatEntityRef.Id;
                                Entity thuadatObj = service.Retrieve("new_thuadat", thuadatId, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy", "new_diachi" }));

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
                                traceService.Trace(CSDT.Count.ToString());
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
                                    throw new InvalidPluginExecutionException("Chưa có Chính sách Đầu tư NT thâm canh nào cho vụ đầu tư này");
                                traceService.Trace(mCSDT == null ? "null" : "not null");
                                
                                if (mCSDT != null && mCSDT.Id != Guid.Empty)
                                {
                                    // ------Gan vao Tham canh
                                    EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                                    Entity en = service.Retrieve(target.LogicalName, target.Id,
                                        new ColumnSet(new string[] { "new_chinhsachdautu", "new_dinhmuchoanlaitienmat", "new_dinhmuchoanlaivattu", "new_dinhmuckhl",
                                            "new_giainganhltienmat","new_giainganhlvattu","new_giaingankhl","new_denghihoanlaitienmat","new_denghihoanlaivattu",
                                            "new_denghikhl","new_dongiahltienmat","new_dongiahlvattu","new_dongiakhl" }));

                                    en["new_chinhsachdautu"] = csdtRef;

                                    Guid csdtKQ = mCSDT.Id;
                                    Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ,
                                        new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_dinhmucdautukhonghoanlai", "new_dinhmucphanbontoithieu", "new_name" }));

                                    // -------- Gan nhom du lieu  Dinh muc

                                    decimal dinhmucHL = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value : 0);
                                    decimal dinhmucKHL = (csdtKQEntity.Contains("new_dinhmucdautukhonghoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautukhonghoanlai").Value : 0);
                                    decimal dinhmucHLVT = (csdtKQEntity.Contains("new_dinhmucphanbontoithieu") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucphanbontoithieu").Value : 0);

                                    decimal dinhmucHLTM = dinhmucHL - dinhmucHLVT;

                                    Money MdinhmucHLTM = new Money();
                                    if (dinhmucHLTM > 0)
                                        MdinhmucHLTM = new Money(dinhmucHLTM);

                                    Money MdinhmucHLVT = new Money(dinhmucHLVT);
                                    Money MdinhmucKHL = new Money(dinhmucKHL);

                                    en["new_dongiahltienmat"] = MdinhmucHLTM;
                                    en["new_dongiahlvattu"] = MdinhmucHLVT;
                                    en["new_dongiakhl"] = MdinhmucKHL;

                                    // ----END---- Gan nhom du lieu  Dinh muc

                                    // -------- Gan nhom du lieu Don gia

                                    decimal dientichconlai = (ctHDDTmia.Contains("new_dientichconlai") ? (decimal)ctHDDTmia["new_dientichconlai"] : new decimal(0));

                                    en["new_dinhmuchoanlaitienmat"] = new Money(dinhmucHLTM * dientichconlai);
                                    en["new_dinhmuchoanlaivattu"] = new Money(dinhmucHLVT * dientichconlai);
                                    en["new_dinhmuckhl"] = new Money(dinhmucKHL * dientichconlai);

                                    //EntityCollection dsNTThamcanh = FindNTThamcanh(service, ctHDDTmia);
                                    //if (dsNTThamcanh != null && dsNTThamcanh.Entities.Count() > 0)
                                    //{
                                    //    foreach(Entity NTTC in dsNTThamcanh.Entities)
                                    //    {
                                    //        daGNHLTM += (NTTC.Contains("new_denghihoanlaitienmat") ? NTTC.GetAttributeValue<Money>("new_denghihoanlaitienmat").Value : 0);
                                    //        daGNHLVT += (NTTC.Contains("new_denghihoanlaivattu") ? NTTC.GetAttributeValue<Money>("new_denghihoanlaivattu").Value : 0);
                                    //        daGNKHL += (NTTC.Contains("new_denghikhl") ? NTTC.GetAttributeValue<Money>("new_denghikhl").Value : 0);
                                    //    }
                                    //}


                                    // ----END---- Gan nhom du lieu Don gia

                                    // -------- Gan nhom du lieu Đề nghị

                                    en["new_denghihoanlaitienmat"] = new Money(dinhmucHL * dientichconlai);
                                    en["new_denghihoanlaivattu"] = new Money(dinhmucHLVT * dientichconlai);
                                    en["new_denghikhl"] = new Money(dinhmucKHL * dientichconlai);

                                    // ----END---- Gan nhom du lieu Đề nghị

                                    service.Update(en);

                                }  //if (mCSDT != null && mCSDT.Id != Guid.Empty)
                                else
                                {
                                    throw new InvalidPluginExecutionException("Không tìm thấy Chính sách Nghiệm thu ĐT bổ sung vốn phù hợp");
                                }
                            }
                        } //if(NTThamcanh.GetAttributeValue<OptionSetValue>("new_loaidanhgia") == loaidanhgia) 

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

        public static EntityCollection FindNTThamcanh(IOrganizationService crmservices, Entity ctHDDTmia)
        {
            string fetchXml =
               @"<fetch mapping='logical' output-format='xml-platform' version='1.0' distinct='false'>
                  <entity name='new_danhgianangsuat'>
                    <attribute name='subject' />
                    <attribute name='createdon' />
                    <attribute name='new_denghihoanlaitienmat' />
                    <attribute name='new_denghihoanlaivattu' />
                    <attribute name='new_denghikhl' />
                    <attribute name='activityid' />
                    <order descending='false' attribute='subject' />
                    <filter type='and'>
                      <condition attribute='new_thuadatcanhtac' operator='eq' uitype='new_thuadatcanhtac' value='{0}'/>
                      <condition attribute='statuscode' operator='eq' value='100000001'/>
                    </filter>
                  </entity>
                </fetch>";

            Guid ctHDDTmiaId = ctHDDTmia.Id;
            fetchXml = string.Format(fetchXml, ctHDDTmiaId);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;

        }
    }
}
