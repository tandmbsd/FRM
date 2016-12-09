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

namespace Plugin_ThemCSDTvaoNTTrongMia
{
    public sealed class Plugin_ThemCSDTvaoNTTrongMiaUpdate : IPlugin
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
                Entity target = (Entity)context.InputParameters["Target"];
                Guid entityId = target.Id;

                if (target.LogicalName == "new_chitietnghiemthutrongmia")
                {
                    //traceService.Trace("Begin plugin");
                    if (context.MessageName.ToUpper() == "UPDATE")
                    {
                        Entity ChiTietNTTrongMia = service.Retrieve("new_chitietnghiemthutrongmia", entityId, new ColumnSet(new string[] { "new_name", "new_nghiemthutrongmia", "new_thuadat", "new_vutrong", "new_giongmia", "createdon", "new_loaigocmia" }));
                        DateTime ngaytao = ChiTietNTTrongMia.GetAttributeValue<DateTime>("createdon");

                        if (!ChiTietNTTrongMia.Contains("new_nghiemthutrongmia"))
                        {
                            throw new InvalidPluginExecutionException("Thiếu thông tin bắt buộc về Nghiệm thu trồng mía");
                        }
                        else
                        {
                            EntityReference NTtrongmiaRef = ChiTietNTTrongMia.GetAttributeValue<EntityReference>("new_nghiemthutrongmia");
                            Guid NTtrongmiaId = NTtrongmiaRef.Id;
                            Entity NTtrongmiaObj = service.Retrieve("new_nghiemthutrongmia", NTtrongmiaId, new ColumnSet(new string[] { "subject", "new_khachhang", "new_khachhangdoanhnghiep", "new_hopdongtrongmia" }));

                            EntityReference HDDTmiaRef = NTtrongmiaObj.GetAttributeValue<EntityReference>("new_hopdongtrongmia");
                            Guid DHDTmiaId = HDDTmiaRef.Id;
                            Entity HDDTmia = service.Retrieve("new_hopdongdautumia", DHDTmiaId, new ColumnSet(new string[] { "new_vudautu" }));

                            EntityReference thuadatEntityRef = ChiTietNTTrongMia.GetAttributeValue<EntityReference>("new_thuadat");
                            EntityReference giongmiaEntityRef = ChiTietNTTrongMia.GetAttributeValue<EntityReference>("new_giongmia");
                            EntityReference vudautuRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");

                            Guid thuadatId = thuadatEntityRef.Id;
                            Entity thuadatObj = service.Retrieve("new_thuadat", thuadatId, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy","new_diachi" }));
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
                                        <attribute name='new_dinhmucdautuhoanlai' />
                                        <attribute name='new_loaigocmia_vl' />
                                        <attribute name='new_nhomgiongmia_vl' />
                                        <attribute name='new_nhomdat_vl' />
                                        <attribute name='new_vutrong_vl' />
                                        <attribute name='new_mucdichsanxuatmia_vl' />
                                        <attribute name='new_loaisohuudat_vl' />
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
                                        if (ChiTietNTTrongMia.Contains("new_vutrong"))
                                        {
                                            if (a["new_vutrong_vl"].ToString().IndexOf(((OptionSetValue)ChiTietNTTrongMia["new_vutrong"]).Value.ToString()) == -1)
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

                                    if (a.Contains("new_loaigocmia_vl"))  // Loai goc mia
                                    {
                                        if (ChiTietNTTrongMia.Attributes.Contains("new_loaigocmia"))
                                        {
                                            if (a["new_loaigocmia_vl"].ToString().IndexOf(((OptionSetValue)ChiTietNTTrongMia["new_loaigocmia"]).Value.ToString()) == -1)
                                                continue;
                                        }
                                        else
                                        {
                                            continue;
                                        }
                                    }

                                    // NHom khach hang
                                    bool co = false;
                                    if (NTtrongmiaObj.Attributes.Contains("new_khachhang"))
                                    {
                                        Guid khId = NTtrongmiaObj.GetAttributeValue<EntityReference>("new_khachhang").Id;
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

                                    if (NTtrongmiaObj.Attributes.Contains("new_khachhangdoanhnghiep"))
                                    {
                                        Guid khId = NTtrongmiaObj.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
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

                                    // NHom nang suat
                                    co = false;

                                    if (NTtrongmiaObj.Attributes.Contains("new_khachhang"))
                                    {
                                        Guid khId = NTtrongmiaObj.GetAttributeValue<EntityReference>("new_khachhang").Id;
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

                                    if (NTtrongmiaObj.Attributes.Contains("new_khachhangdoanhnghiep"))
                                    {
                                        Guid khId = NTtrongmiaObj.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
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
                                throw new InvalidPluginExecutionException("Chưa có Chính sách Đầu tư NT trồng mía nào cho vụ đầu tư này");

                            if (mCSDT != null && mCSDT.Id != Guid.Empty)
                            {
                                // ------Gan vao Chi tiet NT trong mia
                                Entity en = new Entity(ChiTietNTTrongMia.LogicalName);
                                en.Id = ChiTietNTTrongMia.Id;

                                EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                                en["new_chinhsachdautu"] = csdtRef;

                                Guid csdtKQ = mCSDT.Id;
                                Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name" }));

                                // --------  Gan Dinh muc
                                if (csdtKQEntity.Attributes.Contains("new_dinhmucdautuhoanlai"))
                                {
                                    Money dinhmucDT = csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai");

                                    en["new_dinhmuc"] = dinhmucDT;
                                }
                                // -------- End  Gan Dinh muc

                                service.Update(en);

                            }  //if (mCSDT != null && mCSDT.Id != Guid.Empty)
                            else
                            {
                                throw new InvalidPluginExecutionException("Không tìm thấy Chính sách trồng mía phù hợp");
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
