using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Activities;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;

namespace Plugin_ThemCSDTvaoPLHDtangDT
{
    public class Plugin_ThemCSDTvaoPLHDtangDT : IPlugin
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
                traceService.Trace("Truoc id target");
                Entity PLHDtangDT = (Entity)context.InputParameters["Target"];
                Guid entityId = PLHDtangDT.Id;
                traceService.Trace("Lay duoc id target");

                if (PLHDtangDT.LogicalName == "new_phuluchopdong_tangdientich")
                {
                    traceService.Trace("Begin plugin");
                    if (context.MessageName.ToUpper() == "CREATE" || context.MessageName.ToUpper() == "UPDATE")
                    {
                        traceService.Trace("Nhan duoc su kien");
                        PLHDtangDT = service.Retrieve("new_phuluchopdong_tangdientich", entityId, new ColumnSet(new string[] { "createdon", "new_vutrong", "new_loaigocmia", "new_luugoc", "new_mucdichsanxuatmia", "new_giongmiadangky", "new_phuluchopdong", "new_thuadat", "new_dientichhopdong" }));

                        DateTime ngaytao = DateTime.Now;
                        if (PLHDtangDT.Contains("createdon"))
                            ngaytao = PLHDtangDT.GetAttributeValue<DateTime>("createdon");

                        traceService.Trace("Ngay tao " + ngaytao);

                        EntityReference thuadatEntityRef = new EntityReference();
                        Guid thuadatId = new Guid();
                        Entity thuadatObj = new Entity();
                        if (PLHDtangDT.Attributes.Contains("new_thuadat"))
                        {
                            thuadatEntityRef = PLHDtangDT.GetAttributeValue<EntityReference>("new_thuadat");
                            thuadatId = thuadatEntityRef.Id;
                            thuadatObj = service.Retrieve("new_thuadat", thuadatId, new ColumnSet(new string[] { "new_nhomdat", "new_vungdialy", "new_nhomculy", "new_diachi" }));
                        }

                        traceService.Trace("thua dat " + thuadatId);

                        EntityReference phuluchdEntityRef = new EntityReference();
                        Guid phuluchdId = new Guid();
                        Entity phuluchdObj = new Entity();
                        if (PLHDtangDT.Contains("new_phuluchopdong"))
                        {
                            phuluchdEntityRef = PLHDtangDT.GetAttributeValue<EntityReference>("new_phuluchopdong");
                            phuluchdId = phuluchdEntityRef.Id;
                            phuluchdObj = service.Retrieve("new_phuluchopdong", phuluchdId, new ColumnSet(new string[] { "new_hopdongdautumia", "new_khachhangcanhan", "new_khachhangdoanhnghiep" }));
                        }

                        traceService.Trace("phu luc hd " + phuluchdId);

                        EntityReference giongmiaEntityRef = new EntityReference();
                        Guid giongmiaId = new Guid();
                        Entity giongmiaObj = new Entity();
                        if (PLHDtangDT.Attributes.Contains("new_giongmiadangky"))
                        {
                            giongmiaEntityRef = PLHDtangDT.GetAttributeValue<EntityReference>("new_giongmiadangky");
                            giongmiaId = giongmiaEntityRef.Id;
                            giongmiaObj = service.Retrieve("new_giongmia", giongmiaId, new ColumnSet(new string[] { "new_nhomgiong", "new_name" }));
                        }

                        traceService.Trace("giong mia " + giongmiaId);

                        EntityReference HDDTmiaRef = new EntityReference();
                        Guid DHDTmiaId = new Guid();
                        Entity HDDTmia = new Entity();
                        if (phuluchdObj.Attributes.Contains("new_hopdongdautumia"))
                        {
                            HDDTmiaRef = phuluchdObj.GetAttributeValue<EntityReference>("new_hopdongdautumia");
                            DHDTmiaId = HDDTmiaRef.Id;
                            HDDTmia = service.Retrieve("new_hopdongdautumia", DHDTmiaId, new ColumnSet(new string[] { "new_vudautu", "new_chinhantienmat" }));
                        }

                        traceService.Trace("HDDT mia " + DHDTmiaId);

                        EntityReference vudautuRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");
                        Guid vuDTId = vudautuRef.Id;
                        EntityCollection resultCol = FindCSDTtrongmia(service, ngaytao, vuDTId);
                        Entity mCSDT = null;

                        if (resultCol != null && resultCol.Entities.Count > 0)
                        {
                            foreach (Entity a in resultCol.Entities)
                            {
                                traceService.Trace(a["new_name"].ToString());
                                if (a.Contains("new_vutrong_vl"))  // Vu trong
                                {
                                    if (PLHDtangDT.Contains("new_vutrong"))
                                    {
                                        if (a["new_vutrong_vl"].ToString().IndexOf(((OptionSetValue)PLHDtangDT["new_vutrong"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                traceService.Trace("Pass vu trong");

                                if (a.Contains("new_loaigocmia_vl"))  // Loai goc mia
                                {
                                    if (PLHDtangDT.Contains("new_loaigocmia"))
                                    {
                                        if (a["new_loaigocmia_vl"].ToString().IndexOf(((OptionSetValue)PLHDtangDT["new_loaigocmia"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                traceService.Trace("Pass loai goc mia");


                                if (a.Contains("new_mucdichsanxuatmia_vl"))  // Muc dich san xuat mia
                                {
                                    if (PLHDtangDT.Contains("new_mucdichsanxuatmia"))
                                    {
                                        if (a["new_mucdichsanxuatmia_vl"].ToString().IndexOf(((OptionSetValue)PLHDtangDT["new_mucdichsanxuatmia"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }
                                traceService.Trace("Pass muc dich sx mia");

                                if (a.Contains("new_nhomdat_vl"))  // Nhom dat
                                {
                                    if (PLHDtangDT.Contains("new_thuadat") && thuadatObj.Contains("new_nhomdat"))
                                    {
                                        if (a["new_nhomdat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_nhomdat"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                traceService.Trace("Pass nhom dat");

                                if (a.Contains("new_nhomgiongmia_vl"))  // Nhom giong mia
                                {
                                    if (PLHDtangDT.Contains("new_giongmiadangky") && giongmiaObj.Contains("new_nhomgiong"))
                                    {
                                        if (a["new_nhomgiongmia_vl"].ToString().IndexOf(((OptionSetValue)giongmiaObj["new_nhomgiong"]).Value.ToString()) == -1)
                                            continue;
                                    }
                                    else
                                        continue;
                                }

                                traceService.Trace("Pass mnhom goc mia");

                                if (a.Contains("new_luugoc"))  // Luu goc
                                {
                                    if (PLHDtangDT.Attributes.Contains("new_luugoc"))
                                    {
                                        if (((OptionSetValue)a["new_luugoc"]).Value.ToString() != ((OptionSetValue)PLHDtangDT["new_luugoc"]).Value.ToString())
                                            continue;
                                    }
                                    else
                                        continue;
                                }
                                traceService.Trace("Pass luu goc");

                                // NHom khach hang
                                bool co = false;

                                if (phuluchdObj.Contains("new_khachhangcanhan"))
                                {
                                    EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                    Guid khId = phuluchdObj.GetAttributeValue<EntityReference>("new_khachhangcanhan").Id;
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

                                if (phuluchdObj.Attributes.Contains("new_khachhangdoanhnghiep"))
                                {
                                    EntityCollection dsNhomKH = RetrieveNNRecord(service, "new_nhomkhachhang", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomkhachhang", new ColumnSet(new string[] { "new_nhomkhachhangid" }), "new_chinhsachdautuid", a.Id);

                                    Guid khId = phuluchdObj.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
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

                                traceService.Trace("Pass Nhom KH");

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
                                traceService.Trace("Pass nmhom cu ly");

                                // NHom nang suat
                                co = false;

                                EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat", "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat", new ColumnSet(new string[] { "new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden" }), "new_chinhsachdautuid", a.Id);

                                if (phuluchdObj.Contains("new_khachhangcanhan"))
                                {
                                    Guid khId = phuluchdObj.GetAttributeValue<EntityReference>("new_khachhangcanhan").Id;
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
                                if (phuluchdObj.Attributes.Contains("new_khachhangdoanhnghiep"))
                                {
                                    Guid khId = phuluchdObj.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep").Id;
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

                                traceService.Trace("Pass nhom NS");

                                co = false;
                                EntityCollection lstHopdongmia = RetrieveNNRecord(service, "new_hopdongdautumia", "new_chinhsachdautu",
                                    "new_new_chinhsachdautu_new_hopdongdautumia", new ColumnSet(true), "new_chinhsachdautuid", a.Id);
                                traceService.Trace("hop dong ung von : " + lstHopdongmia.Entities.Count.ToString());
                                if (lstHopdongmia.Entities.Count <= 0)
                                {
                                    co = true;
                                }
                                else
                                {
                                    foreach (Entity hd in lstHopdongmia.Entities)
                                    {
                                        if (hd.Id == HDDTmia.Id)
                                        {
                                            co = true;
                                            break;
                                        }
                                    }
                                }

                                if (co == false)
                                    continue;

                                traceService.Trace("pass hop dong ung von");

                                mCSDT = a;
                                break;
                            }
                        }
                        
                        if (mCSDT != null && mCSDT.Id != Guid.Empty)
                        {
                            traceService.Trace("Lay duoc CSDT " + mCSDT.Id);
                            // ------Gan vao phu luc tang dt
                            EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                            Guid csdtKQ = mCSDT.Id;
                            Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name", "new_dinhmucdautukhonghoanlai", "new_dinhmucphanbontoithieu" }));

                            Entity en = new Entity(PLHDtangDT.LogicalName);
                            en.Id = PLHDtangDT.Id;

                            en["new_chinhsachdautu"] = csdtRef;

                            traceService.Trace("Gan xong CSDT ");

                            decimal dientichhd = (PLHDtangDT.Contains("new_dientichhopdong") ? (decimal)PLHDtangDT["new_dientichhopdong"] : 0);

                            traceService.Trace("DT HD  " + dientichhd);

                            decimal dongiaDTKHL = (csdtKQEntity.Contains("new_dinhmucdautukhonghoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautukhonghoanlai").Value : 0);
                            decimal dongiaDTHL = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value : 0);
                            decimal dongiaPhanbon = (csdtKQEntity.Contains("new_dinhmucphanbontoithieu") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucphanbontoithieu").Value : 0);

                            decimal dinhmucHL = dongiaDTHL * dientichhd;
                            decimal dinhmucKHL = dongiaDTKHL * dientichhd;
                            decimal dinhmucPB = dongiaPhanbon * dientichhd;

                            decimal tongdinhmuc = dinhmucHL + dinhmucKHL;

                            traceService.Trace("Tong dinh muc  " + tongdinhmuc);

                            en["new_dongiadautuhoanlai"] = new Money(dongiaDTHL);
                            en["new_dongiadautukhonghoanlai"] = new Money(dongiaDTKHL);
                            en["new_dongiaphanbontoithieu"] = new Money(dongiaPhanbon);

                            en["new_dinhmucdautuhoanlai"] = new Money(dinhmucHL);
                            en["new_dinhmucdautukhonghoanlai"] = new Money(dinhmucKHL);
                            en["new_dinhmucphanbontoithieu"] = new Money(dinhmucPB);

                            en["new_dinhmucdautu"] = new Money(tongdinhmuc);

                            traceService.Trace("Gan xong ");

                            service.Update(en);

                        }  // if (mCSDT != null && mCSDT.Id != Guid.Empty)
                        else
                        {
                            throw new InvalidPluginExecutionException("Không tìm thấy Chính sách Đầu tư Trồng & chăm sóc mía phù hợp");
                        }

                    } /////
                }
            }
        }

        public static EntityCollection FindCSDTtrongmia(IOrganizationService crmservices, DateTime ngayapdung, Guid VuDTid)
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
                                        <attribute name='new_chinhsachdautuid' />
                                        <order attribute='new_ngayapdung' descending='true' />
                                        <filter type='and'>
                                          <condition attribute='new_loaihopdong' operator='eq' value='100000000'/>
                                          <condition attribute='new_mucdichdautu' operator='eq' value='100000000' />
                                          <condition attribute='new_ngayapdung' operator='le' value='{0}' />
                                          <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{1}' />
                                            <condition attribute='statecode' operator='eq' uitype='new_vudautu' value='0' />                                          
                                        </filter>
                                      </entity>
                                    </fetch>";

            fetchXml = string.Format(fetchXml, ngayapdung, VuDTid);
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
