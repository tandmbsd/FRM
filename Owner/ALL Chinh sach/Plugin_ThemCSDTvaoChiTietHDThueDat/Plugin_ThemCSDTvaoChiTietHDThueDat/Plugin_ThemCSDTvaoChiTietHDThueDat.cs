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
using Microsoft.Xrm.Sdk.Metadata;
using System.Collections.ObjectModel;

namespace Plugin_ThemCSDTvaoChiTietHDThueDat
{
    public sealed class Plugin_ThemCSDTvaoChiTietHDThueDat : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        ITracingService traceService = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            if (context.InputParameters.Contains("Target") && (context.InputParameters["Target"] is Entity))
            {
                Entity ChiTietHDThueDatThuaDat = (Entity)context.InputParameters["Target"];
                Guid entityId = ChiTietHDThueDatThuaDat.Id;

                if (ChiTietHDThueDatThuaDat.LogicalName == "new_chitiethdthuedat_thuadat")
                {
                    //traceService.Trace("Begin plugin them CSDT");
                    if (context.MessageName.ToUpper() == "CREATE")
                    {
                        traceService.Trace("Begin plugin them CSDT  " + entityId.ToString());
                        ChiTietHDThueDatThuaDat = service.Retrieve("new_chitiethdthuedat_thuadat", entityId, new ColumnSet(new string[] { "new_sotiendautu", "createdon", "new_chitiethdthuedat", "new_thuadat", "new_dinhmuc", "new_chinhsachdautu", "new_sonamthuedat", "new_dientichthucthue" }));
                        traceService.Trace("truoc ngay tao");
                        DateTime ngaytao = ChiTietHDThueDatThuaDat.GetAttributeValue<DateTime>("createdon");

                        traceService.Trace("sau ngay tao");
                        if (!ChiTietHDThueDatThuaDat.Contains("new_chinhsachdautu"))
                        {
                            if (!ChiTietHDThueDatThuaDat.Contains("new_chitiethdthuedat"))
                            {
                                throw new InvalidPluginExecutionException("Thiếu thông tin bắt buộc về thửa đất / vụ đầu tư");
                            }
                            else
                            {
                                EntityReference thuadatEntityRef = ChiTietHDThueDatThuaDat.GetAttributeValue<EntityReference>("new_thuadat");
                                Guid thuadatId = thuadatEntityRef.Id;
                                Entity thuadatObj = service.Retrieve("new_thuadat", thuadatId, new ColumnSet(new string[] { "new_nhomdat", "new_loaisohuudat", "new_vungdialy", "new_nhomculy", "new_diachi" }));

                                traceService.Trace("1");

                                Entity ChiTietHDThueDat = null;
                                if (ChiTietHDThueDatThuaDat.Contains("new_chitiethdthuedat"))
                                {
                                    EntityReference ctHDDTThuedatRef = ChiTietHDThueDatThuaDat.GetAttributeValue<EntityReference>("new_chitiethdthuedat");
                                    Guid ctDHDTThuedatId = ctHDDTThuedatRef.Id;
                                    ChiTietHDThueDat = service.Retrieve("new_datthue", ctDHDTThuedatId, new ColumnSet(new string[] { "new_hopdongthuedat", "new_benchothuedatkh", "new_benchothuedatkhdn" }));
                                }

                                traceService.Trace("2");

                                Entity HDDTThuedat = null;
                                if (ChiTietHDThueDat.Contains("new_hopdongthuedat"))
                                {
                                    EntityReference HDDTThuedatRef = ChiTietHDThueDat.GetAttributeValue<EntityReference>("new_hopdongthuedat");
                                    Guid DHDTThuedatId = HDDTThuedatRef.Id;
                                    HDDTThuedat = service.Retrieve("new_hopdongthuedat", DHDTThuedatId, new ColumnSet(new string[] { "new_vudautu", "new_khachhang", "new_khachhangdoanhnghiep" }));
                                }

                                traceService.Trace("3");

                                EntityReference vudautuRef = new EntityReference();
                                if (HDDTThuedat.Contains("new_vudautu"))
                                    vudautuRef = HDDTThuedat.GetAttributeValue<EntityReference>("new_vudautu");

                                traceService.Trace("4");

                                Guid vuDTId = Guid.Empty;
                                if (vudautuRef != null)
                                    vuDTId = vudautuRef.Id;

                                traceService.Trace("5");

                                if (vuDTId != null && vuDTId != Guid.Empty)
                                {

                                    traceService.Trace("6");

                                    string fetchXml =
                                              @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                      <entity name='new_chinhsachdautu'>
                                        <attribute name='new_name' />
                                        <attribute name='new_vudautu' />
                                        <attribute name='new_ngayapdung' />
                                        <attribute name='new_mucdichdautu' />
                                        <attribute name='new_loaihopdong' />
                                        <attribute name='new_nhomdat_vl' />
                                        <attribute name='new_dinhmucdautukhonghoanlai' />
                                        <attribute name='new_dinhmucdautuhoanlai' />
                                        <attribute name='new_sonamthue' />
                                        <attribute name='new_loailaisuatcodinhthaydoi' />
                                        
                                        
                                        <attribute name='new_chinhsachdautuid' />
                                        <order attribute='new_ngayapdung' descending='true' />
                                        <filter type='and'>
                                          <condition attribute='new_loaihopdong' operator='eq' value='100000001' />
                                          <condition attribute='new_mucdichdautu' operator='eq' value='100000005' />
                                          <condition attribute='statecode' operator='eq' value='0' />
                                          <condition attribute='new_ngayapdung' operator='le' value='{0}' />
                                          <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{1}' />                                        
                                        </filter>
                                      </entity>
                                    </fetch>";

                                    fetchXml = string.Format(fetchXml, ngaytao, vuDTId);
                                    EntityCollection result = service.RetrieveMultiple(new FetchExpression(fetchXml));

                                    Entity mCSDT = new Entity();
                                    Entity en = new Entity(ChiTietHDThueDatThuaDat.LogicalName);
                                    en.Id = ChiTietHDThueDatThuaDat.Id;

                                    if (result != null && result.Entities.Count > 0)
                                    {
                                        foreach (Entity a in result.Entities)
                                        {
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

                                            traceService.Trace("Pass nhom dat");

                                            //if (a.Contains("new_loaisohuudat_vl"))  // Loai chu so huu
                                            //{
                                            //    if (thuadatObj.Attributes.Contains("new_loaisohuudat"))
                                            //    {
                                            //        if (a["new_loaisohuudat_vl"].ToString().IndexOf(((OptionSetValue)thuadatObj["new_loaisohuudat"]).Value.ToString()) == -1)
                                            //            continue;
                                            //    }
                                            //    else
                                            //    {
                                            //        continue;
                                            //    }
                                            //}

                                            if (a.Contains("new_sonamthue"))  // So nam thue
                                            {
                                                if (ChiTietHDThueDatThuaDat.Contains("new_sonamthuedat"))
                                                {
                                                    int sonamthueCSDT = (int)a["new_sonamthue"];
                                                    int sonamthueCTHDTD = (int)ChiTietHDThueDatThuaDat["new_sonamthuedat"];
                                                    if (sonamthueCSDT != sonamthueCTHDTD)
                                                        continue;
                                                }
                                                else
                                                    continue;
                                            }

                                            traceService.Trace("Pass so nam thue");

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

                                            traceService.Trace("Pass nhom KHHHH");

                                            //Vung dia ly
                                            EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung", "new_chinhsachdautu", "new_new_chinhsachdautu_new_vung", new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuid", a.Id);

                                            traceService.Trace("so vung DL " + dsVungDL.Entities.Count());

                                            if (dsVungDL.Entities.Count > 0)
                                            {
                                                co = false;

                                                List<Guid> dsvung = new List<Guid>();
                                                foreach (Entity n in dsVungDL.Entities)
                                                    dsvung.Add(n.Id);

                                                if (thuadatObj.Attributes.Contains("new_diachi"))
                                                {
                                                    traceService.Trace("thu dat co dia chi ");
                                                    Entity diachi = service.Retrieve("new_diachi", ((EntityReference)thuadatObj["new_diachi"]).Id, new ColumnSet(new string[] { "new_path" }));
                                                    traceService.Trace("lay duoc DC ");
                                                    QueryExpression qe = new QueryExpression("new_vungdialy_hanhchinh");
                                                    qe.ColumnSet = new ColumnSet(new string[] { "new_vungdialy_hanhchinhid", "new_vungdialy", "new_path" });
                                                    qe.Criteria.AddCondition(new ConditionExpression("new_vungdialy", ConditionOperator.In, dsvung.ToArray()));

                                                    traceService.Trace("duong dan " + service.RetrieveMultiple(qe).Entities.Count());

                                                    traceService.Trace(diachi["new_path"].ToString());

                                                    foreach (Entity n in service.RetrieveMultiple(qe).Entities)
                                                    {
                                                        traceService.Trace(n["new_path"].ToString());

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

                                            traceService.Trace("Pass vung Dia Ly");

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

                                            traceService.Trace("Pass Nhom cu ly");

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
                                            traceService.Trace("Pass Nhom nang suat");

                                            mCSDT = a;
                                            break;
                                        }
                                    }

                                    if (mCSDT != null && mCSDT.Id != Guid.Empty)
                                    {
                                        traceService.Trace("Tim duoc CSDT");

                                        // ------Gan vao Chi tiet HDDT mia
                                        EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                                        en.Attributes.Add("new_chinhsachdautu", csdtRef);

                                        traceService.Trace("Gan duoc CSDT");

                                        // -------Gan ty le thu hoi von du kien
                                        // Lay nhung tylethuhoivon trong chinh sach dau tu
                                        string fetchTLTHV =
                                        @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                          <entity name='new_tilethuhoivon'>
                                               <attribute name='new_name' />
                                               <attribute name='new_phantramtilethuhoi' />
                                               <attribute name='new_vuthuhoi' />
                                               <attribute name='new_chinhsachdautu' />
                                               <attribute name='new_tilethuhoivonid' />
                                               <order attribute='new_nam' descending='false' />
                                               <link-entity name='new_chinhsachdautu' from='new_chinhsachdautuid' to='new_chinhsachdautu' alias='ac'>
                                                   <filter type='and'>
                                                         <condition attribute='new_chinhsachdautuid' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                                                   </filter>
                                               </link-entity>
                                          </entity>
                                     </fetch>";

                                        Guid csdtKQ = mCSDT.Id;

                                        fetchTLTHV = string.Format(fetchTLTHV, csdtKQ);
                                        EntityCollection collTLTHV = service.RetrieveMultiple(new FetchExpression(fetchTLTHV));

                                        Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ,
                                            new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name", "new_loailaisuatcodinhthaydoi", "new_dinhmucdautukhonghoanlai" }));

                                        foreach (Entity TLTHV in collTLTHV.Entities)
                                        {
                                            Entity tlthvdkHDCT = new Entity("new_tylethuhoivondukien");

                                            EntityReference vudautuEntityRef = HDDTThuedat.GetAttributeValue<EntityReference>("new_vudautu");
                                            EntityReference cthdtdEntityRef = new EntityReference("new_chitiethdthuedat_thuadat", entityId);

                                            if (TLTHV.Attributes.Contains("new_phantramtilethuhoi") && TLTHV.Attributes.Contains("new_vuthuhoi") && csdtKQEntity.Attributes.Contains("new_dinhmucdautuhoanlai"))
                                            {
                                                EntityReference vdtRef = TLTHV.GetAttributeValue<EntityReference>("new_vuthuhoi");
                                                Entity vdt = service.Retrieve("new_vudautu", vdtRef.Id, new ColumnSet(new string[] { "new_name" }));

                                                string tenvdt = vdt["new_name"].ToString();
                                                string tenTLTHVDK = "Tỷ lệ thu hồi " + tenvdt;

                                                decimal tyle = (TLTHV.Contains("new_phantramtilethuhoi") ? (decimal)TLTHV["new_phantramtilethuhoi"] : 0);
                                                //decimal dientichtt = (ChiTietHDThueDatThuaDat.Contains("new_dientichthucthue") ? (decimal)ChiTietHDThueDatThuaDat["new_dientichthucthue"] : 0);
                                                decimal sotiendautu = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai") ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value : 0);
                                                decimal sotien = 0;

                                                sotien = (sotiendautu * tyle) / 100;

                                                Money sotienM = new Money(sotien);

                                                tlthvdkHDCT.Attributes.Add("new_name", tenTLTHVDK);
                                                tlthvdkHDCT.Attributes.Add("new_loaityle", new OptionSetValue(100000001));
                                                tlthvdkHDCT.Attributes.Add("new_chitiethdthuedat_thuadat", cthdtdEntityRef);
                                                tlthvdkHDCT.Attributes.Add("new_vudautu", vdtRef);
                                                tlthvdkHDCT.Attributes.Add("new_tylephantram", tyle);
                                                tlthvdkHDCT.Attributes.Add("new_sotienthuhoi", sotienM);

                                                service.Create(tlthvdkHDCT);
                                            }
                                        }
                                        // ------End Gan vao ty le thu hoi von du kien                                        
                                        traceService.Trace("gán lãi suất");
                                        if (csdtKQEntity.Attributes.Contains("new_loailaisuatcodinhthaydoi"))
                                        {
                                            bool loails = csdtKQEntity.GetAttributeValue<bool>("new_loailaisuatcodinhthaydoi");
                                            traceService.Trace("Loai ls: " + loails.ToString());
                                            // Loai lai suat
                                            if (loails == false) // ls thay doi
                                                en["new_loailaisuat"] = new OptionSetValue(100000001);

                                            else   // ls co dinh
                                                en["new_loailaisuat"] = new OptionSetValue(100000000);

                                            en["new_laisuat"] = Getlaisuat(vudautuRef, 100000005, DateTime.Now);
                                        }
                                        traceService.Trace("gan xong lai suat");
                                        // -------- Gan nhom du lieu Dinh muc
                                        if (csdtKQEntity.Attributes.Contains("new_dinhmucdautuhoanlai"))
                                        {
                                            Money dinhmucDT = csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai");
                                            en["new_dinhmuc"] = dinhmucDT;
                                        }
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
                                    <order attribute='new_apdungtu' descending='true' />
                                    <filter type='and'>
                                           <condition attribute='new_loai' operator='eq' value='100000001' />
                                           <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{0}' />
                                    </filter>
                                    </entity>
                                </fetch>";

                                        fetchTSVDT = string.Format(fetchTSVDT, vuDTId);
                                        EntityCollection collTSVDT = service.RetrieveMultiple(new FetchExpression(fetchTSVDT));
                                        //Entity TSVDT = collTSVDT.Entities[0];

                                        service.Update(en);
                                        traceService.Trace("Update duoc CSDT");
                                        //throw new Exception("chay plugin");
                                        //Logger.Write("PostCreate - Update Lead/Prospect: Success"));  

                                    } //if (mCSDT != null && mCSDT.Id != Guid.Empty)
                                    else
                                    {
                                        throw new InvalidPluginExecutionException("Không tìm thấy Chính sách Đầu tư thuê đất phù hợp");
                                    }
                                }  //if (vdtId != null && vdtId != Guid.Empty)
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

        private decimal Getlaisuat(EntityReference vudautu, int mucdichdautu, DateTime ngaygiaonhan)
        {
            traceService.Trace("start get lai suat");
            QueryExpression qbangLai = new QueryExpression("new_banglaisuatthaydoi");
            qbangLai.ColumnSet = new ColumnSet(new string[] { "new_name", "new_ngayapdung", "new_phantramlaisuat" });
            qbangLai.Criteria = new FilterExpression(LogicalOperator.And);
            //qbangLai.Criteria.AddCondition(new ConditionExpression("new_chinhsachdautu", ConditionOperator.Equal,
            //    ((EntityReference)thuacanhtac["new_chinhsachdautu"]).Id));
            qbangLai.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            qbangLai.Criteria.AddCondition(new ConditionExpression("new_vudautuapdung", ConditionOperator.Equal, vudautu.Id));
            qbangLai.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, mucdichdautu));
            qbangLai.AddOrder("new_ngayapdung", OrderType.Ascending);
            EntityCollection bls = service.RetrieveMultiple(qbangLai);
            Entity kq = null;
            decimal result = 0;
            int n = bls.Entities.Count;
            traceService.Trace(n.ToString());
            for (int i = 0; i < n; i++)
            {
                traceService.Trace(i.ToString());

                if (n == 1 && CompareDate(ngaygiaonhan, (DateTime)bls[i]["new_ngayapdung"]) == 0)
                {
                    traceService.Trace("a1");
                    result = (decimal)bls[i]["new_phantramlaisuat"];
                    break;
                }
                else if (n > 1 && CompareDate(ngaygiaonhan, (DateTime)bls[i]["new_ngayapdung"]) < 0)
                {
                    traceService.Trace("a2");
                    result = (decimal)bls[i - 1]["new_phantramlaisuat"];
                    break;
                }

                if (i == n - 1)
                {
                    result = (decimal)bls[i]["new_phantramlaisuat"];
                }
            }

            return result;
        }

        private decimal CompareDate(DateTime date1, DateTime date2) // begin,end
        {
            string currentTimerZone = TimeZoneInfo.Local.Id;
            DateTime d1 = TimeZoneInfo.ConvertTimeBySystemTimeZoneId(date1, currentTimerZone);
            DateTime d2 = TimeZoneInfo.ConvertTimeBySystemTimeZoneId(date2, currentTimerZone);
            decimal temp = (decimal)d1.Date.Subtract(d2.Date).TotalDays;
            return temp;
        }
    }
}
