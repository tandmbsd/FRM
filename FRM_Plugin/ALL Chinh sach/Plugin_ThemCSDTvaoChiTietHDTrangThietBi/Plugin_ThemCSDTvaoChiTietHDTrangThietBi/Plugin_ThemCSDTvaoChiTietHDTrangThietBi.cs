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

namespace Plugin_ThemCSDTvaoChiTietHDTrangThietBi
{
    public sealed class Plugin_ThemCSDTvaoChiTietHDTrangThietBi : IPlugin
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
                Entity ChiTietHDDTTrangThietBi = (Entity)context.InputParameters["Target"];
                Guid entityId = ChiTietHDDTTrangThietBi.Id;

                if (ChiTietHDDTTrangThietBi.LogicalName == "new_hopdongdaututrangthietbichitiet")
                {
                    //traceService.Trace("Begin plugin");
                    if (context.MessageName.ToUpper() == "CREATE")
                    {
                        ChiTietHDDTTrangThietBi = service.Retrieve("new_hopdongdaututrangthietbichitiet", entityId, new ColumnSet(new string[] { "new_maymocthietbi", "createdon", "new_name", "new_giatrihopdong", "new_hopdongdaututrangthietbi", "new_giatrithietbi" }));
                        DateTime ngaytao = ChiTietHDDTTrangThietBi.GetAttributeValue<DateTime>("createdon");

                        if (!ChiTietHDDTTrangThietBi.Contains("new_hopdongdaututrangthietbi"))
                        {
                            throw new InvalidPluginExecutionException("Thiếu thông tin bắt buộc về hợp đồng trang thiết bị");
                        }
                        else
                        {
                            EntityReference hddtTTBEntityRef = ChiTietHDDTTrangThietBi.GetAttributeValue<EntityReference>("new_hopdongdaututrangthietbi");
                            Guid hddtTTBId = hddtTTBEntityRef.Id;
                            Entity hddtTTBObj = service.Retrieve("new_hopdongdaututrangthietbi", hddtTTBId, new ColumnSet(new string[] { "new_vudautu", "new_doitaccungcap", "new_doitaccungcapkhdn" }));

                            EntityReference vudautuRef = hddtTTBObj.GetAttributeValue<EntityReference>("new_vudautu");
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
                                        <attribute name='new_chinhsachdautuid' />
                                        <order attribute='new_ngayapdung' descending='true' />
                                        <filter type='and'>
                                          <condition attribute='statecode' operator='eq' value='0' />
                                          <condition attribute='new_loaihopdong' operator='eq' value='100000002' />
                                          <condition attribute='new_mucdichdautu' operator='eq' value='100000006' />
                                          <condition attribute='new_ngayapdung' operator='on-or-before' value='{0}' />
                                          <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{1}' />                                       
                                        </filter>
                                      </entity>
                                    </fetch>";

                            fetchXml = string.Format(fetchXml, ngaytao, vuDTId);
                            EntityCollection result = service.RetrieveMultiple(new FetchExpression(fetchXml));
                            //List<Entity> CSDT = result.Entities.ToList<Entity>();

                            traceService.Trace("so CSDT " + result.Entities.Count());

                            Entity mCSDT = new Entity();
                            Entity en = new Entity(ChiTietHDDTTrangThietBi.LogicalName);
                            en.Id = ChiTietHDDTTrangThietBi.Id;

                            if (result != null && result.Entities.Count > 0)
                            {
                                foreach (Entity a in result.Entities)
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
                                    traceService.Trace("Pass nhom KH");

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

                                                    if ((nangsuatbq >= nangsuattu) && (nangsuatbq < nangsuatden))
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

                                    traceService.Trace("Pass nhom NS");

                                    mCSDT = a;
                                    break;
                                }
                            }
                            

                            if (mCSDT != null && mCSDT.Id != Guid.Empty)
                            {
                                traceService.Trace("Lay duoc CSDT");
                                // ------Gan vao Chi tiet HDDT trang thiet bi
                                EntityReference csdtRef = new EntityReference("new_chinhsachdautu", mCSDT.Id);
                                en["new_chin"] = csdtRef;

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

                                Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ, new ColumnSet(new string[] { "new_dinhmucdautuhoanlai", "new_name", "new_loailaisuatcodinhthaydoi", "new_muclaisuatdautu", "new_cachtinhlai", "new_dinhmucdautukhonghoanlai"}));

                                foreach (Entity TLTHV in collTLTHV.Entities)
                                {
                                    Entity tlthvdkHDCT = new Entity("new_tylethuhoivondukien");

                                    EntityReference vudautuEntityRef = hddtTTBObj.GetAttributeValue<EntityReference>("new_vudautu");
                                    EntityReference hdctEntityRef = new EntityReference("new_hopdongdaututrangthietbichitiet", entityId);

                                    if (ChiTietHDDTTrangThietBi.Attributes.Contains("new_giatrihopdong") && TLTHV.Attributes.Contains("new_phantramtilethuhoi") && TLTHV.Attributes.Contains("new_vuthuhoi"))
                                    {
                                        EntityReference vdtRef = TLTHV.GetAttributeValue<EntityReference>("new_vuthuhoi");
                                        Entity vdt = service.Retrieve("new_vudautu", vdtRef.Id, new ColumnSet(new string[] { "new_name" }));

                                        string tenvdt = vdt["new_name"].ToString();
                                        string tenTLTHVDK = "Tỷ lệ thu hồi " + tenvdt;

                                        decimal tyle = (TLTHV.Contains("new_phantramtilethuhoi") ? TLTHV.GetAttributeValue<decimal>("new_phantramtilethuhoi") : 0);
                                        decimal giatrihopdong = (ChiTietHDDTTrangThietBi.Contains("new_giatrihopdong") ? ChiTietHDDTTrangThietBi.GetAttributeValue<Money>("new_giatrihopdong").Value : 0);

                                        decimal sotien = 0;

                                        sotien = (giatrihopdong * tyle) / 100;
                                        Money sotienM = new Money(sotien);

                                        tlthvdkHDCT.Attributes.Add("new_name", tenTLTHVDK);
                                        tlthvdkHDCT.Attributes.Add("new_loaityle", new OptionSetValue(100000002));
                                        tlthvdkHDCT.Attributes.Add("new_chitiethddttrangthietbi", hdctEntityRef);
                                        tlthvdkHDCT.Attributes.Add("new_vudautu", vdtRef);
                                        tlthvdkHDCT.Attributes.Add("new_tylephantram", tyle);
                                        tlthvdkHDCT.Attributes.Add("new_sotienthuhoi", sotienM);

                                        service.Create(tlthvdkHDCT);
                                    }
                                }
                                // ------End Gan vao ty le thu hoi von du kien

                                // -------- Gan Gia tri toi da
                                if (csdtKQEntity.Attributes.Contains("new_dinhmucdautuhoanlai"))
                                {
                                    decimal dinhmucdt = csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value;
                                    decimal giatrithietbi = (ChiTietHDDTTrangThietBi.Contains("new_giatrithietbi") ? ChiTietHDDTTrangThietBi.GetAttributeValue<Money>("new_giatrithietbi").Value : 0);
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

                                    DMDT = (giatrithietbi * phantramtyle) / 100;
                                    Money dinhmucDT = new Money(DMDT);

                                    en["new_giatritoida"] = dinhmucDT;
                                }
                                // -------- End Gan Gia tri toi da

                                service.Update(en);

                            } //if (mCSDT != null && mCSDT.Id != Guid.Empty)
                            else
                            {
                                throw new InvalidPluginExecutionException("Không tìm thấy Chính sách Đầu tư Trang thiết bị phù hợp");
                            }
                        }
                    }  //if(context.MessageName.ToUpper() == "CREATE")
                }  // if (ChiTietHDDTTrangThietBi.LogicalName == "new_hopdongdaututrangthietbichitiet")
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
    }
}
