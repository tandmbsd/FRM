using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_PhieuChuyenNo_PBDT
{
    public class Plugin_PhieuChuyenNo_PBDT : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            #region cm
            //IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            //factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            //service = factory.CreateOrganizationService(context.UserId);
            //throw new Exception("a");
            //Entity target = (Entity)context.InputParameters["Target"];
            //if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000)
            //{                
            //    Entity phieuchuyenno = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

            //    List<Entity> lst_pcnphanbodautu = RetrieveMultiRecord(service, "new_phieuchuyennophanbodautu", new ColumnSet(true), "new_phieuchuyenno", phieuchuyenno.Id);
            //    foreach (Entity en in lst_pcnphanbodautu)
            //    {
            //        service.Delete(en.LogicalName, en.Id);
            //    }

            //    List<Entity> lstPhanbodautu = new List<Entity>();

            //    bool chuyenhddtthuedat = (bool)phieuchuyenno["new_chuyenhddtthuedat"];

            //    EntityCollection lstHDDTM = RetrieveNNRecord(service, "new_hopdongdautumia", "new_phieuchuyenno"
            //        , "new_new_phieuchuyenno_new_hopdongdautumia", new ColumnSet(true), "new_phieuchuyennoid", phieuchuyenno.Id);
            //    List<Entity> lstChitiet = RetrieveMultiRecord(service, "new_phieuchuyennochitiethddtmia", new ColumnSet(new string[] { "new_chitiethddtmia" }), "new_phieuchuyenno", phieuchuyenno.Id);

            //    EntityCollection lstHDTD = RetrieveNNRecord(service, "new_hopdongthuedat", "new_phieuchuyenno"
            //        , "new_new_phieuchuyenno_new_hopdongthuedat", new ColumnSet(true), "new_phieuchuyennoid", phieuchuyenno.Id);
            //    //throw new Exception(lstHDDTM.Entities.Count.ToString() + lstHDTD.Entities.Count.ToString() + lstChitiet.Count.ToString());
            //    if (lstHDTD.Entities.Count > 0)
            //    {
            //        StringBuilder fetchXml = new StringBuilder();
            //        fetchXml.Append("<fetch mapping='logical' version='1.0'>");
            //        fetchXml.Append("<entity name='new_phanbodautu'>");
            //        fetchXml.Append("<attribute name='new_phanbodautuid'/>");
            //        fetchXml.Append("<filter type='or'>");

            //        foreach (Entity hdtd in lstHDTD.Entities)
            //        {
            //            fetchXml.Append(string.Format("<condition attribute='new_hopdaudaututhuedat' operator='eq' value='{0}'></condition>", hdtd.Id));
            //        }

            //        fetchXml.Append("</filter>");
            //        fetchXml.Append("</entity>");
            //        fetchXml.Append("</fetch>");
            //        EntityCollection entcHDTD = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));
            //        lstPhanbodautu.AddRange(entcHDTD.Entities);
            //    }

            //    if (lstHDDTM.Entities.Count > 0)
            //    {
            //        StringBuilder fetchXml = new StringBuilder();
            //        fetchXml.Append("<fetch mapping='logical' version='1.0'>");
            //        fetchXml.Append("<entity name='new_phanbodautu'>");
            //        fetchXml.Append("<attribute name='new_phanbodautuid'/>");
            //        fetchXml.Append("<filter type='and'>");
            //        fetchXml.Append(string.Format("<condition attribute='new_thuacanhtac' operator='ge' value='{0}'></condition>",null));
            //        fetchXml.Append("<filter type='or'>");

            //        foreach (Entity hddtm in lstHDDTM.Entities)
            //        {
            //            fetchXml.Append(string.Format("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}'></condition>", hddtm.Id));
            //        }

            //        fetchXml.Append("</filter>");
            //        fetchXml.Append("</entity>");
            //        fetchXml.Append("</fetch>");
            //        EntityCollection entcHDDTM = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));
            //        lstPhanbodautu.AddRange(entcHDDTM.Entities);
            //    }

            //    if (lstChitiet.Count > 0)
            //    {
            //        StringBuilder fetchXml = new StringBuilder();
            //        fetchXml.Append("<fetch mapping='logical' version='1.0'>");
            //        fetchXml.Append("<entity name='new_phanbodautu'>");
            //        fetchXml.Append("<attribute name='new_phanbodautuid'/>");
            //        fetchXml.Append("<filter type='or'>");

            //        foreach (Entity ct in lstChitiet)
            //        {
            //            fetchXml.Append(string.Format("<condition attribute='new_thuacanhtac' operator='eq' value='{0}'></condition>", ((EntityReference)ct["new_chitiethddtmia"]).Id));
            //        }

            //        fetchXml.Append("</filter>");
            //        fetchXml.Append("</entity>");
            //        fetchXml.Append("</fetch>");
            //        EntityCollection entcThuacatacnh = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));

            //        lstPhanbodautu.AddRange(entcThuacatacnh.Entities);
            //    }

            //    foreach (Entity en in lstPhanbodautu)
            //    {
            //        Entity pcn_pbdt = new Entity("new_phieuchuyennophanbodautu");
            //        decimal[] arr = new decimal[2];
            //        arr = sumsotienphanboquahan(lstPhanbodautu);

            //        //pcn_pbdt["new_name"] = phieuchuyenno["new_name"].ToString() + "-" + en["new_name"].ToString();
            //        pcn_pbdt["new_sotienphanbotronghan"] = new Money(arr[1]);

            //        pcn_pbdt["new_sotienphanboquahan"] = new Money(arr[0]);
            //        pcn_pbdt["new_sotienchuyennotronghan"] = new Money(arr[1]);

            //        pcn_pbdt["new_sotienchuyennoquahan"] = new Money(arr[0]);
            //        pcn_pbdt["new_phanbodautu"] = en.ToEntityReference();
            //        pcn_pbdt["new_phieuchuyenno"] = new EntityReference(target.LogicalName, target.Id);

            //        service.Create(pcn_pbdt);
            //    }
            //}
            #endregion

            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000)
            {
                Entity phieuchuyenno = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_phieuchuyennoid" }));

                List<Entity> Lstpcn_pbdt = RetrieveMultiRecord(service, "new_phieuchuyennophanbodautu", new ColumnSet(new string[] { "new_phanbodautu","new_phieuchuyenno" }), "new_phieuchuyenno", phieuchuyenno.Id);
                EntityCollection lstHDTD = RetrieveNNRecord(service, "new_hopdongthuedat", "new_phieuchuyenno"
                    , "new_new_phieuchuyenno_new_hopdongthuedat", new ColumnSet(true), "new_phieuchuyennoid", phieuchuyenno.Id);
                EntityCollection lstHDDTM = RetrieveNNRecord(service, "new_hopdongdautumia", "new_phieuchuyenno"
                    , "new_new_phieuchuyenno_new_hopdongdautumia", new ColumnSet(true), "new_phieuchuyennoid", phieuchuyenno.Id);
                EntityCollection phieucnCTHDCol = FindPhieucnCTHD(service, phieuchuyenno);

                Entity KHbenB = new Entity();
                Entity KHbenA = new Entity();
                string namebenA = "";
                #region set khach hang bên A,B
                if (phieuchuyenno.Contains("new_khachhang_bena"))
                {
                    EntityReference KHbenARef = phieuchuyenno.GetAttributeValue<EntityReference>("new_khachhang_bena");
                    KHbenA = service.Retrieve("contact", KHbenARef.Id, new ColumnSet(new string[] { "fullname" }));
                    namebenA = KHbenA.GetAttributeValue<string>("fullname");
                }
                if (phieuchuyenno.Contains("new_khachhangdoanhnghiep_bena"))
                {
                    EntityReference KHbenARef = phieuchuyenno.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep_bena");
                    KHbenA = service.Retrieve("account", KHbenARef.Id, new ColumnSet(new string[] { "name" }));
                    namebenA = KHbenA.GetAttributeValue<string>("name");
                }
                if (phieuchuyenno.Contains("new_khachhang_benb"))
                {
                    EntityReference KHbenBRef = phieuchuyenno.GetAttributeValue<EntityReference>("new_khachhang_benb");
                    KHbenB = service.Retrieve("contact", KHbenBRef.Id, new ColumnSet(new string[] { "fullname" }));
                }
                if (phieuchuyenno.Contains("new_khachhangdoanhnghiep_benb"))
                {
                    EntityReference KHbenBRef = phieuchuyenno.GetAttributeValue<EntityReference>("new_khachhangdoanhnghiep_benb");
                    KHbenB = service.Retrieve("account", KHbenBRef.Id, new ColumnSet(new string[] { "name" }));
                }
                #endregion

                if (phieucnCTHDCol != null && phieucnCTHDCol.Entities.Count() > 0)
                {
                    #region chia thanh cac nhom chi tiet cung hop dong
                    Dictionary<string, List<Entity>> dictionary = new Dictionary<string, List<Entity>>();

                    foreach (Entity phieucnCTHD in phieucnCTHDCol.Entities)
                    {
                        Entity ctHDDTmia = service.Retrieve("new_thuadatcanhtac", ((EntityReference)phieucnCTHD["new_chitiethddtmia"]).Id, new ColumnSet(true));
                        if (!ctHDDTmia.Contains("new_hopdongdautumia"))
                        {
                            throw new Exception("Chi tiết " + ctHDDTmia["new_name"].ToString() + " không có hợp đồng đầu tư mia");
                        }
                        string key = ((EntityReference)ctHDDTmia["new_hopdongdautumia"]).Id.ToString();

                        if (!dictionary.ContainsKey(key))
                        {
                            dictionary.Add(key, new List<Entity>());
                        }
                        dictionary[key].Add(ctHDDTmia);
                    }

                    #endregion
                    foreach (string key in dictionary.Keys)
                    {
                        Guid idHDDTM = Guid.Parse(key);
                        Entity bHDDTmiaRef = service.Retrieve("new_hopdongdautumia", idHDDTM, new ColumnSet(true));
                        Entity newHDDTMia = new Entity(bHDDTmiaRef.LogicalName);
                        newHDDTMia["new_name"] = "New - " + bHDDTmiaRef["new_name"].ToString();

                        foreach (string t in bHDDTmiaRef.Attributes.Keys)
                        {
                            if (t.IndexOf("new") == 0 && t != "new_hopdongdautumiaid" && t != "new_name")
                                newHDDTMia[t] = bHDDTmiaRef[t];
                        }

                        Guid newHDDTMiaid = service.Create(newHDDTMia);

                        foreach (Entity k in dictionary[key]) // duyet tung chi tiet co cung hop dong
                        {
                            // ---------- copy chi tiết HD mía
                            Entity newctHDDTmia = new Entity(k.LogicalName);

                            foreach (string t in k.Attributes.Keys)
                            {
                                if (t.IndexOf("new") == 0 && t != "new_thuadatcanhtacid")
                                    newctHDDTmia[t] = k[t];
                            }

                            Guid newCTHDid = service.Create(newctHDDTmia);

                            //i += 1;
                            traceService.Trace("Tao CT HDDT mia lan ");

                            Entity newCTHDmia = service.Retrieve("new_thuadatcanhtac", newCTHDid, new ColumnSet(new string[] { "new_hopdongdautumia", "new_hopdongdaututhuedat" }));
                            EntityReference cthdEntityRef = new EntityReference("new_thuadatcanhtac", newCTHDid);

                            newCTHDmia["new_hopdongdautumia"] = new EntityReference("new_hopdongdautumia", newHDDTMiaid);
                            service.Update(newCTHDmia);

                            #region Copy ti le thu hoi von du kien
                            //Ti le thu hoi von du kien
                            EntityCollection TyleTHVDKCol = FindTLTHVDK(service, k);
                            if (TyleTHVDKCol != null && TyleTHVDKCol.Entities.Count > 0)
                            {
                                foreach (Entity tyle in TyleTHVDKCol.Entities)
                                {
                                    Entity tyleTHVDK = new Entity("new_tylethuhoivondukien");
                                    Entity tyle1 = service.Retrieve("new_tylethuhoivondukien", tyle.Id, new ColumnSet(true));

                                    string tenTLTHVDK = tyle.GetAttributeValue<string>("new_name");
                                    decimal tyleth = (tyle.Contains("new_tylephantram") ? (decimal)tyle["new_tylephantram"] : 0);

                                    if (tyle1.Attributes.Contains("new_sotienthuhoi"))
                                    {
                                        Money sotienM = (Money)tyle1["new_sotienthuhoi"];
                                        tyleTHVDK["new_sotienthuhoi"] = sotienM;
                                    }
                                    EntityReference vuTDRef = new EntityReference();
                                    if (tyle1.Attributes.Contains("new_vudautu"))   // vu DT
                                    {
                                        vuTDRef = tyle1.GetAttributeValue<EntityReference>("new_vudautu");
                                        tyleTHVDK.Attributes.Add("new_name", tenTLTHVDK);
                                        tyleTHVDK.Attributes.Add("new_loaityle", new OptionSetValue(100000000));
                                        tyleTHVDK.Attributes.Add("new_chitiethddtmia", cthdEntityRef);
                                        tyleTHVDK.Attributes.Add("new_vudautu", vuTDRef);
                                        tyleTHVDK.Attributes.Add("new_tylephantram", tyleth);

                                        service.Create(tyleTHVDK);
                                        traceService.Trace("Tao ty le THV du kien");
                                    }
                                    else
                                    {
                                        throw new InvalidPluginExecutionException("Thiếu thông tin vụ đầu tư  trong tỷ lệ TH vốn DK");
                                    }
                                }
                            }
                            #endregion
                            traceService.Trace("Copy ti le thu hoi vo  ");
                            #region khuyen khich phat trien
                            // Khuyến khích phát triển
                            EntityCollection dsKKPTCTDH = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_khuyenkhichpt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_thuadatcanhtacid", k.Id);
                            EntityReferenceCollection listKKPT = new EntityReferenceCollection();
                            if (dsKKPTCTDH != null && dsKKPTCTDH.Entities.Count > 0)
                            {
                                foreach (Entity kkpt in dsKKPTCTDH.Entities)
                                {
                                    listKKPT.Add(kkpt.ToEntityReference());
                                }
                                service.Associate("new_thuadatcanhtac", newCTHDid, new Relationship("new_new_chitiethddtmia_new_khuyenkhichpt"), listKKPT);
                            }
                            #endregion
                            traceService.Trace("Copy khuyen khich phat trien");
                            #region chinh sach dau tu
                            // CSDT ap dung
                            EntityCollection dsCSDT = RetrieveNNRecord(service, "new_chinhsachdautu", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_chinhsachdautu", new ColumnSet(new string[] { "new_chinhsachdautuid" }), "new_thuadatcanhtacid", k.Id);
                            EntityReferenceCollection listCSDT = new EntityReferenceCollection();
                            if (dsCSDT != null && dsCSDT.Entities.Count > 0)
                            {
                                foreach (Entity csdt in dsCSDT.Entities)
                                {
                                    listCSDT.Add(csdt.ToEntityReference());
                                }
                                service.Associate("new_thuadatcanhtac", newCTHDid, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), listCSDT);
                            }
                            #endregion
                            traceService.Trace("Chinh sach dau tu");

                            #region chuyen tinh trang ben a = chuyen no
                            Entity ctHDDTmiaBenA = new Entity(k.LogicalName);
                            ctHDDTmiaBenA.Id = k.Id;

                            ctHDDTmiaBenA["statuscode"] = new OptionSetValue(100000006);
                            service.Update(ctHDDTmiaBenA);
                            traceService.Trace("cap nhat CT HDDT mia");
                            #endregion
                            
                            if (Lstpcn_pbdt.Count > 0)
                            {
                                foreach (Entity pcn_pbdt in Lstpcn_pbdt)
                                {
                                    Entity pbdt = service.Retrieve("new_phanbodautu", ((EntityReference)pcn_pbdt["new_phanbodautu"]).Id, new ColumnSet(true));
                                    
                                    if (pbdt.Contains("new_thuacanhtac") && ((EntityReference)pbdt["new_thuacanhtac"]).Id == k.Id)
                                    {
                                        Entity newPBDT = new Entity("new_phanbodautu");
                                        
                                        foreach (string t in pbdt.Attributes.Keys)
                                        {
                                            if (t.IndexOf("new") == 0 && t != "new_phanbodautuid")
                                                newPBDT[t] = pbdt[t];
                                        }

                                        newPBDT["new_hopdongdautumia"] = new EntityReference("new_hopdongdautumia", newHDDTMiaid);                                        
                                        newPBDT["new_thuacanhtac"] = new EntityReference("new_thuadatcanhtac",newCTHDid);
                                        
                                        service.Create(newPBDT);
                                    }
                                }
                            }
                        }
                    }
                }

                if (lstHDTD.Entities.Count > 0)
                {
                    #region copyHDTD
                    foreach (Entity hdtd in lstHDTD.Entities)
                    {
                        #region UpdateHDTD
                        Entity hopdongthuedat = service.Retrieve(hdtd.LogicalName, hdtd.Id, new ColumnSet(true));
                        Entity newHDTD = new Entity(hopdongthuedat.LogicalName);
                        hopdongthuedat["statuscode"] = new OptionSetValue(100000003);// hop dong cu chuyen thanh chuyen no                        
                        service.Update(hopdongthuedat);

                        if (phieuchuyenno.Contains("new_khachhang_benb"))
                        {
                            newHDTD["new_khachhang"] = KHbenB.ToEntityReference();
                            newHDTD["new_name"] = "HĐTĐ " + namebenA + " chuyển nợ cho " + KHbenB.GetAttributeValue<string>("fullname");
                            newHDTD["new_khachhangdoanhnghiep"] = null;
                        }

                        if (phieuchuyenno.Contains("new_khachhangdoanhnghiep_benb"))
                        {
                            newHDTD["new_khachhangdoanhnghiep"] = KHbenB.ToEntityReference();
                            newHDTD["new_name"] = "HĐTĐ " + namebenA + " chuyển nợ cho " + KHbenB.GetAttributeValue<string>("name");
                            newHDTD["new_khachhang"] = null;
                        }
                        service.Update(newHDTD);
                        #endregion
                        EntityCollection ctHDTDcol = FindCTHDThuedat(service, hopdongthuedat);
                        if (ctHDTDcol != null && ctHDTDcol.Entities.Count() > 0)
                        {
                            #region copyCTHDTD
                            foreach (Entity ctthuedat in ctHDTDcol.Entities)
                            {
                                Entity newCTHDTD = new Entity(ctthuedat.LogicalName);
                                foreach (string key in ctthuedat.Attributes.Keys)
                                {
                                    if (key.IndexOf("new") == 0 && key != "new_datthueid")
                                        newCTHDTD[key] = ctthuedat[key];
                                }
                                Guid newCTHDTDid = service.Create(newCTHDTD);

                                traceService.Trace("tao moi CT HDDT thue dat");

                                Entity newCTHDthuedat = service.Retrieve("new_datthue", newCTHDTDid, new ColumnSet(new string[] { "new_name", "new_hopdongthuedat" }));
                                EntityReference cthdtdEntityRef = new EntityReference("new_datthue", newCTHDTDid);

                                EntityCollection dsThuadat = RetrieveNNRecord(service, "new_thuadat", "new_datthue", "new_new_datthue_new_thuadat", new ColumnSet(new string[] { "new_thuadatid" }), "new_datthueid", ctthuedat.Id);
                                EntityReferenceCollection listThuadat = new EntityReferenceCollection();
                                if (dsThuadat != null && dsThuadat.Entities.Count > 0)
                                {
                                    foreach (Entity thuadat in dsThuadat.Entities)
                                    {
                                        listThuadat.Add(thuadat.ToEntityReference());
                                    }
                                    service.Associate("new_datthue", newCTHDTDid, new Relationship("new_new_datthue_new_thuadat"), listThuadat);
                                }

                                Entity enCTHDTD = new Entity(newCTHDthuedat.LogicalName);
                                enCTHDTD.Id = newCTHDthuedat.Id;

                                enCTHDTD["new_hopdongthuedat"] = newHDTD.ToEntityReference();
                                service.Update(enCTHDTD);

                                traceService.Trace("cap nhat CT HDDT thue dat");

                            }
                            #endregion
                        }
                    }
                    #endregion
                }
                if (lstHDDTM.Entities.Count > 0 && phieucnCTHDCol.Entities.Count < 1)
                {
                    #region copyHDDTM
                    foreach (Entity hddtm in lstHDDTM.Entities)
                    {
                        hddtm["statuscode"] = new OptionSetValue(100000004);
                        service.Update(hddtm);
                        EntityCollection ctHDDTmiaCol = FindCTHD(service, hddtm);

                        if (ctHDDTmiaCol != null && ctHDDTmiaCol.Entities.Count() > 0)
                        {
                            foreach (Entity a in ctHDDTmiaCol.Entities)
                            {
                                Entity en = new Entity(a.LogicalName);
                                foreach (string key in a.Attributes.Keys)
                                {
                                    if (key.IndexOf("new") == 0 && key != "new_thuadatcanhtacid")
                                        en[key] = a[key];
                                }
                                Guid newCTHDid = service.Create(en);

                                Entity newCTHDmia = service.Retrieve("new_thuadatcanhtac", newCTHDid, new ColumnSet(new string[] { "new_hopdongdautumia", "new_hopdongdaututhuedat" }));
                                EntityReference cthdEntityRef = new EntityReference("new_thuadatcanhtac", newCTHDid);

                                Entity enCTHDDTmia = new Entity(newCTHDmia.LogicalName);
                                enCTHDDTmia.Id = newCTHDmia.Id;

                                enCTHDDTmia["new_hopdongdautumia"] = hddtm.ToEntityReference();

                                #region copy tyle thu hoi von du kien
                                EntityCollection TyleTHVDKCol = FindTLTHVDK(service, a);
                                if (TyleTHVDKCol != null && TyleTHVDKCol.Entities.Count > 0)
                                {
                                    foreach (Entity tyle in TyleTHVDKCol.Entities)
                                    {
                                        Entity tyleTHVDK = new Entity("new_tylethuhoivondukien");
                                        Entity tyle1 = service.Retrieve("new_tylethuhoivondukien", tyle.Id, new ColumnSet(true));

                                        string tenTLTHVDK = tyle.GetAttributeValue<string>("new_name");
                                        decimal tyleth = (tyle.Contains("new_tylephantram") ? (decimal)tyle["new_tylephantram"] : 0);

                                        if (tyle1.Attributes.Contains("new_sotienthuhoi"))   // Dinh muc DT khong hoan lai
                                        {
                                            Money sotienM = (Money)tyle1["new_sotienthuhoi"];
                                            tyleTHVDK["new_sotienthuhoi"] = sotienM;
                                        }
                                        EntityReference vuTDRef = new EntityReference();
                                        if (tyle1.Attributes.Contains("new_vudautu"))   // Dinh muc DT khong hoan lai
                                        {
                                            vuTDRef = tyle1.GetAttributeValue<EntityReference>("new_vudautu");
                                            tyleTHVDK.Attributes.Add("new_name", tenTLTHVDK);
                                            tyleTHVDK.Attributes.Add("new_loaityle", new OptionSetValue(100000000));
                                            tyleTHVDK.Attributes.Add("new_chitiethddtmia", cthdEntityRef);
                                            tyleTHVDK.Attributes.Add("new_vudautu", vuTDRef);
                                            tyleTHVDK.Attributes.Add("new_tylephantram", tyleth);

                                            service.Create(tyleTHVDK);
                                        }
                                        else
                                        {
                                            throw new InvalidPluginExecutionException("Thiếu thông tin vụ đầu tư trong tỷ lệ TH vốn DK");
                                        }
                                    }
                                }
                                #endregion
                                // Khuyến khích phát triển
                                #region khuyen khich phat trien
                                EntityCollection dsKKPTCTDH = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_khuyenkhichpt", new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_thuadatcanhtacid", a.Id);
                                EntityReferenceCollection listKKPT = new EntityReferenceCollection();
                                if (dsKKPTCTDH != null && dsKKPTCTDH.Entities.Count > 0)
                                {
                                    foreach (Entity kkpt in dsKKPTCTDH.Entities)
                                    {
                                        listKKPT.Add(kkpt.ToEntityReference());
                                    }
                                    service.Associate("new_thuadatcanhtac", newCTHDid, new Relationship("new_new_chitiethddtmia_new_khuyenkhichpt"), listKKPT);
                                }
                                #endregion
                                // CSDT ap dung
                                #region chinh sach dau tu
                                EntityCollection dsCSDT = RetrieveNNRecord(service, "new_chinhsachdautu", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_chinhsachdautu", new ColumnSet(new string[] { "new_chinhsachdautuid" }), "new_thuadatcanhtacid", a.Id);
                                EntityReferenceCollection listCSDT = new EntityReferenceCollection();
                                if (dsCSDT != null && dsCSDT.Entities.Count > 0)
                                {
                                    foreach (Entity csdt in dsCSDT.Entities)
                                    {
                                        listCSDT.Add(csdt.ToEntityReference());
                                    }
                                    service.Associate("new_thuadatcanhtac", newCTHDid, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), listCSDT);
                                }
                                #endregion
                            }
                        }
                    }
                    #endregion
                }
            }
        }

        decimal[] sumsotienphanboquahan(List<Entity> lstPBDT)
        {
            decimal[] kq = new decimal[2];
            foreach (Entity en in lstPBDT)
            {
                if (en.GetAttributeValue<DateTime>("new_hanthanhtoan") < DateTime.Today)
                {
                    kq[0] += en.Contains("new_sotien") ? ((Money)en["new_sotien"]).Value : 0;
                }
                else
                    kq[1] += en.Contains("new_sotien") ? ((Money)en["new_sotien"]).Value : 0;
            }
            return kq;
        }

        //List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        //{
        //    QueryExpression q = new QueryExpression(entity);
        //    q.ColumnSet = column;
        //    q.Criteria = new FilterExpression();
        //    q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
        //    EntityCollection entc = service.RetrieveMultiple(q);

        //    return entc.Entities.ToList<Entity>();
        //}

        Entity RetrieveSingleRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>().FirstOrDefault();
        }

        //EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
        //{
        //    EntityReferenceCollection result = new EntityReferenceCollection();
        //    QueryExpression query = new QueryExpression(entity1);
        //    query.ColumnSet = column;
        //    LinkEntity linkEntity1 = new LinkEntity(entity1, relateName, entity1 + "id", entity1 + "id", JoinOperator.Inner);
        //    LinkEntity linkEntity2 = new LinkEntity(relateName, entity2, entity2 + "id", entity2 + "id", JoinOperator.Inner);

        //    linkEntity1.LinkEntities.Add(linkEntity2);
        //    query.LinkEntities.Add(linkEntity1);

        //    linkEntity2.LinkCriteria = new FilterExpression();
        //    linkEntity2.LinkCriteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
        //    EntityCollection collRecords = service.RetrieveMultiple(query);

        //    return collRecords;
        //}

        List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>();
        }

        public static EntityCollection FindPhieucnCTHD(IOrganizationService crmservices, Entity PhieuCN)
        {
            string fetchXml =
               @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_phieuchuyennochitiethddtmia'>
                    <attribute name='new_name' />
                    <attribute name='createdon' />
                    <attribute name='new_hinhthucchuyen' />
                    <attribute name='new_chitiethddtmia' />
                    <attribute name='new_phieuchuyenno' />
                    <attribute name='new_phieuchuyennochitiethddtmiaid' />
                    <order attribute='createdon' descending='true' />
                    <filter type='and'>
                      <condition attribute='new_phieuchuyenno' operator='eq' uitype='new_phieuchuyenno' value='{0}' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, PhieuCN.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindPhieucnCTHDPBDT(IOrganizationService crmservices, Entity pcnCTHD)
        {
            string fetchXml =
               @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_pcnchitiethddtmiaphanbodautu'>
                    <attribute name='new_pcnchitiethddtmiaphanbodautuid' />
                    <attribute name='new_name' />
                    <attribute name='createdon' />
                    <attribute name='new_phieuchuyennochitiethddtmia' />
                    <attribute name='new_phanbodautu' />
                    <order attribute='new_name' descending='false' />
                    <filter type='and'>
                      <condition attribute='new_phieuchuyennochitiethddtmia' operator='eq' uitype='new_phieuchuyennochitiethddtmia' value='{0}' />
                    </filter>
                  </entity>
                </fetch>";

            fetchXml = string.Format(fetchXml, pcnCTHD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindCTHD(IOrganizationService crmservices, Entity HDDTmia)
        {
            QueryExpression q = new QueryExpression("new_thuadatcanhtac");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, HDDTmia.Id));
            q.Orders.Add(new OrderExpression("createdon", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }
        public static EntityCollection FindCTHDThuedat(IOrganizationService crmservices, Entity HDDTthuedat)
        {
            QueryExpression q = new QueryExpression("new_datthue");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_hopdongthuedat", ConditionOperator.Equal, HDDTthuedat.Id));
            q.Orders.Add(new OrderExpression("createdon", OrderType.Ascending));

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
                        <attribute name='new_chitiethddttrangthietbi' />
                        
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

        public static EntityCollection FindTLTHVDKhdThuedat(IOrganizationService crmservices, Entity chitietHDthuedat)
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
                        <attribute name='new_chitiethddtthuedat' />
                        <attribute name='new_chitiethddtmia' />
                        <attribute name='new_tylethuhoivondukienid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_chitiethddtthuedat' operator='eq' uitype='new_datthue' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, chitietHDthuedat.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindPhieuChuyenNoPBDT(IOrganizationService crmservices, Entity PhieuCN)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_phieuchuyennophanbodautu'>
                        <attribute name='createdon' />
                        <attribute name='new_sotienphanbotronghan' />
                        <attribute name='new_sotienphanboquahan' />
                        <attribute name='new_sotienchuyennotronghan' />
                        <attribute name='new_sotienchuyennoquahan' />
                        <attribute name='new_phanbodautu' />
                        <attribute name='new_phieuchuyennophanbodautuid' />
                        <order attribute='createdon' descending='true' />
                        <filter type='and'>
                          <condition attribute='new_phieuchuyenno' operator='eq' uitype='new_phieuchuyenno' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, PhieuCN.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindPBDT(IOrganizationService crmservices, Entity CTHDDTmia)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_phanbodautu'>
                        <attribute name='new_name' />
                        <attribute name='new_sotien' />
                        <attribute name='new_maphieuphanbo' />
                        <attribute name='new_vudautu' />
                        <attribute name='new_loaihopdong' />
                        <attribute name='new_ngayphatsinh' />
                        <attribute name='new_hanthanhtoan' />
                        <attribute name='new_phanbodautuid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_thuacanhtac' operator='eq' uitype='new_thuadatcanhtac' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, CTHDDTmia.Id);
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
    }
}
