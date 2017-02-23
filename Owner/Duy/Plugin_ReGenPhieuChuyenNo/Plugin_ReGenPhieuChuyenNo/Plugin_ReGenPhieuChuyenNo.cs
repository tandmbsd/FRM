using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_ReGenPhieuChuyenNo
{
    public class Plugin_ReGenPhieuChuyenNo : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            Entity target = (Entity)context.InputParameters["Target"];
            #region Update
            if (context.MessageName.ToLower().Trim() == "update")
            {
                if (target.Contains("new_hinhthucchuyen"))
                {
                    Entity pcn_cthddtmia = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                    Entity pcn = service.Retrieve("new_phieuchuyenno", ((EntityReference)pcn_cthddtmia["new_phieuchuyenno"]).Id, new ColumnSet(true));

                    List<Entity> lst_pcnphanbodautu = RetrieveMultiRecord(service, "new_phieuchuyennophanbodautu", new ColumnSet(true), "new_phieuchuyenno", ((EntityReference)pcn_cthddtmia["new_phieuchuyenno"]).Id);
                    foreach (Entity en in lst_pcnphanbodautu)
                    {
                        service.Delete(en.LogicalName, en.Id);
                    }

                    if (((OptionSetValue)target["new_hinhthucchuyen"]).Value == 100000001) // chuyển 1 phần
                    {
                        #region Chuyển 1 phần
                        List<Entity> lst_pcncthdtmPBDT = RetrieveMultiRecord(service, "new_pcnchitiethddtmiaphanbodautu", new ColumnSet(true), "new_phieuchuyennochitiethddtmia", pcn_cthddtmia.Id);

                        foreach (Entity en in lst_pcncthdtmPBDT)
                        {
                            Entity pcn_pbdt = new Entity("new_phieuchuyennophanbodautu");

                            pcn_pbdt["new_name"] = pcn["new_name"].ToString() + "-" + en["new_name"].ToString();
                            pcn_pbdt["new_sotienphanbotronghan"] = new Money(en.Contains("new_sotienphanbotronghan") ? ((Money)en["new_sotienphanbotronghan"]).Value : 0);
                            pcn_pbdt["new_sotienphanboquahan"] = new Money(en.Contains("new_sotienphanboquahan") ? ((Money)en["new_sotienphanboquahan"]).Value : 0);
                            pcn_pbdt["new_sotienchuyennotronghan"] = new Money(en.Contains("new_sotienchuyennotronghan") ? ((Money)en["new_sotienchuyennotronghan"]).Value : 0);
                            pcn_pbdt["new_sotienchuyennoquahan"] = new Money(en.Contains("new_sotienchuyennoquahan") ? ((Money)en["new_sotienchuyennoquahan"]).Value : 0);
                            pcn_pbdt["new_phanbodautu"] = new EntityReference("new_phanbodautu", ((EntityReference)en["new_phanbodautu"]).Id);
                            pcn_pbdt["new_phieuchuyenno"] = pcn.ToEntityReference();
                            service.Create(pcn_pbdt);
                        }
                        #endregion
                    }
                    else if (((OptionSetValue)target["new_hinhthucchuyen"]).Value == 100000000) // chuyển toàn bộ
                    {
                        #region chuyển toàn bộ
                        List<Entity> lstPhanbodautu = RetrieveMultiRecord(service, "new_phanbodautu", new ColumnSet(true), "new_thuacanhtac", ((EntityReference)pcn_cthddtmia["new_chitiethddtmia"]).Id);

                        foreach (Entity en in lstPhanbodautu)
                        {
                            Entity pcn_pbdt = new Entity("new_phieuchuyennophanbodautu");
                            decimal[] arr = new decimal[2];
                            arr = sumsotienphanboquahan(en);

                            pcn_pbdt["new_name"] = pcn["new_name"].ToString() + "-" + en["new_name"].ToString();
                            pcn_pbdt["new_sotienphanbotronghan"] = new Money(arr[1]);
                            pcn_pbdt["new_sotienphanboquahan"] = new Money(arr[0]);
                            pcn_pbdt["new_sotienchuyennotronghan"] = new Money(arr[1]);
                            pcn_pbdt["new_sotienchuyennoquahan"] = new Money(arr[0]);
                            pcn_pbdt["new_phanbodautu"] = en.ToEntityReference();
                            pcn_pbdt["new_phieuchuyenno"] = pcn.ToEntityReference();

                            service.Create(pcn_pbdt);
                        }
                        #endregion
                    }
                    #region Update Phieuchuyenno

                    List<Entity> lst_pcnphanbodautuDel = RetrieveMultiRecord(service, "new_phieuchuyennophanbodautu", new ColumnSet(true), "new_phieuchuyenno", pcn.Id);
                    foreach (Entity en in lst_pcnphanbodautuDel)
                    {
                        service.Delete(en.LogicalName, en.Id);
                    }

                    List<Entity> lstPhanbodautu1 = new List<Entity>();

                    bool chuyenhddtthuedat = (bool)pcn["new_chuyenhddtthuedat"];

                    EntityCollection lstHDDTM = RetrieveNNRecord(service, "new_hopdongdautumia", "new_phieuchuyenno"
                        , "new_new_phieuchuyenno_new_hopdongdautumia", new ColumnSet(true), "new_phieuchuyennoid", pcn.Id);
                    List<Entity> lstChitiet = RetrieveMultiRecord(service, "new_phieuchuyennochitiethddtmia", new ColumnSet(new string[] { "new_chitiethddtmia" }), "new_phieuchuyenno", pcn.Id);

                    EntityCollection lstHDTD = RetrieveNNRecord(service, "new_hopdongthuedat", "new_phieuchuyenno"
                        , "new_new_phieuchuyenno_new_hopdongthuedat", new ColumnSet(true), "new_phieuchuyennoid", pcn.Id);
                    //throw new Exception(lstHDDTM.Entities.Count.ToString() + lstHDTD.Entities.Count.ToString() + lstChitiet.Count.ToString());
                    if (lstHDTD.Entities.Count > 0)
                    {
                        StringBuilder fetchXml = new StringBuilder();
                        fetchXml.Append("<fetch mapping='logical' version='1.0'>");
                        fetchXml.Append("<entity name='new_phanbodautu'>");
                        fetchXml.Append("<attribute name='new_phanbodautuid'/>");
                        fetchXml.Append("<filter type='or'>");

                        foreach (Entity hdtd in lstHDTD.Entities)
                        {
                            fetchXml.Append(string.Format("<condition attribute='new_hopdaudaututhuedat' operator='eq' value='{0}'></condition>", hdtd.Id));
                        }

                        fetchXml.Append("</filter>");
                        fetchXml.Append("</entity>");
                        fetchXml.Append("</fetch>");
                        EntityCollection entcHDTD = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));
                        lstPhanbodautu1.AddRange(entcHDTD.Entities);
                    }

                    if (lstHDDTM.Entities.Count > 0)
                    {
                        StringBuilder fetchXml = new StringBuilder();
                        fetchXml.Append("<fetch mapping='logical' version='1.0'>");
                        fetchXml.Append("<entity name='new_phanbodautu'>");
                        fetchXml.Append("<attribute name='new_phanbodautuid'/>");
                        fetchXml.Append("<filter type='or'>");

                        foreach (Entity hddtm in lstHDDTM.Entities)
                        {
                            fetchXml.Append(string.Format("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}'></condition>", hddtm.Id));
                        }

                        fetchXml.Append("</filter>");
                        fetchXml.Append("</entity>");
                        fetchXml.Append("</fetch>");
                        EntityCollection entcHDDTM = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));
                        lstPhanbodautu1.AddRange(entcHDDTM.Entities);
                    }

                    if (lstChitiet.Count > 0)
                    {
                        StringBuilder fetchXml = new StringBuilder();
                        fetchXml.Append("<fetch mapping='logical' version='1.0'>");
                        fetchXml.Append("<entity name='new_phanbodautu'>");
                        fetchXml.Append("<attribute name='new_phanbodautuid'/>");
                        fetchXml.Append("<filter type='or'>");

                        foreach (Entity ct in lstChitiet)
                        {
                            fetchXml.Append(string.Format("<condition attribute='new_thuacanhtac' operator='eq' value='{0}'></condition>", ((EntityReference)ct["new_chitiethddtmia"]).Id));
                        }

                        fetchXml.Append("</filter>");
                        fetchXml.Append("</entity>");
                        fetchXml.Append("</fetch>");
                        EntityCollection entcThuacatacnh = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));

                        lstPhanbodautu1.AddRange(entcThuacatacnh.Entities);
                    }

                    foreach (Entity en in lstPhanbodautu1)
                    {
                        Entity pcn_pbdt = new Entity("new_phieuchuyennophanbodautu");
                        decimal[] arr = new decimal[2];
                        arr = sumsotienphanboquahan(lstPhanbodautu1);

                        //pcn_pbdt["new_name"] = phieuchuyenno["new_name"].ToString() + "-" + en["new_name"].ToString();
                        pcn_pbdt["new_sotienphanbotronghan"] = new Money(arr[1]);

                        pcn_pbdt["new_sotienphanboquahan"] = new Money(arr[0]);
                        pcn_pbdt["new_sotienchuyennotronghan"] = new Money(arr[1]);

                        pcn_pbdt["new_sotienchuyennoquahan"] = new Money(arr[0]);
                        pcn_pbdt["new_phanbodautu"] = en.ToEntityReference();
                        pcn_pbdt["new_phieuchuyenno"] = new EntityReference(target.LogicalName, target.Id);

                        service.Create(pcn_pbdt);
                    }
                    #endregion
                }
                else if (target.Contains("new_chitiethddtmia") && target.Contains("new_hinhthucchuyen"))
                {
                    Entity pcn_cthddtmia = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                    Entity pcn = service.Retrieve("new_phieuchuyenno", ((EntityReference)pcn_cthddtmia["new_phieuchuyenno"]).Id, new ColumnSet(true));

                    List<Entity> lst_pcnphanbodautu = RetrieveMultiRecord(service, "new_phieuchuyennophanbodautu", new ColumnSet(true), "new_phieuchuyenno", ((EntityReference)pcn_cthddtmia["new_phieuchuyenno"]).Id);
                    foreach (Entity en in lst_pcnphanbodautu)
                    {
                        service.Delete(en.LogicalName, en.Id);
                    }

                    if (((OptionSetValue)target["new_hinhthucchuyen"]).Value == 100000001) // chuyển 1 phần
                    {
                        List<Entity> lst_pcncthdtmPBDT = RetrieveMultiRecord(service, "new_pcnchitiethddtmiaphanbodautu", new ColumnSet(true), "new_phieuchuyennochitiethddtmia", pcn_cthddtmia.Id);

                        foreach (Entity en in lst_pcncthdtmPBDT)
                        {
                            Entity pcn_pbdt = new Entity("new_phieuchuyennophanbodautu");

                            pcn_pbdt["new_name"] = pcn["new_name"].ToString() + "-" + en["new_name"].ToString();
                            pcn_pbdt["new_sotienphanbotronghan"] = new Money(en.Contains("new_sotienphanbotronghan") ? ((Money)pcn_pbdt["new_sotienphanbotronghan"]).Value : 0);
                            pcn_pbdt["new_sotienphanboquahan"] = new Money(en.Contains("new_sotienphanboquahan") ? ((Money)pcn_pbdt["new_sotienphanboquahan"]).Value : 0);
                            pcn_pbdt["new_sotienchuyennotronghan"] = new Money(en.Contains("new_sotienchuyennotronghan") ? ((Money)pcn_pbdt["new_sotienchuyennotronghan"]).Value : 0);
                            pcn_pbdt["new_sotienchuyennoquahan"] = new Money(en.Contains("new_sotienchuyennoquahan") ? ((Money)pcn_pbdt["new_sotienchuyennoquahan"]).Value : 0);
                            pcn_pbdt["new_phanbodautu"] = new EntityReference("new_phanbodautu", ((EntityReference)en["new_phanbodautu"]).Id);
                            pcn_pbdt["new_phieuchuyenno"] = pcn.ToEntityReference();
                            service.Create(pcn_pbdt);
                        }
                        #region Update Phieuchuyenno

                        List<Entity> lst_pcnphanbodautuDel = RetrieveMultiRecord(service, "new_phieuchuyennophanbodautu", new ColumnSet(true), "new_phieuchuyenno", pcn.Id);
                        foreach (Entity en in lst_pcnphanbodautuDel)
                        {
                            service.Delete(en.LogicalName, en.Id);
                        }

                        List<Entity> lstPhanbodautu1 = new List<Entity>();

                        bool chuyenhddtthuedat = (bool)pcn["new_chuyenhddtthuedat"];

                        EntityCollection lstHDDTM = RetrieveNNRecord(service, "new_hopdongdautumia", "new_phieuchuyenno"
                            , "new_new_phieuchuyenno_new_hopdongdautumia", new ColumnSet(true), "new_phieuchuyennoid", pcn.Id);
                        List<Entity> lstChitiet = RetrieveMultiRecord(service, "new_phieuchuyennochitiethddtmia", new ColumnSet(new string[] { "new_chitiethddtmia" }), "new_phieuchuyenno", pcn.Id);

                        EntityCollection lstHDTD = RetrieveNNRecord(service, "new_hopdongthuedat", "new_phieuchuyenno"
                            , "new_new_phieuchuyenno_new_hopdongthuedat", new ColumnSet(true), "new_phieuchuyennoid", pcn.Id);
                        //throw new Exception(lstHDDTM.Entities.Count.ToString() + lstHDTD.Entities.Count.ToString() + lstChitiet.Count.ToString());
                        if (lstHDTD.Entities.Count > 0)
                        {
                            StringBuilder fetchXml = new StringBuilder();
                            fetchXml.Append("<fetch mapping='logical' version='1.0'>");
                            fetchXml.Append("<entity name='new_phanbodautu'>");
                            fetchXml.Append("<attribute name='new_phanbodautuid'/>");
                            fetchXml.Append("<filter type='or'>");

                            foreach (Entity hdtd in lstHDTD.Entities)
                            {
                                fetchXml.Append(string.Format("<condition attribute='new_hopdaudaututhuedat' operator='eq' value='{0}'></condition>", hdtd.Id));
                            }

                            fetchXml.Append("</filter>");
                            fetchXml.Append("</entity>");
                            fetchXml.Append("</fetch>");
                            EntityCollection entcHDTD = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));
                            lstPhanbodautu1.AddRange(entcHDTD.Entities);
                        }

                        if (lstHDDTM.Entities.Count > 0)
                        {
                            StringBuilder fetchXml = new StringBuilder();
                            fetchXml.Append("<fetch mapping='logical' version='1.0'>");
                            fetchXml.Append("<entity name='new_phanbodautu'>");
                            fetchXml.Append("<attribute name='new_phanbodautuid'/>");
                            fetchXml.Append("<filter type='or'>");

                            foreach (Entity hddtm in lstHDDTM.Entities)
                            {
                                fetchXml.Append(string.Format("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}'></condition>", hddtm.Id));
                            }

                            fetchXml.Append("</filter>");
                            fetchXml.Append("</entity>");
                            fetchXml.Append("</fetch>");
                            EntityCollection entcHDDTM = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));
                            lstPhanbodautu1.AddRange(entcHDDTM.Entities);
                        }

                        if (lstChitiet.Count > 0)
                        {
                            StringBuilder fetchXml = new StringBuilder();
                            fetchXml.Append("<fetch mapping='logical' version='1.0'>");
                            fetchXml.Append("<entity name='new_phanbodautu'>");
                            fetchXml.Append("<attribute name='new_phanbodautuid'/>");
                            fetchXml.Append("<filter type='or'>");

                            foreach (Entity ct in lstChitiet)
                            {
                                fetchXml.Append(string.Format("<condition attribute='new_thuacanhtac' operator='eq' value='{0}'></condition>", ((EntityReference)ct["new_chitiethddtmia"]).Id));
                            }

                            fetchXml.Append("</filter>");
                            fetchXml.Append("</entity>");
                            fetchXml.Append("</fetch>");
                            EntityCollection entcThuacatacnh = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));

                            lstPhanbodautu1.AddRange(entcThuacatacnh.Entities);
                        }

                        foreach (Entity en in lstPhanbodautu1)
                        {
                            Entity pcn_pbdt = new Entity("new_phieuchuyennophanbodautu");
                            decimal[] arr = new decimal[2];
                            arr = sumsotienphanboquahan(lstPhanbodautu1);

                            //pcn_pbdt["new_name"] = phieuchuyenno["new_name"].ToString() + "-" + en["new_name"].ToString();
                            pcn_pbdt["new_sotienphanbotronghan"] = new Money(arr[1]);

                            pcn_pbdt["new_sotienphanboquahan"] = new Money(arr[0]);
                            pcn_pbdt["new_sotienchuyennotronghan"] = new Money(arr[1]);

                            pcn_pbdt["new_sotienchuyennoquahan"] = new Money(arr[0]);
                            pcn_pbdt["new_phanbodautu"] = en.ToEntityReference();
                            pcn_pbdt["new_phieuchuyenno"] = new EntityReference(target.LogicalName, target.Id);

                            service.Create(pcn_pbdt);
                        }
                        #endregion
                    }
                    else if (((OptionSetValue)target["new_hinhthucchuyen"]).Value == 100000000) // chuyển toàn bộ
                    {
                        List<Entity> lstPhanbodautu = RetrieveMultiRecord(service, "new_phanbodautu", new ColumnSet(true), "new_thuacanhtac", ((EntityReference)pcn_cthddtmia["new_chitiethddtmia"]).Id);

                        foreach (Entity en in lstPhanbodautu)
                        {
                            Entity pcn_pbdt = new Entity("new_phieuchuyennophanbodautu");
                            decimal[] arr = new decimal[2];
                            arr = sumsotienphanboquahan(en);

                            pcn_pbdt["new_name"] = pcn["new_name"].ToString() + "-" + en["new_name"].ToString();
                            pcn_pbdt["new_sotienphanbotronghan"] = new Money(arr[1]);
                            pcn_pbdt["new_sotienphanboquahan"] = new Money(arr[0]);
                            pcn_pbdt["new_sotienchuyennotronghan"] = new Money(arr[1]);
                            pcn_pbdt["new_sotienchuyennoquahan"] = new Money(arr[0]);
                            pcn_pbdt["new_phanbodautu"] = en.ToEntityReference();
                            pcn_pbdt["new_phieuchuyenno"] = pcn.ToEntityReference();
                            service.Create(pcn_pbdt);
                        }
                        #region Update Phieuchuyenno

                        List<Entity> lst_pcnphanbodautuDel = RetrieveMultiRecord(service, "new_phieuchuyennophanbodautu", new ColumnSet(true), "new_phieuchuyenno", pcn.Id);
                        foreach (Entity en in lst_pcnphanbodautuDel)
                        {
                            service.Delete(en.LogicalName, en.Id);
                        }

                        List<Entity> lstPhanbodautu1 = new List<Entity>();

                        bool chuyenhddtthuedat = (bool)pcn["new_chuyenhddtthuedat"];

                        EntityCollection lstHDDTM = RetrieveNNRecord(service, "new_hopdongdautumia", "new_phieuchuyenno"
                            , "new_new_phieuchuyenno_new_hopdongdautumia", new ColumnSet(true), "new_phieuchuyennoid", pcn.Id);
                        List<Entity> lstChitiet = RetrieveMultiRecord(service, "new_phieuchuyennochitiethddtmia", new ColumnSet(new string[] { "new_chitiethddtmia" }), "new_phieuchuyenno", pcn.Id);

                        EntityCollection lstHDTD = RetrieveNNRecord(service, "new_hopdongthuedat", "new_phieuchuyenno"
                            , "new_new_phieuchuyenno_new_hopdongthuedat", new ColumnSet(true), "new_phieuchuyennoid", pcn.Id);
                        //throw new Exception(lstHDDTM.Entities.Count.ToString() + lstHDTD.Entities.Count.ToString() + lstChitiet.Count.ToString());
                        if (lstHDTD.Entities.Count > 0)
                        {
                            StringBuilder fetchXml = new StringBuilder();
                            fetchXml.Append("<fetch mapping='logical' version='1.0'>");
                            fetchXml.Append("<entity name='new_phanbodautu'>");
                            fetchXml.Append("<attribute name='new_phanbodautuid'/>");
                            fetchXml.Append("<filter type='or'>");

                            foreach (Entity hdtd in lstHDTD.Entities)
                            {
                                fetchXml.Append(string.Format("<condition attribute='new_hopdaudaututhuedat' operator='eq' value='{0}'></condition>", hdtd.Id));
                            }

                            fetchXml.Append("</filter>");
                            fetchXml.Append("</entity>");
                            fetchXml.Append("</fetch>");
                            EntityCollection entcHDTD = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));
                            lstPhanbodautu1.AddRange(entcHDTD.Entities);
                        }

                        if (lstHDDTM.Entities.Count > 0)
                        {
                            StringBuilder fetchXml = new StringBuilder();
                            fetchXml.Append("<fetch mapping='logical' version='1.0'>");
                            fetchXml.Append("<entity name='new_phanbodautu'>");
                            fetchXml.Append("<attribute name='new_phanbodautuid'/>");
                            fetchXml.Append("<filter type='or'>");

                            foreach (Entity hddtm in lstHDDTM.Entities)
                            {
                                fetchXml.Append(string.Format("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}'></condition>", hddtm.Id));
                            }

                            fetchXml.Append("</filter>");
                            fetchXml.Append("</entity>");
                            fetchXml.Append("</fetch>");
                            EntityCollection entcHDDTM = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));
                            lstPhanbodautu1.AddRange(entcHDDTM.Entities);
                        }

                        if (lstChitiet.Count > 0)
                        {
                            StringBuilder fetchXml = new StringBuilder();
                            fetchXml.Append("<fetch mapping='logical' version='1.0'>");
                            fetchXml.Append("<entity name='new_phanbodautu'>");
                            fetchXml.Append("<attribute name='new_phanbodautuid'/>");
                            fetchXml.Append("<filter type='or'>");

                            foreach (Entity ct in lstChitiet)
                            {
                                fetchXml.Append(string.Format("<condition attribute='new_thuacanhtac' operator='eq' value='{0}'></condition>", ((EntityReference)ct["new_chitiethddtmia"]).Id));
                            }

                            fetchXml.Append("</filter>");
                            fetchXml.Append("</entity>");
                            fetchXml.Append("</fetch>");
                            EntityCollection entcThuacatacnh = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));

                            lstPhanbodautu1.AddRange(entcThuacatacnh.Entities);
                        }

                        foreach (Entity en in lstPhanbodautu1)
                        {
                            Entity pcn_pbdt = new Entity("new_phieuchuyennophanbodautu");
                            decimal[] arr = new decimal[2];
                            arr = sumsotienphanboquahan(lstPhanbodautu);

                            //pcn_pbdt["new_name"] = phieuchuyenno["new_name"].ToString() + "-" + en["new_name"].ToString();
                            pcn_pbdt["new_sotienphanbotronghan"] = new Money(arr[1]);

                            pcn_pbdt["new_sotienphanboquahan"] = new Money(arr[0]);
                            pcn_pbdt["new_sotienchuyennotronghan"] = new Money(arr[1]);

                            pcn_pbdt["new_sotienchuyennoquahan"] = new Money(arr[0]);
                            pcn_pbdt["new_phanbodautu"] = en.ToEntityReference();
                            pcn_pbdt["new_phieuchuyenno"] = new EntityReference(target.LogicalName, target.Id);

                            service.Create(pcn_pbdt);
                        }
                        #endregion
                    }
                }
            }
            #endregion

            #region Create
            else if (context.MessageName.ToLower().Trim() == "create")
            {
                if (target.Contains("new_hinhthucchuyen") && target.Contains("new_chitiethddtmia"))
                {
                    Entity pcn_cthddtmia = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                    Entity pcn = service.Retrieve("new_phieuchuyenno", ((EntityReference)pcn_cthddtmia["new_phieuchuyenno"]).Id, new ColumnSet(true));

                    List<Entity> lst_pcnphanbodautu = RetrieveMultiRecord(service, "new_phieuchuyennophanbodautu", new ColumnSet(true), "new_phieuchuyenno", ((EntityReference)pcn_cthddtmia["new_phieuchuyenno"]).Id);
                    foreach (Entity en in lst_pcnphanbodautu)
                    {
                        service.Delete(en.LogicalName, en.Id);
                    }

                    if (((OptionSetValue)target["new_hinhthucchuyen"]).Value == 100000001) // chuyển 1 phần
                    {
                        List<Entity> lst_pcncthdtmPBDT = RetrieveMultiRecord(service, "new_pcnchitiethddtmiaphanbodautu", new ColumnSet(true), "new_phieuchuyennochitiethddtmia", pcn_cthddtmia.Id);

                        foreach (Entity en in lst_pcncthdtmPBDT)
                        {
                            Entity pcn_pbdt = new Entity("new_phieuchuyennophanbodautu");

                            pcn_pbdt["new_name"] = pcn["new_name"].ToString() + "-" + en["new_name"].ToString();
                            pcn_pbdt["new_sotienphanbotronghan"] = new Money(en.Contains("new_sotienphanbotronghan") ? ((Money)pcn_pbdt["new_sotienphanbotronghan"]).Value : 0);
                            pcn_pbdt["new_sotienphanboquahan"] = new Money(en.Contains("new_sotienphanboquahan") ? ((Money)pcn_pbdt["new_sotienphanboquahan"]).Value : 0);
                            pcn_pbdt["new_sotienchuyennotronghan"] = new Money(en.Contains("new_sotienchuyennotronghan") ? ((Money)pcn_pbdt["new_sotienchuyennotronghan"]).Value : 0);
                            pcn_pbdt["new_sotienchuyennoquahan"] = new Money(en.Contains("new_sotienchuyennoquahan") ? ((Money)pcn_pbdt["new_sotienchuyennoquahan"]).Value : 0);
                            pcn_pbdt["new_phanbodautu"] = new EntityReference("new_phanbodautu", ((EntityReference)en["new_phanbodautu"]).Id);
                            pcn_pbdt["new_phieuchuyenno"] = pcn.ToEntityReference();
                            service.Create(pcn_pbdt);
                        }
                    }
                    else if (((OptionSetValue)target["new_hinhthucchuyen"]).Value == 100000000) // chuyển toàn bộ
                    {
                        List<Entity> lstPhanbodautu = RetrieveMultiRecord(service, "new_phanbodautu", new ColumnSet(true), "new_thuacanhtac", ((EntityReference)pcn_cthddtmia["new_chitiethddtmia"]).Id);

                        foreach (Entity en in lstPhanbodautu)
                        {
                            Entity pcn_pbdt = new Entity("new_phieuchuyennophanbodautu");
                            decimal[] arr = new decimal[2];
                            arr = sumsotienphanboquahan(lstPhanbodautu);

                            pcn_pbdt["new_name"] = pcn["new_name"].ToString() + "-" + en["new_name"].ToString();
                            pcn_pbdt["new_sotienphanbotronghan"] = new Money(arr[1]);
                            pcn_pbdt["new_sotienphanboquahan"] = new Money(arr[0]);
                            pcn_pbdt["new_sotienchuyennotronghan"] = new Money(arr[1]);
                            pcn_pbdt["new_sotienchuyennoquahan"] = new Money(arr[0]);
                            pcn_pbdt["new_phanbodautu"] = en.ToEntityReference();
                            pcn_pbdt["new_phieuchuyenno"] = pcn.ToEntityReference();
                            service.Create(pcn_pbdt);
                        }
                    }
                }
            }
            #endregion
        }

        decimal[] sumsotienphanboquahan(Entity en)
        {
            decimal[] kq = new decimal[2];

            if (en.GetAttributeValue<DateTime>("new_hanthanhtoan") <= DateTime.Today)
            {
                kq[0] += en.Contains("new_sotien") ? ((Money)en["new_sotien"]).Value : 0;
            }
            else
                kq[1] += en.Contains("new_sotien") ? ((Money)en["new_sotien"]).Value : 0;

            return kq;
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
