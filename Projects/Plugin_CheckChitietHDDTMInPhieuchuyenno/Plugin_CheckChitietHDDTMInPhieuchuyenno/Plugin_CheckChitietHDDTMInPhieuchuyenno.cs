using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_CheckChitietHDDTMInPhieuchuyenno
{
    public class Plugin_CheckChitietHDDTMInPhieuchuyenno : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        ITracingService trace;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];

            if (context.MessageName.ToLower().Trim() == "create")
            {
                #region Create
                bool flag = true;
                Entity pcn_cthddtm = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                int hinhthucchuyen = ((OptionSetValue)pcn_cthddtm["new_hinhthucchuyen"]).Value;

                Entity chitiethddtm = service.Retrieve("new_thuadatcanhtac", ((EntityReference)pcn_cthddtm["new_chitiethddtmia"]).Id, new ColumnSet(new string[] { "new_thuadatcanhtacid" }));
                Entity phieuchuyenno = service.Retrieve("new_phieuchuyenno", ((EntityReference)pcn_cthddtm["new_phieuchuyenno"]).Id, new ColumnSet(new string[] { "new_phieuchuyennoid", "new_chuyenhddtthuedat" }));

                EntityCollection lstHDDTM = RetrieveNNRecord(service, "new_hopdongdautumia", "new_phieuchuyenno", "new_new_phieuchuyenno_new_hopdongdautumia", new ColumnSet(true), "new_phieuchuyennoid", ((EntityReference)pcn_cthddtm["new_phieuchuyenno"]).Id);

                //foreach (Entity en in lstHDDTM.Entities)
                //{
                //    List<Entity> lstChitiet = RetrieveMultiRecord(service, "new_thuadatcanhtac", new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_hopdongdautumia", en.Id);

                //    foreach (Entity ct in lstChitiet)
                //    {
                //        if (ct.Id == chitiethddtm.Id)
                //        {
                //            flag = true;
                //        }
                //    }
                //}

                if (flag == false)
                {
                    throw new Exception("Chi tiết này không thuộc danh sách hợp đồng đầu tư mía đã chọn !!!");
                }
                else if (flag == true)
                {
                    List<Entity> lst_pcnphanbodautu = RetrieveMultiRecord(service, "new_phieuchuyennophanbodautu", new ColumnSet(true), "new_phieuchuyenno", phieuchuyenno.Id);
                    foreach (Entity en in lst_pcnphanbodautu)
                    {
                        service.Delete(en.LogicalName, en.Id);
                    }

                    List<Entity> lstPhanbodautu = new List<Entity>();
                    bool chuyenhddtthuedat = (bool)phieuchuyenno["new_chuyenhddtthuedat"];
                    EntityCollection lstHDTD = RetrieveNNRecord(service, "new_hopdongthuedat", "new_phieuchuyenno"
                        , "new_new_phieuchuyenno_new_hopdongthuedat", new ColumnSet(true), "new_phieuchuyennoid", phieuchuyenno.Id);

                    List<Entity> lstChitiet = RetrieveMultiRecord(service, "new_phieuchuyennochitiethddtmia", new ColumnSet(new string[] { "new_chitiethddtmia", "new_hinhthucchuyen" }), "new_phieuchuyenno", phieuchuyenno.Id);

                    if (chitiethddtm.Contains("new_hopdongdaututhuedat"))
                    {
                        Entity hdtd = service.Retrieve("new_hopdongthuedat", ((EntityReference)chitiethddtm["new_hopdongdaututhuedat"]).Id, new ColumnSet(new string[] { "new_hopdongthuedatid" }));
                        lstHDTD.Entities.Add(hdtd);
                    }

                    if (lstHDTD.Entities.Count > 0)
                    {
                        EntityCollection entcHDTD = PhanbodautufromHDTD(lstHDTD);
                        lstPhanbodautu.AddRange(entcHDTD.Entities);
                    }

                    if (lstHDDTM.Entities.Count > 0)
                    {
                        EntityCollection entcHDDTM = PhanbodautufromHDTD(lstHDDTM);
                        lstPhanbodautu.AddRange(entcHDDTM.Entities);
                    }

                    if (lstChitiet.Count > 0)
                    {
                        EntityCollection entcThuacatacnh = PhanbodautufromChitietHDDTM(lstChitiet);
                        lstPhanbodautu.AddRange(entcThuacatacnh.Entities);
                    }
                    
                    if (lstPhanbodautu.Count > 0)
                    {
                        foreach (Entity en in lstPhanbodautu)
                        {
                            Entity pcn_pbdt = new Entity("new_phieuchuyennophanbodautu");
                            decimal[] arr = new decimal[2];
                            arr = sumsotienphanboquahan(en);

                            pcn_pbdt["new_name"] = phieuchuyenno["new_name"].ToString() + "-" + en["new_name"].ToString();
                            pcn_pbdt["new_sotienphanbotronghan"] = new Money(arr[1]);

                            pcn_pbdt["new_sotienphanboquahan"] = new Money(arr[0]);
                            pcn_pbdt["new_sotienchuyennotronghan"] = new Money(arr[1]);
                            pcn_pbdt["new_sotienchuyennoquahan"] = new Money(arr[0]);
                            pcn_pbdt["new_phanbodautu"] = en.ToEntityReference();
                            pcn_pbdt["new_phieuchuyenno"] = phieuchuyenno.ToEntityReference();

                            service.Create(pcn_pbdt);
                        }
                    }

                }
                #endregion
            }
            else if (context.MessageName.ToLower().Trim() == "update")
            {
                #region Update
                if (target.Contains("new_hinhthucchuyen") || target.Contains("new_chitiethddtmia"))
                {
                    bool flag = true;
                    Entity pcn_cthddtm = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                    int hinhthucchuyen = ((OptionSetValue)pcn_cthddtm["new_hinhthucchuyen"]).Value;

                    Entity chitiethddtm = service.Retrieve("new_thuadatcanhtac", ((EntityReference)pcn_cthddtm["new_chitiethddtmia"]).Id, new ColumnSet(new string[] { "new_thuadatcanhtacid" }));
                    Entity phieuchuyenno = service.Retrieve("new_phieuchuyenno", ((EntityReference)pcn_cthddtm["new_phieuchuyenno"]).Id, new ColumnSet(new string[] { "new_phieuchuyennoid", "new_chuyenhddtthuedat" }));

                    EntityCollection lstHDDTM = RetrieveNNRecord(service, "new_hopdongdautumia", "new_phieuchuyenno", "new_new_phieuchuyenno_new_hopdongdautumia", new ColumnSet(true), "new_phieuchuyennoid", ((EntityReference)pcn_cthddtm["new_phieuchuyenno"]).Id);
                    //foreach (Entity en in lstHDDTM.Entities)
                    //{
                    //    List<Entity> lstChitiet = RetrieveMultiRecord(service, "new_thuadatcanhtac", new ColumnSet(new string[] { "new_thuadatcanhtacid" }), "new_hopdongdautumia", en.Id);

                    //    foreach (Entity ct in lstChitiet)
                    //    {
                    //        if (ct.Id == chitiethddtm.Id)
                    //        {
                    //            flag = true;
                    //        }
                    //    }
                    //}

                    if (flag == false)
                    {
                        throw new Exception("Chi tiết này không thuộc danh sách hợp đồng đầu tư mía đã chọn !!!");
                    }
                    else if (flag == true)
                    {
                        List<Entity> lst_pcnphanbodautu = RetrieveMultiRecord(service, "new_phieuchuyennophanbodautu", new ColumnSet(true), "new_phieuchuyenno", phieuchuyenno.Id);
                        foreach (Entity en in lst_pcnphanbodautu)
                        {
                            service.Delete(en.LogicalName, en.Id);
                        }

                        List<Entity> lstPhanbodautu = new List<Entity>();
                        bool chuyenhddtthuedat = (bool)phieuchuyenno["new_chuyenhddtthuedat"];
                        EntityCollection lstHDTD = RetrieveNNRecord(service, "new_hopdongthuedat", "new_phieuchuyenno"
                            , "new_new_phieuchuyenno_new_hopdongthuedat", new ColumnSet(true), "new_phieuchuyennoid", phieuchuyenno.Id);

                        List<Entity> lstChitiet = RetrieveMultiRecord(service, "new_phieuchuyennochitiethddtmia", new ColumnSet(new string[] { "new_chitiethddtmia", "new_hinhthucchuyen" }), "new_phieuchuyenno", phieuchuyenno.Id);

                        if (chitiethddtm.Contains("new_hopdongdaututhuedat"))
                        {
                            Entity hdtd = service.Retrieve("new_hopdongthuedat", ((EntityReference)chitiethddtm["new_hopdongdaututhuedat"]).Id, new ColumnSet(new string[] { "new_hopdongthuedatid" }));
                            lstHDTD.Entities.Add(hdtd);
                        }

                        if (lstHDTD.Entities.Count > 0)
                        {
                            EntityCollection entcHDTD = PhanbodautufromHDTD(lstHDTD);
                            lstPhanbodautu.AddRange(entcHDTD.Entities);
                        }

                        if (lstHDDTM.Entities.Count > 0)
                        {
                            EntityCollection entcHDDTM = PhanbodautufromHDTD(lstHDDTM);
                            lstPhanbodautu.AddRange(entcHDDTM.Entities);
                        }

                        if (lstChitiet.Count > 0)
                        {
                            EntityCollection entcThuacatacnh = PhanbodautufromChitietHDDTM(lstChitiet);
                            lstPhanbodautu.AddRange(entcThuacatacnh.Entities);
                        }
                        
                        if (lstPhanbodautu.Count > 0)
                        {
                            foreach (Entity en in lstPhanbodautu)
                            {
                                Entity pcn_pbdt = new Entity("new_phieuchuyennophanbodautu");
                                decimal[] arr = new decimal[2];
                                arr = sumsotienphanboquahan(en);

                                //pcn_pbdt["new_name"] = phieuchuyenno["new_name"].ToString() + "-" + en["new_name"].ToString();
                                pcn_pbdt["new_sotienphanbotronghan"] = new Money(arr[1]);

                                pcn_pbdt["new_sotienphanboquahan"] = new Money(arr[0]);
                                pcn_pbdt["new_sotienchuyennotronghan"] = new Money(arr[1]);
                                pcn_pbdt["new_sotienchuyennoquahan"] = new Money(arr[0]);
                                pcn_pbdt["new_phanbodautu"] = en.ToEntityReference();
                                pcn_pbdt["new_phieuchuyenno"] = phieuchuyenno.ToEntityReference();

                                service.Create(pcn_pbdt);
                            }
                        }
                    }
                }
                #endregion
            }
        }

        EntityCollection PhanbodautufromHDTD(EntityCollection lstHDTD)
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
            return entcHDTD;
        }

        EntityCollection PhanbodautufromHDDTM(EntityCollection lstHDDTM)
        {
            StringBuilder fetchXml = new StringBuilder();
            fetchXml.Append("<fetch mapping='logical' version='1.0'>");
            fetchXml.Append("<entity name='new_phanbodautu'>");
            fetchXml.Append("<attribute name='new_phanbodautuid'/>");
            fetchXml.Append("<filter type='and'>");
            fetchXml.Append(string.Format("<condition attribute='new_thuacanhtac' operator='ge' value='{0}'></condition>", null));
            fetchXml.Append("<filter type='or'>");

            foreach (Entity hddtm in lstHDDTM.Entities)
            {
                fetchXml.Append(string.Format("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}'></condition>", hddtm.Id));
            }

            fetchXml.Append("</filter>");
            fetchXml.Append("</entity>");
            fetchXml.Append("</fetch>");
            EntityCollection entcHDDTM = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));
            return entcHDDTM;
        }

        EntityCollection PhanbodautufromChitietHDDTM(List<Entity> lstChitiet)
        {
            EntityCollection entcThuadatcanhtacMaster = new EntityCollection();

            foreach (Entity ct in lstChitiet)
            {
                if (((OptionSetValue)ct["new_hinhthucchuyen"]).Value == 100000000) // chuyen toan bo 
                {
                    StringBuilder fetchXml = new StringBuilder();
                    fetchXml.Append("<fetch mapping='logical' version='1.0'>");
                    fetchXml.Append("<entity name='new_phanbodautu'>");
                    fetchXml.Append("<attribute name='new_phanbodautuid'/>");
                    fetchXml.Append("<filter type='or'>");
                    fetchXml.Append(string.Format("<condition attribute='new_thuacanhtac' operator='eq' value='{0}'></condition>", ((EntityReference)ct["new_chitiethddtmia"]).Id));
                    fetchXml.Append("</filter>");
                    fetchXml.Append("</entity>");
                    fetchXml.Append("</fetch>");
                    EntityCollection entcThuadatcanhtac = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));
                    entcThuadatcanhtacMaster.Entities.AddRange(entcThuadatcanhtac.Entities);
                }
                else if (((OptionSetValue)ct["new_hinhthucchuyen"]).Value == 100000001) // chuyen 1 phan
                {
                    List<Entity> lst_pcncthdtmPBDT = RetrieveMultiRecord(service, "new_pcnchitiethddtmiaphanbodautu", new ColumnSet(true), "new_phieuchuyennochitiethddtmia"
                        , ct.Id);
                    entcThuadatcanhtacMaster.Entities.AddRange(lst_pcncthdtmPBDT);
                }
            }

            return entcThuadatcanhtacMaster;
        }

        decimal[] sumsotienphanboquahan(Entity en)
        {
            decimal[] kq = new decimal[2];

            if (en.GetAttributeValue<DateTime>("new_hanthanhtoan") < DateTime.Today)
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
