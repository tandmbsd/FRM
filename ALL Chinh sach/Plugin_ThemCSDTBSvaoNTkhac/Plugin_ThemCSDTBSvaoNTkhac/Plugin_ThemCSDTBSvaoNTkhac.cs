using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace Plugin_ThemCSDTBSvaoNTkhac
{
    public class Plugin_ThemCSDTBSvaoNTkhac : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            //throw new Exception("chay plugin");
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            traceService.Trace(string.Format("Context Depth {0}", context.Depth));
            if (context.Depth > 1)
                return;

            if (context.InputParameters.Contains("Target") && (context.InputParameters["Target"] is Entity))
            {  
                traceService.Trace("Truoc id target");

                Entity target = (Entity)context.InputParameters["Target"];
                Guid entityId = target.Id;

                if (target.LogicalName == "new_nghiemthukhac")
                {
                    traceService.Trace("Begin plugin");

                    if (context.MessageName.ToUpper() == "CREATE" || context.MessageName.ToUpper() == "UPDATE")
                    {
                        Entity NTkhac = service.Retrieve("new_nghiemthukhac", entityId,
                            new ColumnSet(new string[] { "new_hopdongdautumia", "new_chitiethddtmia",
                                "new_khuyenkhichphattrien", "new_mohinhkhuyennong",
                                "createdon" , "new_dientich","statuscode" }));
                        
                        DateTime ngaytao = NTkhac.GetAttributeValue<DateTime>("createdon");
                        
                        if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000) // da duyet
                        {                            
                            Entity thuadatcanhtac = service.Retrieve("new_thuadatcanhtac",
                                ((EntityReference)NTkhac["new_chitiethddtmia"]).Id, new ColumnSet(new string[] { "new_thuadatcanhtacid" }));
                            
                            if (!NTkhac.Contains("new_khuyenkhichphattrien"))
                                return;
                            
                            EntityReferenceCollection ErfCollection = new EntityReferenceCollection();
                            ErfCollection.Add((EntityReference)NTkhac["new_khuyenkhichphattrien"]);
                            
                            service.Associate("new_thuadatcanhtac", thuadatcanhtac.Id,
                                new Relationship("new_new_chitiethddtmia_new_khuyenkhichpt"), ErfCollection);
                        }

                        if (NTkhac.Contains("new_hopdongdautumia") && NTkhac.Contains("new_chitiethddtmia"))
                        {
                            traceService.Trace("Begin plugin");

                            EntityReference HDDTmiaRef = NTkhac.GetAttributeValue<EntityReference>("new_hopdongdautumia");
                            Entity HDDTmia = service.Retrieve("new_hopdongdautumia", HDDTmiaRef.Id, new ColumnSet(new string[] { "new_vudautu" }));

                            Entity Vudautu = null;
                            if (HDDTmia.Contains("new_vudautu"))
                            {
                                EntityReference vudautuRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");
                                Vudautu = service.Retrieve("new_vudautu", vudautuRef.Id, new ColumnSet(new string[] { "new_name" }));
                            }
                            if (Vudautu != null)
                            {
                                EntityReference ChiTietHDRef = NTkhac.GetAttributeValue<EntityReference>("new_chitiethddtmia");
                                Entity ChiTietHD = service.Retrieve("new_thuadatcanhtac", ChiTietHDRef.Id, new ColumnSet(new string[] { "new_thamgiamohinhkhuyennong" }));

                                decimal tienbsHL = 0;
                                decimal tienbsKHL = 0;
                                decimal tienbsPB = 0;
                                decimal tongtienbs = 0;
                                decimal dientich = (NTkhac.Contains("new_dientich") ? (decimal)NTkhac["new_dientich"] : new decimal(0));

                                Entity en = new Entity(NTkhac.LogicalName);
                                en.Id = NTkhac.Id;

                                // Tim CSBS khi có Mo hinh khuyen nong
                                if (NTkhac.Contains("new_mohinhkhuyennong") && !NTkhac.Contains("new_khuyenkhichphattrien"))
                                {
                                    traceService.Trace("nghiem thu MHKN");

                                    if (ChiTietHD.Contains("new_thamgiamohinhkhuyennong"))
                                    {
                                        traceService.Trace("Thua dat tham gia MHKN");

                                        EntityReference MHKNRef = NTkhac.GetAttributeValue<EntityReference>("new_mohinhkhuyennong");
                                        Entity MHKN = service.Retrieve("new_mohinhkhuyennong", MHKNRef.Id, new ColumnSet(new string[] { "new_name" }));

                                        EntityCollection dsCSDTBSbyMHKN = FindCSDTBSbyMHKN(service, MHKN, Vudautu, ngaytao);

                                        EntityReference MHKNtrongChitietHDRef = ChiTietHD.GetAttributeValue<EntityReference>("new_thamgiamohinhkhuyennong");
                                        //Entity MHKNtrongChitietHD = service.Retrieve("new_mohinhkhuyennong", MHKNtrongChitietHDRef.Id, new ColumnSet(new string[] { "new_name" }));

                                        if (MHKNRef.Id == MHKNtrongChitietHDRef.Id)
                                        {
                                            if (dsCSDTBSbyMHKN != null && dsCSDTBSbyMHKN.Entities.Count > 0)
                                            {
                                                Entity a = dsCSDTBSbyMHKN[0];

                                                tienbsHL = (a.Contains("new_sotienbosung") ? a.GetAttributeValue<Money>("new_sotienbosung").Value : 0);
                                                tienbsKHL = (a.Contains("new_sotienbosung_khl") ? a.GetAttributeValue<Money>("new_sotienbosung_khl").Value : 0);
                                                tienbsPB = (a.Contains("new_bosungphanbon") ? a.GetAttributeValue<Money>("new_bosungphanbon").Value : 0);

                                                tongtienbs = tienbsHL * dientich + tienbsKHL * dientich;

                                                en["new_dinhmuchl"] = new Money(tienbsHL);
                                                en["new_dinhmuckhl"] = new Money(tienbsKHL);
                                                en["new_dautuhl"] = new Money(tienbsHL * dientich);
                                                en["new_dautukhl"] = new Money(tienbsKHL * dientich);
                                                en["new_thanhtien"] = new Money(tongtienbs);
                                                en["new_chinhsachdautubosung"] = a.ToEntityReference();
                                                service.Update(en);
                                            }
                                        }
                                        else
                                            throw new InvalidPluginExecutionException("Thửa đất không tham gia mô hình khuyến nông " + MHKN["new_name"].ToString());
                                    }

                                } // if (NTkhac.Contains("new_mohinhkhuyennong") && !NTkhac.Contains("new_khuyenkhichphattrien"))

                                // Tim CSBS khi có Khuyen khich phat trien
                                if (NTkhac.Contains("new_khuyenkhichphattrien") && !NTkhac.Contains("new_mohinhkhuyennong"))
                                {
                                    EntityCollection dsKKPTHDCT = RetrieveNNRecord(service, "new_khuyenkhichphattrien", "new_thuadatcanhtac", "new_new_chitiethddtmia_new_khuyenkhichpt",
                                        new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }), "new_thuadatcanhtacid", ChiTietHD.Id);

                                    EntityReference KKPTcuaNTkhacRef = NTkhac.GetAttributeValue<EntityReference>("new_khuyenkhichphattrien");
                                    Entity KKPTcuaNTkhac = service.Retrieve("new_khuyenkhichphattrien", KKPTcuaNTkhacRef.Id, new ColumnSet(new string[] { "new_name" }));
                                    traceService.Trace("Ngày tạo: " + ngaytao.ToString());
                                    traceService.Trace("Danh sach KKPT cua CT : " + dsKKPTHDCT.Entities.Count.ToString());
                                    if (dsKKPTHDCT != null && dsKKPTHDCT.Entities.Count > 0)
                                    {
                                        foreach (Entity KKPTHDCT in dsKKPTHDCT.Entities)
                                        {
                                            if (KKPTHDCT.Id == KKPTcuaNTkhac.Id)
                                            {
                                                traceService.Trace("Có khuyến khích phat triển giống nhau");
                                                EntityCollection dsCSDTBSbyKKPT = FindCSDTBSbyKKPT(service, KKPTcuaNTkhac, Vudautu, ngaytao);
                                                traceService.Trace("ds chinh sach dau tu bo sung : "+ dsCSDTBSbyKKPT.Entities.Count.ToString());
                                                if (dsCSDTBSbyKKPT != null && dsCSDTBSbyKKPT.Entities.Count > 0)
                                                {
                                                    Entity a = dsCSDTBSbyKKPT[0];
                                                    tienbsHL = (a.Contains("new_sotienbosung") ? a.GetAttributeValue<Money>("new_sotienbosung").Value : 0);
                                                    tienbsKHL = (a.Contains("new_sotienbosung_khl") ? a.GetAttributeValue<Money>("new_sotienbosung_khl").Value : 0);
                                                    tienbsPB = (a.Contains("new_bosungphanbon") ? a.GetAttributeValue<Money>("new_bosungphanbon").Value : 0);

                                                    tongtienbs = tienbsHL * dientich + tienbsKHL * dientich;

                                                    en["new_dinhmuchl"] = new Money(tienbsHL);
                                                    en["new_dinhmuckhl"] = new Money(tienbsKHL);
                                                    en["new_dautuhl"] = new Money(tienbsHL * dientich);
                                                    en["new_dautukhl"] = new Money(tienbsKHL * dientich);
                                                    en["new_thanhtien"] = new Money(tongtienbs);
                                                    en["new_chinhsachdautubosung"] = a.ToEntityReference();
                                                    service.Update(en);
                                                }
                                            }
                                        }
                                    }
                                    
                                    //else
                                        //throw new InvalidPluginExecutionException("Thửa đất không tham gia khuyến khích phát triển ");

                                } // if (NTkhac.Contains("new_khuyenkhichphattrien") && !NTkhac.Contains("new_mohinhkhuyennong"))
                            }
                        }
                    }
                }
            }
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
        public static EntityCollection FindCSDTBSbyMHKN(IOrganizationService crmservices, Entity MHNK, Entity vuDT, DateTime ngay)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_chinhsachdautuchitiet'>
                        <attribute name='new_chinhsachdautuchitietid' />
                        <attribute name='new_name' />
                        <attribute name='createdon' />
                        <attribute name='new_sotienbosung' />
                        <attribute name='new_sotienbosung_khl' />
                        <attribute name='new_bosungphanbon' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_mohinhkhuyennong' operator='eq' uitype='new_mohinhkhuyennong' value='{0}' />
                          <condition attribute='new_nghiemthu' operator='eq' value='1'/>
                          <condition attribute='statecode' operator='eq' value='0' />
                          <condition attribute='new_vudautu' operator='eq' value='{1}' />
                          <condition attribute='new_tungay' operator='on-or-before' value='{2}' />
                          <condition attribute='new_denngay' operator='on-or-after' value='{3}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, MHNK.Id, vuDT.Id, ngay, ngay);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

        public static EntityCollection FindCSDTBSbyKKPT(IOrganizationService crmservices, Entity KKPT, Entity vuDT, DateTime ngay)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_chinhsachdautuchitiet'>
                        <attribute name='new_chinhsachdautuchitietid' />
                        <attribute name='new_name' />
                        <attribute name='createdon' />
                        <attribute name='new_sotienbosung' />
                        <attribute name='new_sotienbosung_khl' />
                        <attribute name='new_bosungphanbon' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_khuyenkhichphattrien' operator='eq' uitype='new_khuyenkhichphattrien' value='{0}' />
                          <condition attribute='new_nghiemthu' operator='eq' value='1'/>
                          <condition attribute='statecode' operator='eq' value='0' />
                          <condition attribute='new_vudautu' operator='eq' value='{1}' />
                          <condition attribute='new_tungay' operator='on-or-before' value='{2}' />
                          <condition attribute='new_denngay' operator='on-or-after' value='{3}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, KKPT.Id, vuDT.Id, ngay, ngay);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
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
    }
}
