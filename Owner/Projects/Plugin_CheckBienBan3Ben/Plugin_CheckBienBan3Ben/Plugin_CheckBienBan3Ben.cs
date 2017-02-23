using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_CheckBienBan3Ben
{
    public class Plugin_CheckBienBan3Ben : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000)
            {
                Entity lenhchi = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                EntityCollection lstLenhdon = RetrieveNNRecord(service, "new_lenhdon", "new_bangketienmia",
                    "new_new_bangketienmia_new_lenhdon", new ColumnSet(true), "new_bangketienmiaid", lenhchi.Id);
                foreach (Entity en in lstLenhdon.Entities)
                {
                    if (en.Contains("new_hopdongthuhoach"))
                    {
                        Entity KH = null;
                        Entity KHDN = null;

                        if (en.Contains("new_khachhang"))
                            KH = service.Retrieve("contact", ((EntityReference)en["new_khachhang"]).Id,
                                new ColumnSet(new string[] { "contactid" }));
                        else if (en.Contains("new_khachhangdoanhnghiep"))
                            KHDN = service.Retrieve("account", ((EntityReference)en["new_khachhangdoanhnghiep"]).Id,
                                new ColumnSet(new string[] { "accountid" }));

                        if (en.Contains("new_hopdongdautumia") && en.Contains("new_vudautu"))
                        {
                            Entity HD = service.Retrieve("new_hopdongdautumia", ((EntityReference)en["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_hopdongdautumiaid" }));
                            Entity VDT = service.Retrieve("new_vudautu", ((EntityReference)en["new_vudautu"]).Id, new ColumnSet(new string[] { "new_vudautuid" }));

                            Entity BBCongdon = KH != null ? FindBBCongDon(service, KH, HD, VDT) : FindBBCongDonKHDN(service, KHDN, HD, VDT);
                            if (BBCongdon == null)
                            {
                                throw new Exception(en["new_name"].ToString() + " chưa có biên bản 3 bên !!!");
                            }
                        }

                        Entity newLenhdon = new Entity(en.LogicalName);
                        newLenhdon = service.Retrieve(en.LogicalName, en.Id, new ColumnSet(new string[] { "statuscode" }));

                        newLenhdon["statuscode"] = new OptionSetValue(100000002);
                        service.Update(newLenhdon);
                    }
                }
            }
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

        public static Entity FindBBCongDon(IOrganizationService crmservices, Entity khachhang, Entity hddtMia, Entity vudautu)
        {
            string fetchXml =
               @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_bienbanthoathuancongdon'>
                    <attribute name='new_name' />
                    <attribute name='createdon' />
                    <attribute name='new_mabienban' />
                    <attribute name='new_vudautu' />
                    <attribute name='new_tram' />
                    <attribute name='new_ngaylapphieu' />
                    <attribute name='new_chumiakhdn' />
                    <attribute name='new_chumiakh' />
                    <attribute name='new_canbonongvu' />
                    <attribute name='new_bienbanthoathuancongdonid' />
                    <order attribute='new_ngaylapphieu' descending='true' />
                    <filter type='and'>
                      <condition attribute='statecode' operator='eq' value='0' />
                      <condition attribute='new_chumiakh' operator='eq' uitype='contact' value='{0}' />
                      <condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='{1}' />
                      <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{2}' />
                      <condition attribute='statuscode' operator='eq' value='100000000' />
                    </filter>
                  </entity>
                </fetch>";

            Guid khId = khachhang.Id;
            Guid hddtmiaId = hddtMia.Id;
            Guid vudautuId = vudautu.Id;
            fetchXml = string.Format(fetchXml, khId, hddtmiaId, vudautuId);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            if (entc != null && entc.Entities.Count() > 0)
            {
                return entc.Entities[0];
            }
            else
            {
                return null;
            }
        }

        public static Entity FindBBCongDonKHDN(IOrganizationService crmservices, Entity khachhangDN, Entity hddtMia, Entity vudautu)
        {
            string fetchXml =
               @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='new_bienbanthoathuancongdon'>
                    <attribute name='new_name' />
                    <attribute name='createdon' />
                    <attribute name='new_mabienban' />
                    <attribute name='new_vudautu' />
                    <attribute name='new_tram' />
                    <attribute name='new_ngaylapphieu' />
                    <attribute name='new_chumiakhdn' />
                    <attribute name='new_chumiakh' />
                    <attribute name='new_canbonongvu' />
                    <attribute name='new_bienbanthoathuancongdonid' />
                    <order attribute='new_ngaylapphieu' descending='true' />
                    <filter type='and'>
                      <condition attribute='statecode' operator='eq' value='0' />
                      <condition attribute='new_chumiakhdn' operator='eq' uitype='account' value='{0}' />
                      <condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='{1}' />
                      <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{2}' />
                      <condition attribute='statuscode' operator='eq' value='100000000' />
                    </filter>
                  </entity>
                </fetch>";

            Guid khId = khachhangDN.Id;
            Guid hddtmiaId = hddtMia.Id;
            Guid vudautuId = vudautu.Id;
            fetchXml = string.Format(fetchXml, khId, hddtmiaId, vudautuId);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            if (entc != null && entc.Entities.Count() > 0)
            {
                return entc.Entities[0];
            }
            else
            {
                return null;
            }
        }
    }
}
