using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;
using System.Globalization;


namespace Plugin_UpdateNangSuatCTNTTuoimia
{
    public class UpdateNangSuat_SanLuongDukien : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            try
            {
                IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
                Entity target = (Entity)context.InputParameters["Target"];
                if (context.MessageName.ToLower().Trim() != "update" || (!target.Contains("statuscode")))
                    return;
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);

                if (((OptionSetValue)target["statuscode"]).Value.ToString() == "100000000")
                {
                    Entity nttuoimia = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                    string tinhtrang = ((OptionSetValue)nttuoimia["statuscode"]).Value.ToString();
                    List<Entity> chitietnttuoimia = RetrieveMultiRecord(service, "new_chitietnghiemthutuoimia", new ColumnSet(true), "new_nghiemthutuoimia", nttuoimia.Id);
                    foreach (Entity en in chitietnttuoimia)
                    {
                        Entity thuadatcanhtac = new Entity();
                        //List<Entity> lstthuadatcanhtac = RetrieveMultiRecord(service, "new_thuadatcanhtac", new ColumnSet(new string[] { "new_nangsuatlythuyet", "new_hopdongdautumia", "new_loaigocmia", "new_name" }), "new_thuadat", ((EntityReference)en["new_thuadat"]).Id);
                        Entity hopdongdautumia = service.Retrieve("new_hopdongdautumia", ((EntityReference)nttuoimia["new_hopdongtrongmia"]).Id, new ColumnSet(new string[] { "new_vudautu", "new_name" }));

                        QueryExpression q1 = new QueryExpression("new_thuadatcanhtac");
                        q1.ColumnSet = new ColumnSet(true);
                        q1.Criteria = new FilterExpression();
                        q1.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, ((EntityReference)en["new_thuadat"]).Id));
                        q1.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, ((EntityReference)nttuoimia["new_hopdongtrongmia"]).Id));
                        EntityCollection entc = service.RetrieveMultiple(q1);

                        if (entc.Entities.ToList().Count() > 0)
                        {
                            thuadatcanhtac = entc.Entities.ToList<Entity>().FirstOrDefault();
                            QueryExpression q = new QueryExpression("new_khaibaotuoitangnangsuat");
                            q.ColumnSet = new ColumnSet(true);
                            q.Criteria = new FilterExpression();
                            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, ((EntityReference)hopdongdautumia["new_vudautu"]).Id));
                            q.Criteria.AddCondition(new ConditionExpression("new_loaituoi", ConditionOperator.Equal, ((OptionSetValue)en["new_mucdichtuoi"]).Value));
                            q.Criteria.AddCondition(new ConditionExpression("new_loaimia", ConditionOperator.Equal, ((OptionSetValue)thuadatcanhtac["new_loaigocmia"]).Value));
                            EntityCollection lstKhaibaotuoitangnangsuat = service.RetrieveMultiple(q);

                            foreach (Entity item in lstKhaibaotuoitangnangsuat.Entities)
                            {
                                Entity Uthuadatcanhtac = service.Retrieve(thuadatcanhtac.LogicalName, thuadatcanhtac.Id, new ColumnSet(new string[] { "new_nangsuatlythuyet", "new_name" }));
                                decimal nangsuattoithieu = item.GetAttributeValue<decimal>("new_nangsuattoithieu");
                                Uthuadatcanhtac["new_nangsuatlythuyet"] = String.Format("{0:0.##}", nangsuattoithieu);

                                service.Update(Uthuadatcanhtac);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
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

        Entity RetrieveSingleRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>().FirstOrDefault();
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
