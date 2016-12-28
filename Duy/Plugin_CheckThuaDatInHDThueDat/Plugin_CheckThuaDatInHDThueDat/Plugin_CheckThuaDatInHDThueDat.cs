using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_CheckThuaDatInHDThueDat
{
    public class Plugin_CheckThuaDatInHDThueDat : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));

            service = factory.CreateOrganizationService(context.UserId);

            EntityReference targetEntity = null;
            string relationshipName = string.Empty;
            EntityReferenceCollection relatedEntities = null;
            EntityReference relatedEntity = null;

            Entity datthue = new Entity();
            Entity thuadat = new Entity();
            //throw new Exception("dsadsa");
            int count = 0;
            //String parameters = "";

            //foreach (KeyValuePair<string, object> attr in context.InputParameters)
            //{
            //    parameters += attr.Key.ToString();
            //}

            //throw new Exception(parameters);

            if (context.MessageName.ToLower().Trim() == "associate")
            {
                //get the "relationship"
                if (context.InputParameters.Contains("Relationship"))
                {
                    relationshipName = context.InputParameters["Relationship"].ToString();
                }

                //check the relationshipname with intended one
                if (relationshipName != "new_new_datthue_new_thuadat.")
                {
                    return;
                }

                // Get Entity 1 reference from “Target” Key from context

                if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is EntityReference)
                {
                    targetEntity = (EntityReference)context.InputParameters["Target"];
                    datthue = service.Retrieve(targetEntity.LogicalName, targetEntity.Id, new ColumnSet(new string[] { "new_hopdongthuedat" }));
                }

                if (context.InputParameters.Contains("RelatedEntities") && context.InputParameters["RelatedEntities"] is EntityReferenceCollection)
                {
                    relatedEntities = context.InputParameters["RelatedEntities"] as EntityReferenceCollection;
                    if (relatedEntities.Count > 0)
                    {
                        relatedEntity = relatedEntities[0];
                        thuadat = service.Retrieve(relatedEntity.LogicalName, relatedEntity.Id, new ColumnSet(new string[] { "new_diachi" }));
                    }
                    else
                    {
                        return;
                    }
                }

                Entity hopdongdaututhuedat = service.Retrieve("new_hopdongthuedat", ((EntityReference)datthue["new_hopdongthuedat"]).Id, new ColumnSet(true));
                EntityCollection lstThuadat = RetrieveNNRecord(service, "new_thuadat", "new_datthue", "new_new_datthue_new_thuadat",
                    new ColumnSet(true), "new_datthueid", datthue.Id);

                foreach (Entity en in lstThuadat.Entities)
                {
                    if (en.Id == thuadat.Id)
                    {
                        count++;
                    }
                }

                if (count > 1)
                {
                    throw new Exception("Thửa đất đã tồn tại");
                }

                if (hopdongdaututhuedat.Contains("new_quocgia") && thuadat.Contains("new_diachi"))
                {
                    Entity diachi = service.Retrieve("new_diachi", ((EntityReference)thuadat["new_diachi"]).Id, new ColumnSet(new string[] { "new_quocgia" }));

                    if (((EntityReference)hopdongdaututhuedat["new_quocgia"]).Id != ((EntityReference)diachi["new_quocgia"]).Id)
                    {
                        throw new Exception("Quốc gia của thửa đất và hợp đồng tư thuê đất không giống nhau !! ");
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
    }
}
