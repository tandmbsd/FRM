using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_CheckXeInHDVanChuyen
{
    public class Plugin_CheckXeInHDVanChuyen : IPlugin
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

            Entity hdvanchuyen = new Entity();
            Entity xevanchuyen = new Entity();
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
                if (relationshipName != "new_new_hopdongvanchuyen_new_xevanchuyen.")
                {
                    return;
                }

                // Get Entity 1 reference from “Target” Key from context

                if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is EntityReference)
                {
                    targetEntity = (EntityReference)context.InputParameters["Target"];
                    hdvanchuyen = service.Retrieve(targetEntity.LogicalName, targetEntity.Id, new ColumnSet(true));
                }
                string statuscode = ((OptionSetValue)hdvanchuyen["statuscode"]).Value.ToString();
                Entity vudautu = service.Retrieve("new_vudautu", ((EntityReference)hdvanchuyen["new_vudautu"]).Id, new ColumnSet(true));

                if (context.InputParameters.Contains("RelatedEntities") && context.InputParameters["RelatedEntities"] is EntityReferenceCollection)
                {
                    relatedEntities = context.InputParameters["RelatedEntities"] as EntityReferenceCollection;
                    relatedEntity = relatedEntities[0];

                    xevanchuyen = service.Retrieve(relatedEntity.LogicalName, relatedEntity.Id, new ColumnSet(true));

                }

                QueryExpression q = new QueryExpression("new_hopdongvanchuyen");
                q.ColumnSet = new ColumnSet(true);
                q.Criteria = new FilterExpression();
                q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautu.Id));
                q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));
                EntityCollection entc = service.RetrieveMultiple(q);
                List<Entity> lstHopdongvc = entc.Entities.ToList<Entity>();
                int count = 0;
                foreach (Entity hdvc in lstHopdongvc)
                {

                    //throw new Exception(hdvc["new_name"].ToString() + "  " + lstHopdongvc.Count.ToString());
                    EntityCollection eXevanchuyen = RetrieveNNRecord(service, "new_xevanchuyen", "new_hopdongvanchuyen",
                        "new_new_hopdongvanchuyen_new_xevanchuyen", new ColumnSet(true), "new_hopdongvanchuyenid", hdvc.Id);
                    List<Entity> lstXevanchuyen = eXevanchuyen.Entities.ToList<Entity>();

                    foreach (Entity xvc in lstXevanchuyen)
                    {
                        if (xvc.Id == xevanchuyen.Id)
                        {
                            count++;
                        }
                    }
                }
                if (statuscode == "100000000")
                {
                    if (count > 1)
                    {
                        throw new Exception("Xe này đã ký hợp đồng vận chuyển khác !");
                    }
                }
                else
                {
                    if (count > 0)
                    {
                        throw new Exception("Xe này đã ký hợp đồng vận chuyển khác !");
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
