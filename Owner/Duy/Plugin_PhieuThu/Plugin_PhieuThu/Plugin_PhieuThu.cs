using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_PhieuThu
{
    public class Plugin_PhieuThu : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            try
            {
                IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);

                Entity target = (Entity)context.InputParameters["Target"];
                Entity phieuthu = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_sotienthu", "new_phieudenghithuno", "new_name" }));
                Entity pdnthuno = service.Retrieve("new_phieudenghithuno", ((EntityReference)phieuthu["new_phieudenghithuno"]).Id, new ColumnSet(new string[] { "new_tongtienthu" }));
                if (target.Contains("statuscode"))
                {
                    var temp = ((Microsoft.Xrm.Sdk.OptionSetValue)target["statuscode"]).Value;
                    if (temp.ToString() == "100000000") // đã duyệt
                    {
                        EntityCollection DSPBDT = RetrieveNNRecord(service, "new_phanbodautu", "new_phieuthu", "new_new_phieuthu_new_phanbodautu", new ColumnSet(new string[] { "new_datra", "new_conlai", "new_sotien", "new_name" }), "new_phieuthuid", phieuthu.Id);

                        foreach (Entity en in DSPBDT.Entities)
                        {                            
                            en["new_datra"] = new Money(((Money)en["new_datra"]).Value + ((Money)pdnthuno["new_tongtienthu"]).Value);
                            en["new_conlai"] = new Money(((Money)en["new_sotien"]).Value - ((Money)en["new_datra"]).Value);
                            service.Update(en);
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
