using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;
using System.Web.Script.Serialization;

namespace Plugin_AutoSum
{
    public class Plugin_AutoSum : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            Entity target;
            if (context.MessageName.ToLower().Trim() == "create" || context.MessageName.ToLower().Trim() == "update")
                target = context.InputParameters["Target"] as Entity;
            else {
                target = new Entity(((EntityReference)context.InputParameters["Target"]).LogicalName);
                target.Id = ((EntityReference)context.InputParameters["Target"]).Id;
            }
            bool co = false;

            QueryExpression q = new QueryExpression("new_autosum");
            q.ColumnSet = new ColumnSet(new string[] { "new_name", "new_phuongthuctinh", "new_parentfield", "new_childfield", "new_lookupfield", "new_datatype" });
            q.Distinct = true;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_childentity", ConditionOperator.Equal, target.LogicalName));
            q.Criteria.AddCondition(new ConditionExpression("new_active", ConditionOperator.Equal, true));

            if (context.MessageName.ToLower().Trim() == "create" || context.MessageName.ToLower().Trim() == "update" || context.MessageName.Trim().ToLower() == "delete")
                q.Criteria.AddCondition(new ConditionExpression("new_type", ConditionOperator.Equal, 100000000));

            EntityCollection entc = service.RetrieveMultiple(q);

            if (entc.Entities.Count > 0)
            {
                if (context.MessageName.Trim().ToLower() == "update")
                    co = CheckRun(entc, target);
                else co = true;
            }

            if (co)
            {
                NhomLookupCollection nhomLookup = new NhomLookupCollection();

                foreach (Entity a in entc.Entities)
                {
                    if (a.Contains("new_phuongthuctinh") && a.Contains("new_parentfield") && a.Contains("new_childfield") && a.Contains("new_lookupfield") && a.Contains("new_datatype") && a.Contains("new_name"))
                        nhomLookup.AddRecord(a["new_name"].ToString(), a["new_lookupfield"].ToString(), a["new_childfield"].ToString(), a["new_parentfield"].ToString(), ((OptionSetValue)a["new_phuongthuctinh"]).Value, a["new_datatype"].ToString());
                }

                foreach (NhomLookup a in nhomLookup.NhomLookups)
                {
                    Guid lookupValue = Guid.Empty;

                    if (context.MessageName.ToLower().Trim() == "create" || context.MessageName.ToLower().Trim() == "update")
                    {
                        Entity fullEntity = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(a.lookupField));
                        if (fullEntity.Contains(a.lookupField))
                            lookupValue = ((EntityReference)fullEntity[a.lookupField]).Id;
                        else
                            break;
                    }
                    else
                    {
                        Entity fullEntity = context.PreEntityImages["PreImg"];
                        if (fullEntity.Contains(a.lookupField))
                            lookupValue = ((EntityReference)fullEntity[a.lookupField]).Id;
                        else
                            break;
                    }

                    Entity update = new Entity(a.parentEntity);
                    update.Id = lookupValue;

                    StringBuilder fetchXML = new StringBuilder();
                    fetchXML.AppendFormat("<fetch mapping='logical' distinct='false' aggregate='true' version='1.0'>");
                    fetchXML.AppendFormat("<entity name='" + target.LogicalName + "'>");

                    foreach (Field f in a.fields)
                    {
                        fetchXML.AppendFormat("<attribute name='" + f.childfield + "' aggregate='" + f.agg + "' alias='v_" + f.childfield + "'/>");
                    }
                    fetchXML.AppendFormat("<filter type='and'>");
                    fetchXML.AppendFormat("<condition attribute='" + a.lookupField + "' operator='eq' value='{0}' />", lookupValue.ToString());
                    fetchXML.AppendFormat("</filter>");
                    fetchXML.AppendFormat("</entity>");
                    fetchXML.AppendFormat("</fetch>");

                    bool check = false;
                    EntityCollection result = service.RetrieveMultiple(new FetchExpression(fetchXML.ToString()));
                    if (result.Entities.Count > 0)
                        foreach (Field f in a.fields)
                        {
                            if (((AliasedValue)result[0]["v_" + f.childfield]).Value != null)
                            {
                                update[f.parentfield] = (f.datatype == "decimal" ?
                                    (result[0].Contains("v_" + f.childfield) ? ((AliasedValue)result[0]["v_" + f.childfield]).Value : 0)
                                    : (result[0].Contains("v_" + f.childfield) ? ((AliasedValue)result[0]["v_" + f.childfield]).Value : new Money(0))
                                    );
                                check = true;
                            }
                        }
                    if (check) service.Update(update);
                }
            }
        }

        public bool CheckRun(EntityCollection collect, Entity target)
        {
            foreach (Entity a in collect.Entities)
                if (target.Contains(a["new_childfield"].ToString()))
                    return true;
            return false;
        }
    }
}
