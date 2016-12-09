using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_AutoSum
{
    public class Plugin_AutoSum : IPlugin
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
                Entity target;

                if (context.MessageName.Trim().ToLower() == "disassociate" || context.MessageName.Trim().ToLower() == "associate")
                {
                    string relationshipName = "";
                    EntityReference targetEntity = new EntityReference();
                    EntityReferenceCollection relatedEntities = new EntityReferenceCollection();
                    if (context.InputParameters.Contains("Relationship"))
                    {
                        relationshipName = context.InputParameters["Relationship"].ToString();
                    }

                    if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is EntityReference)
                    {
                        targetEntity = (EntityReference)context.InputParameters["Target"];
                    }

                    if (context.InputParameters.Contains("RelatedEntities") && context.InputParameters["RelatedEntities"] is EntityReferenceCollection)
                    {
                        relatedEntities = context.InputParameters["RelatedEntities"] as EntityReferenceCollection;
                    }

                    QueryExpression q = new QueryExpression("new_autosum");
                    q.ColumnSet = new ColumnSet(new string[] { "new_autosumid", "new_name", "new_childentity", "new_parentfield", "new_childfield", "new_lookupfield", "new_datatype" });
                    q.Criteria = new FilterExpression();
                    q.Criteria.AddCondition(new ConditionExpression("new_relationentity", ConditionOperator.Equal, relationshipName));
                    q.Criteria.AddCondition(new ConditionExpression("new_active", ConditionOperator.Equal, true));
                    q.Criteria.AddCondition(new ConditionExpression("new_type", ConditionOperator.Equal, 100000001));
                    q.Criteria.AddCondition(new ConditionExpression("new_name", ConditionOperator.Equal, targetEntity.LogicalName));
                    EntityCollection entc = service.RetrieveMultiple(q);

                    if (entc.Entities.Count > 0)
                    {
                        foreach (Entity a in entc.Entities)
                        {
                            EntityReferenceCollection result = new EntityReferenceCollection();
                            QueryExpression query = new QueryExpression(a["new_childentity"].ToString());
                            query.ColumnSet = new ColumnSet(a["new_childfield"].ToString().Split(','));
                            LinkEntity linkEntity1 = new LinkEntity(a["new_childentity"].ToString(), relationshipName, a["new_childentity"].ToString() + "id", a["new_childentity"].ToString() + "id", JoinOperator.Inner);
                            LinkEntity linkEntity2 = new LinkEntity(relationshipName, a["new_name"].ToString() , a["new_name"].ToString() + "id", a["new_name"].ToString() + "id", JoinOperator.Inner);

                            linkEntity1.LinkEntities.Add(linkEntity2);
                            query.LinkEntities.Add(linkEntity1);

                            linkEntity2.LinkCriteria = new FilterExpression();
                            linkEntity2.LinkCriteria.AddCondition(new ConditionExpression(a["new_name"].ToString() + "id", ConditionOperator.Equal, targetEntity.Id));
                            EntityCollection entc2 = service.RetrieveMultiple(query);

                            decimal[] rs = new decimal[a["new_childfield"].ToString().Split(',').Length];

                            foreach (Entity b in entc2.Entities)
                            {
                                int i = -1;
                                if (a["new_datatype"].ToString().ToLower().Trim() == "decimal")
                                {
                                    foreach (string c in a["new_childfield"].ToString().Split(','))
                                        if (c.Trim() != "")
                                        {
                                            i++;
                                            rs[i] += (b.Attributes.Contains(c) ? (decimal)b[c] : new decimal(0));
                                        }
                                }
                                else
                                {
                                    foreach (string c in a["new_childfield"].ToString().Split(','))
                                        if (c.Trim() != "")
                                        {
                                            i++;
                                            rs[i] += (b.Attributes.Contains(c) ? ((Money)b[c]).Value : new decimal(0));
                                        }
                                }
                            }

                            Entity Ers = new Entity(a["new_name"].ToString());
                            Ers.Id = targetEntity.Id;
                            int k = -1;
                            foreach (string c in a["new_parentfield"].ToString().Split(','))
                                if (c.Trim() != "")
                                {
                                    k++;
                                    if (a["new_datatype"].ToString().ToLower().Trim() == "decimal")
                                        Ers[c] = rs[k];
                                    else
                                        Ers[c] = new Money(rs[k]);
                                }

                            service.Update(Ers);
                        }
                    }
                }
                else
                {
                    if (context.MessageName.ToLower().Trim() == "delete")
                    {
                        target = new Entity(((EntityReference)context.InputParameters["Target"]).LogicalName);
                        target.Id = ((EntityReference)context.InputParameters["Target"]).Id;
                    }
                    else
                        target = (Entity)context.InputParameters["Target"];
                    Entity fullEntity = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                    QueryExpression q = new QueryExpression("new_autosum");
                    q.ColumnSet = new ColumnSet(new string[] { "new_autosumid", "new_name", "new_childentity", "new_parentfield", "new_childfield", "new_lookupfield", "new_datatype" });
                    q.Criteria = new FilterExpression();
                    q.Criteria.AddCondition(new ConditionExpression("new_childentity", ConditionOperator.Equal, target.LogicalName));
                    q.Criteria.AddCondition(new ConditionExpression("new_active", ConditionOperator.Equal, true));
                    EntityCollection entc = service.RetrieveMultiple(q);

                    if (entc.Entities.Count > 0)
                    {
                        foreach (Entity a in entc.Entities)
                        {
                            QueryExpression q2 = new QueryExpression(a["new_childentity"].ToString());
                            q2.ColumnSet = new ColumnSet(a["new_childfield"].ToString().Split(','));
                            q2.Criteria = new FilterExpression();
                            q2.Criteria.AddCondition(new ConditionExpression(a["new_lookupfield"].ToString(), ConditionOperator.Equal, ((EntityReference)fullEntity[a["new_lookupfield"].ToString()]).Id));
                            EntityCollection entc2 = service.RetrieveMultiple(q2);

                            decimal[] rs = new decimal[a["new_childfield"].ToString().Split(',').Length];

                            foreach (Entity b in entc2.Entities)
                            {
                                if (b.Id == target.Id && context.MessageName.ToLower().Trim() == "delete")
                                {
                                }
                                else
                                {
                                    int i = -1;
                                    if (a["new_datatype"].ToString().ToLower().Trim() == "decimal")
                                    {
                                        foreach (string c in a["new_childfield"].ToString().Split(','))
                                            if (c.Trim() != "")
                                            {
                                                i++;
                                                rs[i] += (b.Attributes.Contains(c) ? (decimal)b[c] : new decimal(0));
                                            }
                                    }
                                    else
                                    {
                                        foreach (string c in a["new_childfield"].ToString().Split(','))
                                            if (c.Trim() != "")
                                            {
                                                i++;
                                                rs[i] += (b.Attributes.Contains(c) ? ((Money)b[c]).Value : new decimal(0));
                                            }
                                    }
                                }
                            }

                            Entity Ers = new Entity(a["new_name"].ToString());
                            Ers.Id = ((EntityReference)fullEntity[a["new_lookupfield"].ToString()]).Id;
                            int k = -1;
                            foreach (string c in a["new_parentfield"].ToString().Split(','))
                                if (c.Trim() != "")
                                {
                                    k++;
                                    if (a["new_datatype"].ToString().ToLower().Trim() == "decimal")
                                        Ers[c] = rs[k];
                                    else
                                        Ers[c] = new Money(rs[k]);
                                }

                            service.Update(Ers);
                        }
                    }
                }
            }
            catch (Exception ex) { }
        }
    }
}
