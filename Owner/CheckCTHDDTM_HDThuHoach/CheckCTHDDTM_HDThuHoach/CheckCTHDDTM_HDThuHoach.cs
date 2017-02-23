using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace CheckCTHDDTM_HDThuHoach
{
    public class CheckCTHDDTM_HDThuHoach : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            ITracingService trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            service = factory.CreateOrganizationService(context.UserId);

            EntityReference targetEntity = null;
            string relationshipName = string.Empty;
            EntityReferenceCollection relatedEntities = null;
            EntityReference relatedEntity = null;

            Entity hdthuhoach = new Entity();
            Entity thuadatcanhtac = new Entity();

            if (context.MessageName.ToLower().Trim() == "associate")
            {
                if (context.InputParameters.Contains("Relationship"))
                {
                    relationshipName = context.InputParameters["Relationship"].ToString();
                }

                if (relationshipName != "new_new_hopdongthuhoach_new_chitiethddtmia.")
                {
                    return;
                }

                if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is EntityReference)
                {
                    targetEntity = (EntityReference)context.InputParameters["Target"];
                    hdthuhoach = service.Retrieve(targetEntity.LogicalName, targetEntity.Id, new ColumnSet(new string[] { "new_doitacthuhoach", "new_doitacthuhoachkhdn" }));
                }

                if (context.InputParameters.Contains("RelatedEntities") && context.InputParameters["RelatedEntities"] is EntityReferenceCollection)
                {
                    relatedEntities = context.InputParameters["RelatedEntities"] as EntityReferenceCollection;
                    if (relatedEntities.Count > 0)
                    {
                        relatedEntity = relatedEntities[0];
                        thuadatcanhtac = service.Retrieve(relatedEntity.LogicalName, relatedEntity.Id, new ColumnSet(new string[] {"new_thuadat" }));
                    }
                    else
                    {
                        return;
                    }
                }

                trace.Trace("Start");
                List<Entity> lstLenhdon = RetrieveMultiRecord(service, "new_lenhdon",
                    new ColumnSet(new string[] { "statuscode" }), "new_thuacanhtac", thuadatcanhtac.Id);
                trace.Trace("Lấy dc danh sach lệnh đốn");
                bool co = false;
                Entity ldTemp = null;

                foreach (Entity ld in lstLenhdon)
                {
                    if (!ld.Contains("statuscode") || ((OptionSetValue)ld["statuscode"]).Value != 100000000)
                    {
                        continue;
                    }

                    if (ld.Contains("new_doitacthuhoach"))
                    {
                        if (!hdthuhoach.Contains("new_doitacthuhoach"))
                        {
                            continue;
                        }

                        if (((EntityReference)ld["new_doitacthuhoach"]).Id != ((EntityReference)hdthuhoach["new_doitacthuhoach"]).Id)
                        {
                            continue;
                        }
                    }
                    else if (ld.Contains("new_doitacthuhoachkhdn"))
                    {
                        if (!hdthuhoach.Contains("new_doitacthuhoachkhdn"))
                        {
                            continue;
                        }

                        if (((EntityReference)ld["new_doitacthuhoachkhdn"]).Id != ((EntityReference)hdthuhoach["new_doitacthuhoachkhdn"]).Id)
                        {
                            continue;
                        }
                    }
                    trace.Trace("Dò danh sách lệnh đốn");
                    ldTemp = ld;
                }

                List<Entity> lstDiadiemthuhoach = RetrieveMultiRecord(service, "new_diadiemthuhoach",
                    new ColumnSet(new string[] { "new_diadiemthuhoachid" }), "new_hopdongthuhoach", hdthuhoach.Id);
                trace.Trace("lấy danh sách địa điểm thu hoach");
                foreach (Entity en in lstDiadiemthuhoach)
                {
                    EntityCollection entcThuadat = RetrieveNNRecord(service, "new_thuadat", "new_diadiemthuhoach", "new_new_diadiemthuhoach_new_thuadat"
                        , new ColumnSet(true), "new_diadiemthuhoachid", en.Id);
                    trace.Trace("Lấy danh sách thửa đất trong dia điểm thu hoạch");
                    
                    foreach (Entity td in entcThuadat.Entities)
                    {
                        trace.Trace("Bat dau do DS thửa đất");
                        if (thuadatcanhtac.Contains("new_thuadat"))
                        {
                            trace.Trace("Thửa đất canh tác có thửa đất");
                            if (((EntityReference)thuadatcanhtac["new_thuadat"]).Id == td.Id)
                            {
                                co = true;
                            }
                        }
                    }
                    trace.Trace("Finish");

                    if ((co == true) || ldTemp == null)
                    {
                        throw new Exception("Chi tiết không hợp lệ");
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
