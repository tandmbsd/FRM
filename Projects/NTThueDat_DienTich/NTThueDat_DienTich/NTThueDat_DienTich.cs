using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace NTThueDat_DienTich
{
    public class NTThueDat_DienTich : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            ITracingService trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value.ToString() == "100000000")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);

                Entity nghiemthuthuedat = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_datthue" }));
                trace.Trace("1");
                Entity chitiethddtthuedat = service.Retrieve("new_datthue", ((EntityReference)nghiemthuthuedat["new_datthue"]).Id, new ColumnSet(true));
                trace.Trace("2");
                List<Entity> lstChitietntthuedat = RetrieveMultiRecord(service, "new_chitietnghiemthuthuedat", new ColumnSet(true), "new_nghiemthuthuedat", target.Id);
                trace.Trace("3");
                EntityReferenceCollection ErCl = new EntityReferenceCollection();
                EntityCollection lstThuadat = RetrieveNNRecord(service, "new_thuadat", "new_datthue", "new_new_datthue_new_thuadat", new ColumnSet(true), "new_datthueid", chitiethddtthuedat.Id);

                foreach (Entity en in lstChitietntthuedat)
                {
                    trace.Trace("4");
                    QueryExpression q = new QueryExpression("new_chitiethdthuedat_thuadat");
                    q.ColumnSet = new ColumnSet(new string[] { "new_chitiethdthuedat_thuadatid" });
                    q.Criteria = new FilterExpression();
                    q.Criteria.AddCondition(new ConditionExpression("new_chitiethdthuedat", ConditionOperator.Equal, chitiethddtthuedat.Id));
                    q.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, ((EntityReference)en["new_thuadat"]).Id));
                    Entity chitietthuedat_thuadat = service.RetrieveMultiple(q).Entities.FirstOrDefault();

                    if (chitietthuedat_thuadat != null && chitietthuedat_thuadat.Id != Guid.Empty)
                    {
                        trace.Trace("5");
                        Entity t = service.Retrieve(chitietthuedat_thuadat.LogicalName, chitietthuedat_thuadat.Id,
                            new ColumnSet(new string[] { "new_dientichthucthue", "new_name", "new_sotiendaututhucte" }));
                        trace.Trace("6");
                        decimal dientichnghiemthu = en.Contains("new_dientichnghiemthu") ? (decimal)en["new_dientichnghiemthu"] : 0;
                        t["new_dientichthucthue"] = dientichnghiemthu;
                        t["new_sotiendaututhucte"] = new Money(en.Contains("new_sotiendautu") ? ((Money)en["new_sotiendautu"]).Value : new decimal(0));

                        service.Update(t);
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
