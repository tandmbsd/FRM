using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Update_DienTichMiaToGoc
{
    public class Update_DienTichMiaToGoc : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            Entity target = null;
            Entity hopdongdautumia = null;

            if (context.MessageName == "Create" || context.MessageName == "Update")
            {
                target = (Entity)context.InputParameters["Target"];

                Entity chitiet = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_hopdongdautumia" }));

                hopdongdautumia = service.Retrieve("new_hopdongdautumia",
                   ((EntityReference)chitiet["new_hopdongdautumia"]).Id,
                   new ColumnSet(new string[] { "new_dientichmiato", "new_dientichmiagoc" }));
            }
            else if (context.MessageName == "Delete")
            {
                target = (Entity)context.PreEntityImages["PreImg"];
                hopdongdautumia = service.Retrieve("new_hopdongdautumia",
                   ((EntityReference)target["new_hopdongdautumia"]).Id,
                   new ColumnSet(new string[] { "new_dientichmiato", "new_dientichmiagoc" }));
            }

            if (target.Contains("new_hopdongdautumia") || target.Contains("new_loaigocmia"))
            {
                decimal dientichmiato = 0;
                decimal dientichmiagoc = 0;

                List<Entity> lstThuadatcanhtac = RetrieveMultiRecord(service, "new_thuadatcanhtac",
                    new ColumnSet(new string[] { "new_dientichconlai", "new_loaigocmia" }), "new_hopdongdautumia", hopdongdautumia.Id);

                foreach (Entity en in lstThuadatcanhtac)
                {
                    if (!en.Contains("new_loaigocmia"))
                        continue;

                    int loaigocmia = ((OptionSetValue)en["new_loaigocmia"]).Value;
                    decimal dientichconlai = en.Contains("new_dientichconlai") ? (decimal)en["new_dientichconlai"] : new decimal(0);

                    if (loaigocmia == 100000000) // miato
                        dientichmiato += dientichconlai;
                    else if (loaigocmia == 100000001) // mia goc
                        dientichmiagoc += dientichconlai;
                }
                //throw new Exception(dientichmiagoc.ToString() + "-" + dientichmiato.ToString());
                hopdongdautumia["new_dientichmiato"] = dientichmiato;
                hopdongdautumia["new_dientichmiagoc"] = dientichmiagoc;

                service.Update(hopdongdautumia);
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
    }
}
