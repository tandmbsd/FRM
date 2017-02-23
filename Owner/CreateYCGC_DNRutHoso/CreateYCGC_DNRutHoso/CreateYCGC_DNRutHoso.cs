using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace CreateYCGC_DNRutHoso
{
    public class CreateYCGC_DNRutHoso : IPlugin
    {
        //moi nhat
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000 && context.Depth < 2) // da duyet
            {
                List<Entity> lstctDenghiruthoso = RetrieveMultiRecord(service, "new_chitietdenghiruthoso",
                        new ColumnSet(true), "new_denghiruthoso", target.Id);
                traceService.Trace("1");
                foreach (Entity en in lstctDenghiruthoso)
                {
                    if (!en.Contains("new_taisanthechap"))
                        continue;

                    if (!en.Contains("new_hopdongthechap"))
                        continue;

                    //Entity updateDenghi = service.Retrieve(en.LogicalName, en.Id,
                    //    new ColumnSet(new string[] {"statuscode"}));

                    //updateDenghi["statuscode"] = new OptionSetValue(100000001);

                    Entity k = new Entity("new_yeucaugiaichap");
                    k["new_name"] = "Yêu cầu giải chấp - " + DateTime.Now.Date.ToString();
                    k["new_hopdauthechap"] = en["new_hopdongthechap"];

                    Entity taisanthechap = service.Retrieve("new_taisanthechap", ((EntityReference)en["new_taisanthechap"]).Id,
                        new ColumnSet(new string[] { "new_taisan", "new_name" }));

                    Entity taisan = service.Retrieve("new_taisan", ((EntityReference)taisanthechap["new_taisan"]).Id, new ColumnSet(true));
                    k["new_taisan"] = taisan.ToEntityReference();
                    k["new_ngaylapphieu"] = DateTime.Now;
                    service.Create(k);
                    //service.Update(updateDenghi);
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
    }
}
