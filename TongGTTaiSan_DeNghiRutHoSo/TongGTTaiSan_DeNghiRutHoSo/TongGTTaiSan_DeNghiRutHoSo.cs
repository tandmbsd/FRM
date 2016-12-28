using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;

namespace TongGTTaiSan_DeNghiRutHoSo
{
    public class TongGTTaiSan_DeNghiRutHoSo : IPlugin
    {
        private IOrganizationServiceFactory serviceProxy;
        private IOrganizationService service;
        public void Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            serviceProxy = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = serviceProxy.CreateOrganizationService(context.UserId);
            ITracingService trace= (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            if (context.Depth > 1)
            {
                return;
            }

            if (context.MessageName == "Create" || context.MessageName == "Update")
            {
                Entity target = (Entity)context.InputParameters["Target"];
                if (target.Contains("new_hopdongthechap"))
                {
                    Entity ctDenghiruthoso = service.Retrieve(target.LogicalName, target.Id,
                        new ColumnSet(new string[] { "new_hopdongthechap", "new_denghiruthoso" }));

                    Entity denghiruthoso = service.Retrieve("new_denghiruthoso", ((EntityReference)ctDenghiruthoso["new_denghiruthoso"]).Id,
                            new ColumnSet(new string[] { "new_tonggiatritaisandangthechap" }));

                    List<Entity> lstctDenghiruthoso = RetrieveMultiRecord(service, "new_chitietdenghiruthoso",
                        new ColumnSet(new string[] { "new_taisanthechap" }), "new_denghiruthoso", denghiruthoso.Id);
                    decimal tonggiatri = 0;

                    foreach (Entity k in lstctDenghiruthoso)
                    {
                        List<Entity> lstTaisanthechap = RetrieveMultiRecord(service, "new_taisanthechap",
                        new ColumnSet(new string[] { "statuscode", "new_giatridinhgiagiatrithechap" }), "new_hopdongthechap", ((EntityReference)ctDenghiruthoso["new_hopdongthechap"]).Id);


                        foreach (Entity en in lstTaisanthechap)
                        {
                            decimal gtdinhgia = ((Money)en["new_giatridinhgiagiatrithechap"]).Value;
                            tonggiatri += gtdinhgia;
                        }
                    }
                    
                    denghiruthoso["new_tonggiatritaisandangthechap"] = new Money(tonggiatri);
                    service.Update(denghiruthoso);
                }
            }
            else if (context.MessageName == "Delete")
            {
                Entity target = new Entity(((EntityReference)context.InputParameters["Target"]).LogicalName);
                target.Id = ((EntityReference)context.InputParameters["Target"]).Id;

                Entity ctDenghiruthoso = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(true));
                trace.Trace("2");
                Entity denghiruthoso = service.Retrieve("new_denghiruthoso", ((EntityReference)ctDenghiruthoso["new_denghiruthoso"]).Id,
                        new ColumnSet(new string[] { "new_tonggiatritaisandangthechap" }));
                trace.Trace("2");
                List<Entity> lstctDenghiruthoso = RetrieveMultiRecord(service, "new_chitietdenghiruthoso",
                    new ColumnSet(new string[] { "new_taisanthechap" }), "new_denghiruthoso", denghiruthoso.Id);
                decimal tonggiatri = denghiruthoso.Contains("new_tonggiatritaisandangthechap") ? ((Money)denghiruthoso["new_tonggiatritaisandangthechap"]).Value : 0;
                trace.Trace("2");
                foreach (Entity k in lstctDenghiruthoso)
                {
                    List<Entity> lstTaisanthechap = RetrieveMultiRecord(service, "new_taisanthechap",
                    new ColumnSet(new string[] { "statuscode", "new_giatridinhgiagiatrithechap" }), "new_hopdongthechap", ((EntityReference)ctDenghiruthoso["new_hopdongthechap"]).Id);

                    foreach (Entity en in lstTaisanthechap)
                    {
                        decimal gtdinhgia = ((Money)en["new_giatridinhgiagiatrithechap"]).Value;
                        tonggiatri -= gtdinhgia;
                    }
                }
                
                denghiruthoso["new_tonggiatritaisandangthechap"] = new Money(tonggiatri);
                service.Update(denghiruthoso);

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
