using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;


namespace Tilethuhoivon_SoNamthuhoi
{
    public class Tilethuhoivon_SoNamthuhoi : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];
            Entity tilthuhoivon = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_chinhsachdautu" }));
            if (tilthuhoivon.Contains("new_chinhsachdautu"))
            {
                Entity chinhsachdautu = service.Retrieve("new_chinhsachdautu", ((EntityReference)tilthuhoivon["new_chinhsachdautu"]).Id, new ColumnSet(new string[] { "new_sonamthue" }));
                List<Entity> lsttilethuhoivon = RetrieveMultiRecord(service, "new_tilethuhoivon", new ColumnSet(new string[] { "new_tilethuhoivonid" }), "new_chinhsachdautu", chinhsachdautu.Id);

                if (chinhsachdautu.Contains("new_sonamthue"))
                {
                    int sonamthue = (int)chinhsachdautu["new_sonamthue"];
                    if (lsttilethuhoivon.Count > sonamthue)
                    {
                        throw new Exception("Tỷ lệ thu hồi vốn không được vượt quá số năm thuê !!!");
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
    }
}
