using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace KyHDVanChuyen
{
    public class KyHDVanChuyen : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            
            Entity target = (Entity)context.InputParameters["Target"];
           
            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000) // tinh trang da ky
            {
                Entity hdvc = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_doitacvanchuyen", "new_doitacvanchuyenkhdn" }));
                
                List<Entity> lsHdvc_xvc = RetrieveMultiRecord(service, "new_hopdongvanchuyen_xevanchuyen",
                    new ColumnSet(new string[] { "new_xevanchuyen" }), "new_hopdongvanchuyen", target.Id);

                foreach (Entity en in lsHdvc_xvc)
                {
                    Entity xvc = service.Retrieve("new_xevanchuyen", ((EntityReference)en["new_xevanchuyen"]).Id
                        , new ColumnSet(new string[] { "new_doitacvanchuyenkh", "new_doitacvanchuyenkhdn" }));
                    
                    if (hdvc.Contains("new_doitacvanchuyen"))
                    {
                        xvc["new_doitacvanchuyenkh"] = hdvc["new_doitacvanchuyen"];
                    }
                    else if (hdvc.Contains("new_doitacvanchuyenkhdn"))
                    {
                        xvc["new_doitacvanchuyenkhdn"] = hdvc["new_doitacvanchuyenkhdn"];
                    }
                    service.Update(xvc);
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
