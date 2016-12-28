using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Runtime.Serialization;


namespace ChangeStatusTheXeFormHDVC
{
    public class ChangeStatusTheXeFormHDVC : IPlugin
    {
        private IOrganizationService service = null;
        private IOrganizationServiceFactory factory = null;
        public ITracingService trace;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statuscode"))
            {
                List<Entity> lstThexe = RetrieveMultiRecord(service, "new_hopdongvanchuyen_xevanchuyen",
                    new ColumnSet(new string[] { "statuscode" }), "new_hopdongvanchuyen", target.Id);

                int statusHD = ((OptionSetValue)target["statuscode"]).Value;

                if (lstThexe.Count > 0 && (statusHD == 100000000 || statusHD == 100000002))
                {
                    foreach (Entity a in lstThexe)
                    {
                        if (statusHD == 100000000) // da ky
                        {
                            a["statuscode"] = new OptionSetValue(100000001);
                        }
                        else if (statusHD == 100000002) // huy
                        {
                            a["statuscode"] = new OptionSetValue(100000002);
                        }                       

                        service.Update(a);
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
