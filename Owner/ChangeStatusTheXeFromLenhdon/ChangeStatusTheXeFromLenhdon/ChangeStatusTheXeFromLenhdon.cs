using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Runtime.Serialization;

namespace ChangeStatusTheXeFromLenhdon
{
    public class ChangeStatusTheXeFromLenhdon : IPlugin
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

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000) // da duyet
            {
                Entity lenhdon = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_thexe" }));

                if (lenhdon.Contains("new_thexe"))
                {
                    Entity thexe = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "statuscode" }));
                    thexe["statuscode"] = new OptionSetValue(100000001); // dang van chuyen

                    service.Update(thexe);
                }
            }
        }
    }
}
