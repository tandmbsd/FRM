using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace TraceDeepPlugin
{
    public class Plugin_TraceDeepPlugin : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            if (context.PrimaryEntityName.ToLower().Trim() != "new_log")
            {
                Entity a = new Entity("new_log");
                a["new_name"] = context.PrimaryEntityName;
                a["new_depth"] = context.Depth;
                a["new_message"] = context.MessageName;
                if (context.ParentContext != null) {
                    a["new_preevent"] = context.ParentContext.PrimaryEntityName + " - " + context.ParentContext.MessageName + " - " + context.ParentContext.PrimaryEntityId.ToString();
                }
                service.Create(a);
            }
        }
    }
}
