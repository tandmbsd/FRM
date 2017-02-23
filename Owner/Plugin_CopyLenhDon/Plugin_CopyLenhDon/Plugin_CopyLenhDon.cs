using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.Discovery;
using Microsoft.Xrm.Sdk.XmlNamespaces;

namespace Plugin_CopyLenhDon
{
    public class Plugin_CopyLenhDon : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            EntityReference target = (EntityReference)context.InputParameters["Target"];

            if (target.LogicalName == "new_lenhdon")
            {
                Entity lenhdon = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                if (lenhdon == null)
                {
                    throw new Exception("Lệnh đốn này không tồn tại !!");
                }

                Entity newLenhdon = new Entity("new_lenhdon");

                foreach (string key in lenhdon.Attributes.Keys)
                {
                    if (key.IndexOf("new") == 0 && key != "new_lenhdonid" && key != "new_name")
                        newLenhdon[key] = lenhdon[key];

                    Guid newLenhdonID = service.Create(newLenhdon);
                    context.OutputParameters["ReturnId"] = newLenhdonID.ToString();
                }
            }
        }
    }
}
