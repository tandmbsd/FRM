using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Runtime.Serialization;

namespace CheckMaThuaDat
{
    public class CheckMaThuaDat : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("new_name"))
            {
                QueryExpression q = new QueryExpression("new_thuadat");
                q.ColumnSet = new ColumnSet(new string[] { "new_name" });
                q.Criteria = new FilterExpression();
                q.Criteria.AddCondition(new ConditionExpression("new_name", ConditionOperator.Equal, (string)target["new_name"]));
                EntityCollection entc = service.RetrieveMultiple(q);
                
                if (entc.Entities.Count > 1)
                {
                    throw new Exception("Mã thửa "  + (string)target["new_name"] +  " đã tồn tại");
                }
            }
        }
    }
}
