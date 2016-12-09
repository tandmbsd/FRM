using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_CheckStageProcessConfig
{
    public class Plugin_CheckStageProcessConfig : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("new_tinhtrangduyet"))
            {
                QueryExpression q1 = new QueryExpression();
                q1.EntityName = "new_processconfig";
                q1.ColumnSet = new ColumnSet(new string[] { "new_processid", "new_stageid" });
                q1.Criteria.AddCondition(new ConditionExpression("new_schemaname", ConditionOperator.Equal, target.LogicalName));
                q1.Criteria.AddCondition(new ConditionExpression("new_stagevalue", ConditionOperator.Equal, ((OptionSetValue)target["new_tinhtrangduyet"]).Value));

                EntityCollection cl = service.RetrieveMultiple(q1);
                if (cl.Entities.Count > 0 && cl[0].Contains("new_stageid") && cl[0].Contains("new_processid"))
                {
                    Entity updatedStage = new Entity(target.LogicalName);
                    updatedStage.Id = target.Id;
                    updatedStage["stageid"] = Guid.Parse(cl[0]["new_stageid"].ToString());
                    updatedStage["processid"] = Guid.Parse(cl[0]["new_processid"].ToString());
                    service.Update(updatedStage);
                }
            }
               
        }
    }
}
