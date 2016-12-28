using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace Action_ChangeStage
{
    public class Action_ChangeStage : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            try
            {
                string entityName = context.InputParameters["Entity"].ToString();
                Guid RecordID = Guid.Parse(context.InputParameters["ObjectId"].ToString());
                Guid StageId = Guid.Parse(context.InputParameters["StageId"].ToString());
                string attributeName = context.InputParameters["Attribute"].ToString();
                int attributeValue = int.Parse(context.InputParameters["Value"].ToString());

                Entity updatedStage = new Entity(entityName);
                updatedStage.Id = RecordID;
                updatedStage["stageid"] = StageId;
                updatedStage[attributeName] = new OptionSetValue(attributeValue);
                service.Update(updatedStage);

                context.OutputParameters["Return"] = "success";
            }
            catch (Exception ex)
            {
                context.OutputParameters["Return"] = ex.Message;
            }
        }
    }
}
