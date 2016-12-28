using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Check_Tylethuhoivondukien
{
    public class Check_Tylethuhoivondukien : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];
            if (context.MessageName == "Create")
            {
                Entity tylethuhoi = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                int tyle = 0;
                List<Entity> lstTylethuhoivondukien = new List<Entity>();

                Entity chitiethdthuedat_thuadat = service.Retrieve("new_chitiethdthuedat_thuadat", ((EntityReference)target["new_chitiethdthuedat_thuadat"]).Id, new ColumnSet(new string[] { "new_sonamthuedat" }));
                int sonamthuedat = (int)chitiethdthuedat_thuadat["new_sonamthuedat"];

                lstTylethuhoivondukien = RetrieveMultiRecord(service, "new_tylethuhoivondukien", new ColumnSet(new string[] { "new_tylethuhoivondukienid" }), "new_chitiethdthuedat_thuadat", target.Id);
                if (lstTylethuhoivondukien.Count > sonamthuedat)
                {
                    throw new Exception("Số năm tỷ lệ thu hồi không được vượt quá số năm thuê đất !!!");
                }

                foreach (Entity en in lstTylethuhoivondukien)
                {
                    tyle += (int)en["new_tylephantram"];
                }

                if (tyle > 100)
                {
                    throw new Exception("Tỷ lệ thu hồi không được vượt quá 100%");
                }
            }

            else if (context.MessageName == "Update")
            {

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
