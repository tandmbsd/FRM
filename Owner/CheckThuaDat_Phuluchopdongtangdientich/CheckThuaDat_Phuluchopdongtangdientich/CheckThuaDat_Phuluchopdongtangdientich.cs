using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace CheckThuaDat_Phuluchopdongtangdientich
{
    public class CheckThuaDat_Phuluchopdongtangdientich : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("new_thuadat"))
            {
                Guid thuadatID = ((EntityReference)target["new_thuadat"]).Id;

                Entity ctplhd = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_phuluchopdong" }));

                Entity plhd = service.Retrieve("new_phuluchopdong", ((EntityReference)ctplhd["new_phuluchopdong"]).Id,
                    new ColumnSet(new string[] { "new_hopdongdautumia" }));

                Entity hopdongmia = service.Retrieve("new_hopdongdautumia", ((EntityReference)plhd["new_hopdongdautumia"]).Id,
                    new ColumnSet(new string[] { "new_vudautu" }));

                Guid vdtID = ((EntityReference)hopdongmia["new_vudautu"]).Id;

                QueryExpression q = new QueryExpression("new_thuadatcanhtac");
                q.ColumnSet = new ColumnSet(new string[] { "new_hopdongdautumia", "new_thuadat","statuscode" });
                q.Criteria = new FilterExpression();
                q.Criteria.AddCondition(new ConditionExpression("statuscode",ConditionOperator.Equal, 100000000));
                int count = 0;

                EntityCollection entc = service.RetrieveMultiple(q);

                foreach (Entity en in entc.Entities)
                {
                    if (!en.Contains("new_thuadat"))
                        continue;

                    Entity tdct = service.Retrieve(en.LogicalName, en.Id, new ColumnSet(new string[] { "new_hopdongdautumia" }));

                    Entity hd = service.Retrieve("new_hopdongdautumia", ((EntityReference)tdct["new_hopdongdautumia"]).Id,
                        new ColumnSet(new string[] { "new_vudautu" }));

                    if (thuadatID == ((EntityReference)en["new_thuadat"]).Id && ((EntityReference)hd["new_vudautu"]).Id == vdtID)
                        count++;
                }

                if (count > 0)
                {
                    throw new Exception("Thửa đất đã tồn tại trong chi tiết hợp đồng đầu tư mía khác !!!");
                }
            }
        }
    }
}
