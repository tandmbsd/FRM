using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Action_TienTienHuongLoi
{
    public class Action_TienTienHuongLoi : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            EntityReference target = (EntityReference)context.InputParameters["Target"];
            
            if (target.LogicalName == "new_hopdongdautuhatang")
            {
                
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity hopdongdautuhatang = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                Guid hopdongdautuhatangID = hopdongdautuhatang.Id;

                if (hopdongdautuhatang == null)
                {
                    throw new Exception("Hợp đồng đầu tư hạ tầng không tồn tại !!!");
                }
                List<Entity> lstchitietHDDTHatang = RetrieveMultiRecord(service, "new_chitietgopdongdautuhatang", new ColumnSet(true), "new_hopdongdautuhatang", hopdongdautuhatang.Id);
                foreach (Entity en in lstchitietHDDTHatang)
                {
                    Entity updateEn = service.Retrieve(en.LogicalName, en.Id, new ColumnSet(true));

                    updateEn["new_sotien"] = new Money((updateEn.Contains("new_dientichhuongloi") ? (decimal)updateEn["new_dientichhuongloi"] : 0) *
                         (hopdongdautuhatang.Contains("new_dinhmucdonggop") ? ((Money)hopdongdautuhatang["new_dinhmucdonggop"]).Value : 0));
                    
                    service.Update(updateEn);
                    
                }

                context.OutputParameters["ReturnId"] = hopdongdautuhatangID.ToString();
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
