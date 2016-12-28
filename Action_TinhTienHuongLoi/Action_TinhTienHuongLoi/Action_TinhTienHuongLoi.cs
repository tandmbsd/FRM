using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Action_TinhTienHuongLoi
{
    public class Action_TinhTienHuongLoi : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            EntityReference target = (EntityReference)context.InputParameters["Target"];
            //throw new Exception("abc");
            if (target.LogicalName == "new_hopdongdautuhatang")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity hopdongdautuhatang = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                if (hopdongdautuhatang == null)
                {
                    throw new Exception("Hợp đồng đầu tư hạ tầng này không tồn tại ");
                }

                decimal giatrihopdong = hopdongdautuhatang.Contains("new_giatrihopdong") ? ((Money)hopdongdautuhatang["new_giatrihopdong"]).Value : 0;
                decimal tongdientichuongloi = hopdongdautuhatang.Contains("new_tongdientichhuongloi") ? (decimal)hopdongdautuhatang["new_tongdientichhuongloi"] : 0;

                if (tongdientichuongloi == 0)
                {
                    throw new Exception("Tổng diện tích hưởng lợi bằng 0");
                }

                decimal sotiendiaphuonghotro = hopdongdautuhatang.Contains("new_sotienbenkhachotro") ? ((Money)hopdongdautuhatang["new_sotienbenkhachotro"]).Value : 0;
                decimal sotiennhamayhotro = hopdongdautuhatang.Contains("new_sotienhotro") ? ((Money)hopdongdautuhatang["new_sotienhotro"]).Value : 0;
                decimal sotiennongdanchiu = giatrihopdong - sotiendiaphuonghotro - sotiennhamayhotro;
                decimal dongia = sotiennongdanchiu / tongdientichuongloi;                

                List<Entity> lstChitiethatang = RetrieveMultiRecord(service, "new_chitietgopdongdautuhatang",
                    new ColumnSet(new string[] { "new_dientichhuongloi", "new_sotien", "new_name" }), "new_hopdongdautuhatang", hopdongdautuhatang.Id);

                foreach (Entity en in lstChitiethatang)
                {
                    Entity k = service.Retrieve(en.LogicalName, en.Id, new ColumnSet(new string[] { "new_sotien", "new_name" }));

                    k["new_sotien"] = new Money((dongia * (decimal)en["new_dientichhuongloi"]));
                    service.Update(k);                    
                }

                context.OutputParameters["ReturnId"] = hopdongdautuhatang.Id.ToString();
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
