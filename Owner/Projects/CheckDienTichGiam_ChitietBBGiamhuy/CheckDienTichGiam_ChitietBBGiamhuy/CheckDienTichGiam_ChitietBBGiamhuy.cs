using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace CheckDienTichGiam_ChitietBBGiamhuy
{
    public class CheckDienTichGiam_ChitietBBGiamhuy : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        ITracingService trace;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("new_thuadat"))
            {
                Entity en = service.Retrieve(target.LogicalName,target.Id,new ColumnSet(true));
                Entity BBGiamHuy = service.Retrieve("new_bienbangiamhuydientich", target.Id, new ColumnSet(new string[] {"new_hopdongdautumia"}));
                Entity hopdongdautumia = service.Retrieve("new_hopdongdautumia", ((EntityReference)BBGiamHuy["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_masohopdong" }));
                Entity chitietHD = null;

                decimal dientichgiam = (decimal)en["new_dientichgiam"];

                QueryExpression q = new QueryExpression("new_thuadatcanhtac");
                q.ColumnSet = new ColumnSet(true);
                q.Criteria = new FilterExpression();
                q.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, ((EntityReference)en["new_thuadat"]).Id));
                q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, hopdongdautumia.Id));
                EntityCollection entc = service.RetrieveMultiple(q);

                if (entc.Entities.Count > 0)
                {
                    chitietHD = entc.Entities.ToList<Entity>().FirstOrDefault();
                }
                if (chitietHD == null)
                {
                    return;
                }

                chitietHD["new_dientichgiamhuy"] = (chitietHD.Contains("new_dientichgiamhuy") ? (decimal)chitietHD["new_dientichgiamhuy"] : 0) + (decimal)en["new_dientichgiam"];
                decimal dientichconlai = (chitietHD.Contains("new_dientichthucte") ? (decimal)chitietHD["new_dientichthucte"] : 0) - (decimal)en["new_dientichgiam"];

                chitietHD["new_dientichconlai"] = dientichconlai;

                if (dientichconlai < 0)
                {
                    throw new Exception("Chi tiết HD" + chitietHD["new_name"].ToString() + "không còn diện tích để giảm !!!");
                }
            }
        }
    }
}
