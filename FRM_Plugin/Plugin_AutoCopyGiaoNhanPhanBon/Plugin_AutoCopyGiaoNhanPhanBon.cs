using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_AutoCopyGiaoNhanPhanBon
{
    public class Plugin_AutoCopyGiaoNhanPhanBon : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            
            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("new_phieudangkyphanbon"))
            {
                Entity PhieuGiaoNhan = service.Retrieve("new_phieugiaonhanphanbon", target.Id, new ColumnSet(true));
                Entity PDKPhanBon = service.Retrieve("new_phieudangkyphanbon", ((EntityReference)target["new_phieudangkyphanbon"]).Id, new ColumnSet(true));

                List<Entity> DSchitietcu = RetrieveMultiRecord(service, "new_chitietgiaonhanphanbon", new ColumnSet(true), "new_phieugiaonhanphanbon", target.Id);
                foreach (Entity a in DSchitietcu)
                    service.Delete(a.LogicalName, a.Id);

                List<Entity> DSCtPhanBon = RetrieveMultiRecord(service, "new_chitietdangkyphanbon", new ColumnSet(true), "new_phieudangkyphanbon", ((EntityReference)target["new_phieudangkyphanbon"]).Id);
                foreach (Entity a in DSCtPhanBon)
                {
                    Entity rs = new Entity("new_chitietgiaonhanphanbon");
                    rs["new_name"] = "Nhận phân " + ((EntityReference)a["new_phanbon"]).Name;
                    rs["new_phieugiaonhanphanbon"] = new EntityReference("new_phieugiaonhanphanbon", target.Id);
                    rs["new_donvitinh"] = a.Attributes.Contains("new_donvitinh") ? a["new_donvitinh"] : null ;
                    rs["new_soluong"] = a.Attributes.Contains("new_soluong") ? a["new_soluong"] : null;
                    rs["new_dongia"] = a.Attributes.Contains("new_dongia") ? a["new_dongia"] : null;
                    service.Create(rs);
                }
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
