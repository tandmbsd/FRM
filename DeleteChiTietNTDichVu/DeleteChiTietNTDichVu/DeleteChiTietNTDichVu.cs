using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace DeleteChiTietNTDichVu
{
    public class DeleteChiTietNTDichVu : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));

            if (context.MessageName == "Delete")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

                EntityReference target = (EntityReference)context.InputParameters["Target"];
                
                Entity chitietntdichvu = context.PreEntityImages["Prechitietnghiemthudichvu"];
                
                bool co = false;
                
                if (chitietntdichvu.Contains("new_dichvu") && chitietntdichvu.Contains("new_thuadat"))
                    co = true;
                
                Entity ntdichvu = service.Retrieve("new_nghiemthudichvu", ((EntityReference)chitietntdichvu["new_nghiemthudichvu"]).Id,
                    new ColumnSet(new string[] { "new_phieudangkydichvu", "new_tinhtrangduyet" }));
               
                if (ntdichvu.Contains("new_phieudangkydichvu") && ((OptionSetValue)ntdichvu["new_tinhtrangduyet"]).Value == 100000001 && co == true)
                {
                    Entity pdkdichvu = service.Retrieve("new_phieudangkydichvu",
                                ((EntityReference)ntdichvu["new_phieudangkydichvu"]).Id, new ColumnSet(new string[] { "new_danghiemthu" }));

                    decimal danghiemthu = pdkdichvu.Contains("new_danghiemthu") ? (decimal)pdkdichvu["new_danghiemthu"] : 0;

                    if (danghiemthu == 0)
                    {
                        QueryExpression q = new QueryExpression("new_chitietdangkydichvu");
                        q.ColumnSet = new ColumnSet(true);
                        q.Criteria = new FilterExpression();
                        q.Criteria.AddCondition(new ConditionExpression("new_phieudangkydichvu", ConditionOperator.Equal, pdkdichvu.Id));
                        q.Criteria.AddCondition(new ConditionExpression("new_dichvu", ConditionOperator.Equal, ((EntityReference)chitietntdichvu["new_dichvu"]).Id));
                        q.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, ((EntityReference)chitietntdichvu["new_thuadat"]).Id));
                        EntityCollection entc = service.RetrieveMultiple(q);
                        
                        foreach (Entity en in entc.Entities)
                        {
                            service.Delete(en.LogicalName, en.Id);
                        }
                    }
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
