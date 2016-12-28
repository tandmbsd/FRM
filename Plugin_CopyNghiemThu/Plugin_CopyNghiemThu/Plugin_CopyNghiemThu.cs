using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Plugin_CopyNghiemThu
{
    public class Plugin_CopyNghiemThu : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            // moi nhat
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("new_hopdongtrongmia"))
            {
                Entity nghiemthutrongmia = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_khachhang", "new_khachhangdoanhnghiep" }));

                Entity hopdongdautumia = service.Retrieve("new_hopdongdautumia", ((EntityReference)target["new_hopdongtrongmia"]).Id,
                    new ColumnSet(new string[] { "new_hopdongdautumiaid" }));

                QueryExpression q = new QueryExpression("new_nghiemthutrongmia");
                q.ColumnSet = new ColumnSet(true);
                q.Criteria = new FilterExpression();
                q.Criteria.AddCondition(new ConditionExpression("new_hopdongtrongmia", ConditionOperator.Equal, hopdongdautumia.Id));

                if (nghiemthutrongmia.Contains("new_khachhang"))                
                    q.Criteria.AddCondition(new ConditionExpression("new_khachhang", ConditionOperator.Equal, ((EntityReference)nghiemthutrongmia["new_khachhang"]).Id));                

                else if (nghiemthutrongmia.Contains("new_khachhangdoanhnghiep"))                
                    q.Criteria.AddCondition(new ConditionExpression("new_khachhangdoanhnghiep", ConditionOperator.Equal, ((EntityReference)nghiemthutrongmia["new_khachhangdoanhnghiep"]).Id));                

                q.AddOrder("new_lannghiemthu_global", OrderType.Descending);

                EntityCollection entc = service.RetrieveMultiple(q);
                
                if (entc.Entities.Count > 1)
                {
                    Entity nghiemthutrongmiaNew = entc.Entities[0];

                    List<Entity> lstChitiet = RetrieveMultiRecord(service, "new_chitietnghiemthutrongmia", new ColumnSet(true),
                        "new_nghiemthutrongmia", nghiemthutrongmiaNew.Id);
                    
                    foreach (Entity en in lstChitiet)
                    {
                        Entity chitietNew = new Entity("new_chitietnghiemthutrongmia");

                        if (en.Contains("new_thuadat"))                        
                            chitietNew["new_thuadat"] = en["new_thuadat"];                       

                        chitietNew["new_nghiemthutrongmia"] = nghiemthutrongmia.ToEntityReference();

                        if (en.Contains("new_vutrong"))                        
                            chitietNew["new_vutrong"] = en["new_vutrong"];                       

                        if (en.Contains("new_loaigocmia"))                        
                            chitietNew["new_loaigocmia"] = en["new_loaigocmia"];                       

                        if (en.Contains("new_giongmia"))                        
                            chitietNew["new_giongmia"] = en["new_giongmia"];                        

                        if (en.Contains("new_ngaytrongxulygoc"))                        
                            chitietNew["new_ngaytrongxulygoc"] = en["new_ngaytrongxulygoc"];                        

                        if (en.Contains("new_dientichnghiemthu"))                        
                            chitietNew["new_dientichnghiemthu"] = en["new_dientichnghiemthu"];                       

                        service.Create(chitietNew);
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
