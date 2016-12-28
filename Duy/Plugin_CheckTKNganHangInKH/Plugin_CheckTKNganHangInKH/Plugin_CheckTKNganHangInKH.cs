using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_CheckTKNganHangInKH
{
    public class Plugin_CheckTKNganHangInKH : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));           

            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("new_giaodichchinh"))
            {                
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);

                Entity TKNH = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_khachhang", "new_khachhangdoanhnghiep", "new_giaodichchinh" }));
                if(TKNH.Contains("new_khachhang")){                    
                    List<Entity> lstTkNganhang = RetrieveMultiRecord(service, "new_taikhoannganhang", new ColumnSet(new string[] { "new_giaodichchinh" }), "new_khachhang", ((EntityReference)TKNH["new_khachhang"]).Id);
                    int count = 0;

                    foreach(Entity en in lstTkNganhang){                        
                        if (en.Contains("new_giaodichchinh") && en["new_giaodichchinh"].ToString() == "True")
                        {
                            count++;
                        }
                    }
                    
                    if(count > 1){
                        throw new Exception("Khách hàng chỉ được phép có tối đa 1 tài khoản giao dịch chính !!");
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
