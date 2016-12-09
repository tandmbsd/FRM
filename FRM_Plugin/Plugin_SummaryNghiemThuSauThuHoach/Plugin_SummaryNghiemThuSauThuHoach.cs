using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_SummaryNghiemThuSauThuHoach
{
    public class Plugin_SummaryNghiemThuSauThuHoach : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            
            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("new_thuadatcanhtac"))
            {
                decimal miatuoi = 0;
                decimal miachay = 0;
                decimal tong = 0;
                List<Entity> AllLenhDon = RetrieveMultiRecord(service, "new_lenhdon", new ColumnSet(new string[] {"new_trongluongmia", "new_miachay"}), "new_thuacanhtac", ((EntityReference)target["new_thuadatcanhtac"]).Id);
                foreach(Entity a in AllLenhDon)
                {
                    if (a.Attributes.Contains("new_miachay") && (bool)a["new_miachay"])
                        miachay += (a.Attributes.Contains("new_trongluongmia") ? (decimal)a["new_trongluongmia"] : 0);
                    else miatuoi += (a.Attributes.Contains("new_trongluongmia") ? (decimal)a["new_trongluongmia"] : 0);
                    tong += (a.Attributes.Contains("new_trongluongmia") ? (decimal)a["new_trongluongmia"] : 0);
                }

                Entity rs = new Entity("new_chitietnghiemthusauthuhoach");
                rs["new_miatuoi"] = miatuoi;
                rs["new_miachay"] = miachay;
                rs["new_tongsanluong"] = tong;

                service.Update(rs);
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
