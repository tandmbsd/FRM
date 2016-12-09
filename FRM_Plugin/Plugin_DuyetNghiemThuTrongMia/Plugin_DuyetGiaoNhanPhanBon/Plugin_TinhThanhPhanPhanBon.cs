using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_TinhThanhPhanPhanBon
{
    public class Plugin_TinhThanhPhanPhanBon : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            
            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("new_bonphan"))
            {
                decimal new_dongcu = 0;
                decimal new_huucoompvs = 0;
                decimal new_kali = 0;
                decimal new_kemzn = 0;
                decimal new_landetieu = 0;
                decimal new_magie = 0;
                decimal new_mangan = 0;
                decimal new_molypdenmo = 0;
                decimal new_nitodamn = 0;
                decimal new_satfe = 0;
                decimal new_silic = 0;
                decimal new_phantramcao = 0;
                decimal new_dolomite = 0;

                List<Entity> AllChiTietBonPhan = RetrieveMultiRecord(service, "new_chitietbonphan", new ColumnSet(true), "new_bonphan", target.Id);
                foreach (Entity a in AllChiTietBonPhan)
                {
                    Entity Phanbon = service.Retrieve("new_phanbon", ((EntityReference)a["new_phanbon"]).Id, new ColumnSet(true));
                    new_dongcu += (Phanbon.Contains("new_dongcu") ? (decimal)Phanbon["new_dongcu"] : 0) * (decimal)a["new_soluong"];
                    new_huucoompvs += (Phanbon.Contains("new_huucoompvs") ? (decimal)Phanbon["new_huucoompvs"] : 0) * (decimal)a["new_soluong"];
                    new_kali += (Phanbon.Contains("new_kali") ? (decimal)Phanbon["new_kali"] : 0) * (decimal)a["new_soluong"];
                    new_kemzn += (Phanbon.Contains("new_kemzn") ? (decimal)Phanbon["new_kemzn"] : 0) * (decimal)a["new_soluong"];
                    new_landetieu += (Phanbon.Contains("new_landetieu") ? (decimal)Phanbon["new_landetieu"] : 0) * (decimal)a["new_soluong"];
                    new_magie += (Phanbon.Contains("new_magie") ? (decimal)Phanbon["new_magie"] : 0) * (decimal)a["new_soluong"];
                    new_mangan += (Phanbon.Contains("new_mangan") ? (decimal)Phanbon["new_mangan"] : 0) * (decimal)a["new_soluong"];
                    new_molypdenmo += (Phanbon.Contains("new_molypdenmo") ? (decimal)Phanbon["new_molypdenmo"] : 0) * (decimal)a["new_soluong"];
                    new_nitodamn += (Phanbon.Contains("new_nitodamn") ? (decimal)Phanbon["new_nitodamn"] : 0) * (decimal)a["new_soluong"];
                    new_satfe += (Phanbon.Contains("new_satfe") ? (decimal)Phanbon["new_satfe"] : 0) * (decimal)a["new_soluong"];
                    new_silic += (Phanbon.Contains("new_silic") ? (decimal)Phanbon["new_silic"] : 0) * (decimal)a["new_soluong"];
                    new_phantramcao += (Phanbon.Contains("new_phantramcao") ? (decimal)Phanbon["new_phantramcao"] : 0) * (decimal)a["new_soluong"];
                    new_dolomite += (Phanbon.Contains("new_dolomite") ? (decimal)Phanbon["new_dolomite"] : 0) * (decimal)a["new_soluong"];
                }

                Entity rs = new Entity("new_bonphan");
                rs.Id = ((EntityReference)target["new_bonphan"]).Id;
                rs["new_huucoompvs"] = new_huucoompvs;
                rs["new_dongcu"] = new_dongcu;
                rs["new_kali"] = new_kali;
                rs["new_kemzn"] = new_kemzn;
                rs["new_landetieu"] = new_landetieu;
                rs["new_magie"] = new_magie;
                rs["new_mangan"] = new_mangan;
                rs["new_molypdenmo"] = new_molypdenmo;
                rs["new_nitodamn"] = new_nitodamn;
                rs["new_satfe"] = new_satfe;
                rs["new_silic"] = new_silic;
                rs["new_canxi"] = new_phantramcao;
                rs["new_dolomite"] = new_dolomite;
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
