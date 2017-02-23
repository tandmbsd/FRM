using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_Khoiluongbonphan
{
    public class Plugin_Khoiluongbonphan : IPlugin
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

            //List<Entity> chitietbonphan = RetrieveMultiRecord(service, "new_chitietbonphan", new ColumnSet(new string[] { "new_phanbon", "new_soluong" }), "new_bonphan", target.Id);
            Entity target;
            Entity fullentity;
            bool flag = false;

            if (context.MessageName.ToLower().Trim() == "create" || context.MessageName.ToLower().Trim() == "update")
            {
                target = context.InputParameters["Target"] as Entity;
                fullentity = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                if (fullentity.Contains("new_bonphan"))
                    flag = true;
            }
            else
            {
                target = new Entity(((EntityReference)context.InputParameters["Target"]).LogicalName);
                target.Id = ((EntityReference)context.InputParameters["Target"]).Id;

                fullentity = context.PreEntityImages["PreImage"];

                if (fullentity.Contains("new_bonphan"))
                    flag = true;
            }

            if (flag == true)
            {
                Entity bonphan = service.Retrieve("new_bonphan", ((EntityReference)fullentity["new_bonphan"]).Id,
                new ColumnSet(new string[] { "new_satfe", "new_huucoompvs", "new_landetieu", "new_nitodamn", "new_kali",
                        "new_canxi", "new_magie", "new_silic", "new_kemzn", "new_mangan", "new_molypdenmo" }));

                List<Entity> lstChitietbonphan = RetrieveMultiRecord(service, "new_chitietbonphan",
                    new ColumnSet(new string[] { "new_phanbon", "new_soluong" }), "new_bonphan", bonphan.Id);

                decimal fe = 0;
                decimal huucoompvs = 0;
                decimal landetieu = 0;
                decimal nitodamn = 0;
                decimal kali = 0;
                decimal canxi = 0;
                decimal magie = 0;
                decimal silic = 0;
                decimal kemzn = 0;
                decimal mangan = 0;
                decimal molypdenmo = 0;

                foreach (Entity en in lstChitietbonphan)
                {
                    decimal soluong = en.Contains("new_soluong") ? (decimal)en["new_soluong"] : new decimal(0);

                    if (!en.Contains("new_phanbon"))
                        throw new Exception("Chi tiết bón phân không có phân bón");

                    Entity phanbon = service.Retrieve("new_phanbon", ((EntityReference)en["new_phanbon"]).Id, new ColumnSet(new string[] { "new_satfe", "new_huucoompvs", "new_landetieu", "new_nitodamn", "new_kali",
                        "new_canxi", "new_magie", "new_silic", "new_kemzn", "new_mangan", "new_molypdenmo" }));

                    fe += (phanbon.Contains("new_satfe") ? phanbon.GetAttributeValue<decimal>("new_satfe") : 0) * soluong / 100;
                    huucoompvs += (phanbon.Contains("new_huucoompvs") ? phanbon.GetAttributeValue<decimal>("new_huucoompvs") : 0) * soluong / 100;
                    landetieu += (phanbon.Contains("new_landetieu") ? phanbon.GetAttributeValue<decimal>("new_landetieu") : 0) * soluong / 100;
                    nitodamn += (phanbon.Contains("new_nitodamn") ? phanbon.GetAttributeValue<decimal>("new_nitodamn") : 0) * soluong / 100;
                    kali += (phanbon.Contains("new_kali") ? phanbon.GetAttributeValue<decimal>("new_kali") : 0) * soluong / 100;
                    canxi += (phanbon.Contains("new_canxi") ? phanbon.GetAttributeValue<decimal>("new_canxi") : 0) * soluong / 100;
                    magie += (phanbon.Contains("new_magie") ? phanbon.GetAttributeValue<decimal>("new_magie") : 0) * soluong / 100;
                    silic += (phanbon.Contains("new_silic") ? phanbon.GetAttributeValue<decimal>("new_silic") : 0) * soluong / 100;
                    kemzn += (phanbon.Contains("new_kemzn") ? phanbon.GetAttributeValue<decimal>("new_kemzn") : 0) * soluong / 100;
                    mangan += (phanbon.Contains("new_mangan") ? phanbon.GetAttributeValue<decimal>("new_mangan") : 0) * soluong / 100;
                    molypdenmo += (phanbon.Contains("new_molypdenmo") ? phanbon.GetAttributeValue<decimal>("new_molypdenmo") : 0) * soluong / 100;
                }

                //throw new Exception(kali.ToString() + "-" + nitodamn.ToString());
                bonphan["new_satfe"] = fe;
                bonphan["new_huucoompvs"] = huucoompvs;
                bonphan["new_landetieu"] = landetieu;
                bonphan["new_nitodamn"] = nitodamn;
                bonphan["new_kali"] = kali;
                bonphan["new_canxi"] = canxi;
                bonphan["new_magie"] = magie;
                bonphan["new_silic"] = silic;
                bonphan["new_kemzn"] = kemzn;
                bonphan["new_mangan"] = mangan;
                bonphan["new_molypdenmo"] = molypdenmo;

                service.Update(bonphan);
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
